"""Metadata Management Service.
Handles transactional metadata, concurrency locking, and audit logs.
"""
import asyncio
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from backend.app.models.models import (
    NodeModel, ObjectModel, ReplicaModel, RepairJobModel,
    IntegrityCheckModel, SystemEventModel, NetworkPartitionModel,
    utc_now, generate_uuid
)
from backend.app.config import settings

class MetadataService:
    def __init__(self):
        # Concurrency locks per object_id to serialize concurrent mutations safely
        self._object_locks: Dict[str, asyncio.Lock] = {}
        self._global_lock = asyncio.Lock()

    async def get_object_lock(self, object_id: str) -> asyncio.Lock:
        """Get or create an async lock for a specific object."""
        async with self._global_lock:
            if object_id not in self._object_locks:
                self._object_locks[object_id] = asyncio.Lock()
            return self._object_locks[object_id]

    def init_default_nodes(self, db: Session):
        """Seed the 4 default nodes if the database is newly created."""
        existing_nodes = db.query(NodeModel).count()
        if existing_nodes == 0:
            for node_cfg in settings.DEFAULT_NODES:
                node = NodeModel(
                    id=node_cfg["id"],
                    name=node_cfg["name"],
                    url=node_cfg["url"],
                    storage_path=node_cfg["storage_path"],
                    status="ONLINE",
                    total_capacity_bytes=settings.DEFAULT_NODE_CAPACITY_BYTES,
                    used_capacity_bytes=0,
                    free_capacity_bytes=settings.DEFAULT_NODE_CAPACITY_BYTES,
                    object_count=0,
                    last_heartbeat=utc_now(),
                    response_time_ms=0.0,
                    is_partitioned=False,
                    simulated_latency_ms=0
                )
                db.add(node)
            self.log_event(
                db,
                level="INFO",
                component="METADATA_MANAGER",
                message="Initialized default 4 distributed storage nodes in database."
            )
            db.commit()

    # --- Node Operations ---
    def get_node(self, db: Session, node_id: str) -> Optional[NodeModel]:
        return db.query(NodeModel).filter(NodeModel.id == node_id).first()

    def list_nodes(self, db: Session) -> List[NodeModel]:
        return db.query(NodeModel).all()

    def get_healthy_online_nodes(self, db: Session) -> List[NodeModel]:
        return db.query(NodeModel).filter(
            NodeModel.status == "ONLINE",
            NodeModel.is_partitioned == False
        ).all()

    def update_node_heartbeat(
        self,
        db: Session,
        node_id: str,
        status: str,
        used_bytes: int,
        free_bytes: int,
        object_count: int,
        response_time_ms: float
    ):
        node = self.get_node(db, node_id)
        if node:
            node.status = status
            node.used_capacity_bytes = used_bytes
            node.free_capacity_bytes = free_bytes
            node.object_count = object_count
            node.last_heartbeat = utc_now()
            node.response_time_ms = response_time_ms
            db.commit()

    def set_node_status(self, db: Session, node_id: str, status: str) -> Optional[NodeModel]:
        node = self.get_node(db, node_id)
        if node:
            old_status = node.status
            node.status = status
            self.log_event(
                db,
                level="WARNING" if status == "OFFLINE" else "INFO",
                component="STORAGE_NODE_MANAGER",
                message=f"Node {node_id} status changed from {old_status} to {status}."
            )
            db.commit()
            db.refresh(node)
        return node

    # --- Object Operations ---
    def create_object(
        self,
        db: Session,
        object_id: str,
        name: str,
        content_type: str,
        size_bytes: int,
        checksum: str,
        replication_factor: int,
        version: int = 1
    ) -> ObjectModel:
        obj = ObjectModel(
            id=object_id,
            name=name,
            content_type=content_type,
            size_bytes=size_bytes,
            checksum=checksum,
            version=version,
            replication_factor=replication_factor,
            status="HEALTHY",
            created_at=utc_now(),
            updated_at=utc_now()
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj

    def get_object(self, db: Session, object_id: str) -> Optional[ObjectModel]:
        return db.query(ObjectModel).filter(ObjectModel.id == object_id).first()

    def list_objects(self, db: Session, search: Optional[str] = None) -> List[ObjectModel]:
        query = db.query(ObjectModel).order_by(desc(ObjectModel.created_at))
        if search:
            query = query.filter(ObjectModel.name.ilike(f"%{search}%"))
        return query.all()

    def update_object_status(self, db: Session, object_id: str, status: str):
        obj = self.get_object(db, object_id)
        if obj:
            obj.status = status
            obj.updated_at = utc_now()
            db.commit()

    def delete_object(self, db: Session, object_id: str) -> bool:
        obj = self.get_object(db, object_id)
        if obj:
            db.delete(obj)
            db.commit()
            return True
        return False

    # --- Replica Operations ---
    def add_replica(
        self,
        db: Session,
        object_id: str,
        node_id: str,
        checksum: str,
        version: int,
        status: str = "HEALTHY"
    ) -> ReplicaModel:
        # Check if replica for this node already exists
        replica = db.query(ReplicaModel).filter(
            ReplicaModel.object_id == object_id,
            ReplicaModel.node_id == node_id
        ).first()

        if replica:
            replica.checksum = checksum
            replica.version = version
            replica.status = status
            replica.last_verified_at = utc_now()
        else:
            replica = ReplicaModel(
                object_id=object_id,
                node_id=node_id,
                checksum=checksum,
                version=version,
                status=status,
                created_at=utc_now(),
                last_verified_at=utc_now()
            )
            db.add(replica)

        db.commit()
        db.refresh(replica)
        return replica

    def get_replicas_for_object(self, db: Session, object_id: str) -> List[ReplicaModel]:
        return db.query(ReplicaModel).filter(ReplicaModel.object_id == object_id).all()

    def get_healthy_replicas_for_object(self, db: Session, object_id: str) -> List[ReplicaModel]:
        """Fetch replicas that are marked HEALTHY on nodes that are currently ONLINE."""
        return db.query(ReplicaModel).join(NodeModel).filter(
            ReplicaModel.object_id == object_id,
            ReplicaModel.status == "HEALTHY",
            NodeModel.status == "ONLINE",
            NodeModel.is_partitioned == False
        ).all()

    def update_replica_status(self, db: Session, replica_id: str, status: str):
        replica = db.query(ReplicaModel).filter(ReplicaModel.id == replica_id).first()
        if replica:
            replica.status = status
            replica.last_verified_at = utc_now()
            db.commit()

    def remove_replica(self, db: Session, object_id: str, node_id: str):
        replica = db.query(ReplicaModel).filter(
            ReplicaModel.object_id == object_id,
            ReplicaModel.node_id == node_id
        ).first()
        if replica:
            db.delete(replica)
            db.commit()

    # --- Repair Jobs ---
    def create_repair_job(
        self,
        db: Session,
        object_id: str,
        object_name: str,
        source_node_id: Optional[str],
        target_node_id: str,
        reason: str
    ) -> RepairJobModel:
        job = RepairJobModel(
            object_id=object_id,
            object_name=object_name,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            reason=reason,
            status="IN_PROGRESS",
            progress_percent=10,
            started_at=utc_now()
        )
        db.add(job)
        db.commit()
        db.refresh(job)
        return job

    def update_repair_job(
        self,
        db: Session,
        job_id: str,
        status: str,
        progress_percent: int,
        bytes_transferred: int = 0,
        error_message: Optional[str] = None
    ):
        job = db.query(RepairJobModel).filter(RepairJobModel.id == job_id).first()
        if job:
            job.status = status
            job.progress_percent = progress_percent
            job.bytes_transferred = bytes_transferred
            if error_message:
                job.error_message = error_message
            if status in ("COMPLETED", "FAILED"):
                job.completed_at = utc_now()
            db.commit()

    def list_repair_jobs(self, db: Session, limit: int = 50) -> List[RepairJobModel]:
        return db.query(RepairJobModel).order_by(desc(RepairJobModel.started_at)).limit(limit).all()

    # --- Integrity Check Logs ---
    def log_integrity_check(
        self,
        db: Session,
        object_id: str,
        object_name: str,
        node_id: str,
        expected_checksum: str,
        actual_checksum: str,
        is_valid: bool,
        status: str,
        action_taken: Optional[str] = None
    ) -> IntegrityCheckModel:
        record = IntegrityCheckModel(
            object_id=object_id,
            object_name=object_name,
            node_id=node_id,
            expected_checksum=expected_checksum,
            actual_checksum=actual_checksum,
            is_valid=is_valid,
            status=status,
            action_taken=action_taken,
            checked_at=utc_now()
        )
        db.add(record)
        db.commit()
        db.refresh(record)
        return record

    def list_integrity_checks(self, db: Session, limit: int = 50) -> List[IntegrityCheckModel]:
        return db.query(IntegrityCheckModel).order_by(desc(IntegrityCheckModel.checked_at)).limit(limit).all()

    # --- System Events ---
    def log_event(
        self,
        db: Session,
        level: str,
        component: str,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        details_str = json.dumps(details) if details else None
        event = SystemEventModel(
            timestamp=utc_now(),
            level=level.upper(),
            component=component,
            message=message,
            details=details_str
        )
        db.add(event)
        try:
            db.commit()
        except Exception:
            db.rollback()

    def list_events(self, db: Session, limit: int = 100) -> List[SystemEventModel]:
        return db.query(SystemEventModel).order_by(desc(SystemEventModel.timestamp)).limit(limit).all()

metadata_service = MetadataService()
