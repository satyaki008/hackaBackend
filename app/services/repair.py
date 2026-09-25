"""Automatic Replica Repair Manager.
Handles detection of missing/corrupted replicas and autonomous self-healing across nodes.
"""
import asyncio
from typing import List, Tuple, Dict, Any, Optional
from sqlalchemy.orm import Session

from app.models.models import ObjectModel, NodeModel, ReplicaModel, RepairJobModel
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.utils.hashing import calculate_sha256, verify_checksum
from app.config import settings

class ReplicaRepairManager:
    def __init__(self):
        self._repair_semaphore = asyncio.Semaphore(5)  # Limit concurrent repairs

    async def repair_object(
        self,
        db: Session,
        object_id: str,
        reason: str = "UNDER_REPLICATED"
    ) -> Tuple[bool, str]:
        """Self-heal an under-replicated or degraded object by allocating new replicas on healthy nodes."""
        lock = await metadata_service.get_object_lock(object_id)
        async with lock:
            async with self._repair_semaphore:
                obj = metadata_service.get_object(db, object_id)
                if not obj:
                    return False, f"Object {object_id} not found."
                
                # Fetch all replicas
                all_replicas = metadata_service.get_replicas_for_object(db, object_id)
                
                # Identify healthy replicas hosted on currently ONLINE nodes
                healthy_replicas = []
                nodes_with_replicas = set()
                
                for r in all_replicas:
                    node = metadata_service.get_node(db, r.node_id)
                    if node and node.status == "ONLINE" and not node.is_partitioned and r.status == "HEALTHY":
                        healthy_replicas.append((r, node))
                        nodes_with_replicas.add(node.id)

                needed_replicas = obj.replication_factor - len(healthy_replicas)
                if needed_replicas <= 0:
                    metadata_service.update_object_status(db, object_id, "HEALTHY")
                    return True, "Object already satisfies replication factor."

                if len(healthy_replicas) == 0:
                    metadata_service.update_object_status(db, object_id, "CORRUPTED")
                    metadata_service.log_event(
                        db,
                        level="ERROR",
                        component="REPAIR_MANAGER",
                        message=f"CRITICAL: Zero healthy replicas remain for object '{obj.name}' ({object_id})! Data loss imminent."
                    )
                    return False, "No healthy source replicas available for self-healing."

                # Find healthy target nodes that don't currently have a healthy replica
                healthy_online_nodes = metadata_service.get_healthy_online_nodes(db)
                available_targets = [
                    n for n in healthy_online_nodes
                    if n.id not in nodes_with_replicas and n.free_capacity_bytes >= obj.size_bytes
                ]

                # Sort candidates by least used capacity
                available_targets.sort(key=lambda n: (n.used_capacity_bytes, n.object_count))

                if not available_targets:
                    metadata_service.update_object_status(db, object_id, "DEGRADED")
                    msg = f"Insufficient spare nodes to restore target replication factor ({len(healthy_replicas)}/{obj.replication_factor} active). Awaiting recovered nodes."
                    metadata_service.log_event(
                        db,
                        level="WARNING",
                        component="REPAIR_MANAGER",
                        message=f"Object '{obj.name}': {msg}"
                    )
                    return False, msg

                # Perform repair using first available target and first healthy source
                source_replica, source_node = healthy_replicas[0]
                target_node = available_targets[0]

                # Create repair job in database
                job = metadata_service.create_repair_job(
                    db=db,
                    object_id=object_id,
                    object_name=obj.name,
                    source_node_id=source_node.id,
                    target_node_id=target_node.id,
                    reason=reason
                )

                metadata_service.log_event(
                    db,
                    level="INFO",
                    component="REPAIR_MANAGER",
                    message=f"Starting self-healing repair for '{obj.name}' ({object_id}): copying from {source_node.id} to {target_node.id}..."
                )

                # 1. Read from source node
                ok_read, payload, read_err = await storage_node_client.read_replica(source_node.url, object_id)
                if not ok_read or not payload:
                    err = f"Failed reading from source node {source_node.id}: {read_err}"
                    metadata_service.update_repair_job(db, job.id, "FAILED", 20, error_message=err)
                    metadata_service.log_event(db, level="ERROR", component="REPAIR_MANAGER", message=err)
                    return False, err

                metadata_service.update_repair_job(db, job.id, "IN_PROGRESS", 50, bytes_transferred=len(payload))

                # 2. Verify source checksum matches authoritative checksum
                source_hash = calculate_sha256(payload)
                if not verify_checksum(source_hash, obj.checksum):
                    err = f"Source replica on {source_node.id} failed checksum verification during repair!"
                    metadata_service.update_repair_job(db, job.id, "FAILED", 60, error_message=err)
                    metadata_service.log_event(db, level="ERROR", component="REPAIR_MANAGER", message=err)
                    return False, err

                # 3. Write to target node
                ok_write, write_res = await storage_node_client.write_replica(target_node.url, object_id, payload)
                if not ok_write:
                    err = f"Failed writing to target node {target_node.id}: {write_res.get('error')}"
                    metadata_service.update_repair_job(db, job.id, "FAILED", 80, error_message=err)
                    metadata_service.log_event(db, level="ERROR", component="REPAIR_MANAGER", message=err)
                    return False, err

                # 4. Verify target checksum
                target_hash = write_res.get("checksum", "")
                if not verify_checksum(target_hash, obj.checksum):
                    err = f"Target replica checksum mismatch after write to {target_node.id}"
                    metadata_service.update_repair_job(db, job.id, "FAILED", 90, error_message=err)
                    metadata_service.log_event(db, level="ERROR", component="REPAIR_MANAGER", message=err)
                    return False, err

                # 5. Update metadata
                metadata_service.add_replica(
                    db=db,
                    object_id=object_id,
                    node_id=target_node.id,
                    checksum=obj.checksum,
                    version=obj.version,
                    status="HEALTHY"
                )

                # Re-check replica count
                new_healthy_count = len(healthy_replicas) + 1
                if new_healthy_count >= obj.replication_factor:
                    metadata_service.update_object_status(db, object_id, "HEALTHY")
                else:
                    metadata_service.update_object_status(db, object_id, "DEGRADED")

                metadata_service.update_repair_job(
                    db,
                    job.id,
                    "COMPLETED",
                    100,
                    bytes_transferred=len(payload)
                )

                success_msg = f"Replica repair completed successfully: object '{obj.name}' restored to target node {target_node.id} from {source_node.id}."
                metadata_service.log_event(
                    db,
                    level="SUCCESS",
                    component="REPAIR_MANAGER",
                    message=success_msg
                )
                return True, success_msg

    async def handle_node_failure(self, db: Session, failed_node_id: str) -> List[Dict[str, Any]]:
        """Invoked when a node fails: find all affected objects and trigger automatic healing."""
        # Find all replicas on this failed node
        affected_replicas = db.query(ReplicaModel).filter(
            ReplicaModel.node_id == failed_node_id
        ).all()
        
        results = []
        if not affected_replicas:
            return results

        metadata_service.log_event(
            db,
            level="WARNING",
            component="REPAIR_MANAGER",
            message=f"Node {failed_node_id} failure detected. Identified {len(affected_replicas)} affected object replica(s). Launching self-healing..."
        )

        for rep in affected_replicas:
            ok, msg = await self.repair_object(db, rep.object_id, reason=f"NODE_FAILURE ({failed_node_id})")
            results.append({"object_id": rep.object_id, "success": ok, "message": msg})

        return results

    async def scan_and_repair_all(self, db: Session) -> List[Dict[str, Any]]:
        """Global scan for any under-replicated objects."""
        objects = db.query(ObjectModel).all()
        results = []
        for obj in objects:
            healthy = metadata_service.get_healthy_replicas_for_object(db, obj.id)
            if len(healthy) < obj.replication_factor:
                ok, msg = await self.repair_object(db, obj.id, reason="UNDER_REPLICATED")
                results.append({"object_id": obj.id, "success": ok, "message": msg})
        return results

repair_manager = ReplicaRepairManager()
