"""Storage Rebalancer Service.
Identifies capacity skew across storage nodes and performs non-destructive, safe replica migration.
Rule: Never delete the source replica until the target replica is fully written, checksum-verified, and registered in metadata.
"""
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from app.models.models import NodeModel, ReplicaModel, ObjectModel
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.utils.hashing import calculate_sha256, verify_checksum
from app.config import settings

class StorageRebalancer:
    def __init__(self):
        self.last_rebalance_time: Optional[datetime] = None

    def analyze_balance(self, db: Session) -> Dict[str, Any]:
        """Compute cluster utilization skew and identify candidate replica migrations."""
        nodes = metadata_service.get_healthy_online_nodes(db)
        if len(nodes) < 2:
            return {
                "is_balanced": True,
                "skew_percentage": 0.0,
                "overloaded_nodes": [],
                "underutilized_nodes": [],
                "node_utilizations": {},
                "recommended_moves": []
            }

        utilizations = {}
        for n in nodes:
            pct = (n.used_capacity_bytes / n.total_capacity_bytes * 100.0) if n.total_capacity_bytes > 0 else 0.0
            utilizations[n.id] = round(pct, 2)

        vals = list(utilizations.values())
        max_u = max(vals)
        min_u = min(vals)
        skew = round(max_u - min_u, 2)
        avg_u = sum(vals) / len(vals)

        threshold = settings.REBALANCE_SKEW_THRESHOLD * 100.0  # e.g. 20.0%
        is_balanced = skew < threshold

        overloaded = [nid for nid, u in utilizations.items() if u > (avg_u + 5.0) and u > min_u]
        underutilized = [nid for nid, u in utilizations.items() if u < (avg_u - 5.0) or u == min_u]

        # Find candidate replicas on overloaded nodes that are absent on underutilized nodes
        moves = []
        if not is_balanced and overloaded and underutilized:
            for source_id in overloaded:
                source_replicas = db.query(ReplicaModel).filter(
                    ReplicaModel.node_id == source_id,
                    ReplicaModel.status == "HEALTHY"
                ).all()

                for rep in source_replicas:
                    for target_id in underutilized:
                        if target_id == source_id:
                            continue
                        # Check if target already has this replica
                        has_replica = db.query(ReplicaModel).filter(
                            ReplicaModel.object_id == rep.object_id,
                            ReplicaModel.node_id == target_id
                        ).first()

                        if not has_replica:
                            obj = metadata_service.get_object(db, rep.object_id)
                            if obj:
                                moves.append({
                                    "object_id": rep.object_id,
                                    "object_name": obj.name,
                                    "size_bytes": obj.size_bytes,
                                    "source_node_id": source_id,
                                    "target_node_id": target_id
                                })
                                break

        return {
            "is_balanced": is_balanced,
            "skew_percentage": skew,
            "overloaded_nodes": overloaded,
            "underutilized_nodes": underutilized,
            "node_utilizations": utilizations,
            "recommended_moves": moves,
            "last_rebalance_time": self.last_rebalance_time
        }

    async def execute_rebalance(self, db: Session, max_migrations: int = 5) -> Dict[str, Any]:
        """Perform verified safe migration of replicas from heavily loaded to lightly loaded nodes."""
        analysis = self.analyze_balance(db)
        moves = analysis.get("recommended_moves", [])

        if not moves:
            return {
                "status": "BALANCED",
                "message": f"Cluster is balanced (skew: {analysis.get('skew_percentage')}%), no migrations required.",
                "migrated_count": 0,
                "migrations": []
            }

        successful_migrations = []
        limit_moves = moves[:max_migrations]

        for m in limit_moves:
            object_id = m["object_id"]
            source_id = m["source_node_id"]
            target_id = m["target_node_id"]

            lock = await metadata_service.get_object_lock(object_id)
            async with lock:
                obj = metadata_service.get_object(db, object_id)
                source_node = metadata_service.get_node(db, source_id)
                target_node = metadata_service.get_node(db, target_id)

                if not obj or not source_node or not target_node:
                    continue

                metadata_service.log_event(
                    db,
                    level="INFO",
                    component="REBALANCER",
                    message=f"Migrating replica '{obj.name}' ({object_id}) from {source_id} to {target_id} to balance storage load..."
                )

                # 1. Read from source node
                ok_read, payload, read_err = await storage_node_client.read_replica(source_node.url, object_id)
                if not ok_read or not payload:
                    metadata_service.log_event(
                        db,
                        level="ERROR",
                        component="REBALANCER",
                        message=f"Failed reading from source node {source_id}: {read_err}"
                    )
                    continue

                # 2. Verify source checksum
                if not verify_checksum(calculate_sha256(payload), obj.checksum):
                    metadata_service.log_event(
                        db,
                        level="ERROR",
                        component="REBALANCER",
                        message=f"Source replica on {source_id} failed checksum verification during rebalance."
                    )
                    continue

                # 3. Write to target node
                ok_write, write_res = await storage_node_client.write_replica(target_node.url, object_id, payload)
                if not ok_write or not verify_checksum(write_res.get("checksum", ""), obj.checksum):
                    metadata_service.log_event(
                        db,
                        level="ERROR",
                        component="REBALANCER",
                        message=f"Target node {target_id} failed write/checksum verification during rebalance."
                    )
                    continue

                # 4. Safe metadata transition: create/update replica on target node
                metadata_service.add_replica(
                    db=db,
                    object_id=object_id,
                    node_id=target_id,
                    checksum=obj.checksum,
                    version=obj.version,
                    status="HEALTHY"
                )

                # 5. ONLY AFTER target is confirmed healthy and saved in DB, safely delete from source node
                await storage_node_client.delete_replica(source_node.url, object_id)
                metadata_service.remove_replica(db, object_id, source_id)

                metadata_service.log_event(
                    db,
                    level="SUCCESS",
                    component="REBALANCER",
                    message=f"Rebalanced replica '{obj.name}' from {source_id} to {target_id} successfully."
                )

                successful_migrations.append({
                    "object_id": object_id,
                    "object_name": obj.name,
                    "source": source_id,
                    "target": target_id,
                    "bytes": len(payload)
                })

        self.last_rebalance_time = datetime.now(timezone.utc)

        return {
            "status": "COMPLETED",
            "message": f"Successfully migrated {len(successful_migrations)} replica(s) across nodes.",
            "migrated_count": len(successful_migrations),
            "migrations": successful_migrations
        }

storage_rebalancer = StorageRebalancer()
