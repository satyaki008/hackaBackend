"""Replication Manager.
Handles node selection, concurrent parallel replica writes, quorum validation, and rollback.
"""
import asyncio
from typing import List, Tuple, Dict, Any
from sqlalchemy.orm import Session

from app.models.models import NodeModel, ObjectModel
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.utils.hashing import calculate_sha256, verify_checksum
from app.config import settings

class ReplicationManager:
    def __init__(self):
        pass

    def select_placement_nodes(
        self,
        db: Session,
        replication_factor: int,
        required_bytes: int,
        exclude_node_ids: List[str] = None
    ) -> List[NodeModel]:
        """Select best candidate nodes for replica placement based on health, capacity, and current load."""
        exclude = set(exclude_node_ids or [])
        healthy_nodes = metadata_service.get_healthy_online_nodes(db)
        
        candidates = [
            n for n in healthy_nodes
            if n.id not in exclude and n.free_capacity_bytes >= required_bytes
        ]
        
        # Sort by used capacity ascending (least utilized first)
        candidates.sort(key=lambda n: (n.used_capacity_bytes, n.object_count))
        
        return candidates[:replication_factor]

    async def replicate_object(
        self,
        db: Session,
        object_id: str,
        filename: str,
        data: bytes,
        content_type: str,
        replication_factor: int = settings.DEFAULT_REPLICATION_FACTOR,
        version: int = 1
    ) -> Tuple[bool, ObjectModel, List[str], str]:
        """Upload and replicate object across N healthy storage nodes concurrently."""
        file_size = len(data)
        expected_checksum = calculate_sha256(data)
        
        # Select target nodes
        nodes = self.select_placement_nodes(db, replication_factor, file_size)
        if len(nodes) == 0:
            return False, None, [], "No healthy online storage nodes available with sufficient capacity."
            
        write_quorum = min(settings.MIN_QUORUM_WRITE, len(nodes))
        
        # Execute concurrent writes across chosen nodes
        async def write_to_node(node: NodeModel):
            ok, resp = await storage_node_client.write_replica(node.url, object_id, data)
            if ok:
                remote_checksum = resp.get("checksum", "")
                if verify_checksum(remote_checksum, expected_checksum):
                    return node.id, True, resp
                else:
                    return node.id, False, {"error": "Checksum verification failed after write"}
            return node.id, False, resp

        write_tasks = [write_to_node(node) for node in nodes]
        results = await asyncio.gather(*write_tasks)
        
        successful_nodes = []
        failed_nodes = []
        
        for node_id, ok, info in results:
            if ok:
                successful_nodes.append(node_id)
            else:
                failed_nodes.append((node_id, info))
                
        # Check Quorum
        if len(successful_nodes) < write_quorum:
            # Quorum failed: Rollback successful writes to avoid partial orphaned data
            rollback_tasks = [
                storage_node_client.delete_replica(
                    next(n.url for n in nodes if n.id == nid),
                    object_id
                )
                for nid in successful_nodes
            ]
            if rollback_tasks:
                await asyncio.gather(*rollback_tasks, return_exceptions=True)
                
            metadata_service.log_event(
                db,
                level="ERROR",
                component="REPLICATION_MANAGER",
                message=f"Write quorum failed for object '{filename}' ({object_id}). Successful: {len(successful_nodes)}/{len(nodes)} (required {write_quorum}). Rolled back."
            )
            return False, None, [], f"Write quorum not satisfied ({len(successful_nodes)}/{write_quorum} succeeded)."

        # Quorum succeeded: Save metadata transaction
        obj = metadata_service.create_object(
            db=db,
            object_id=object_id,
            name=filename,
            content_type=content_type,
            size_bytes=file_size,
            checksum=expected_checksum,
            replication_factor=replication_factor,
            version=version
        )
        
        # Add replicas in DB
        for node_id in successful_nodes:
            metadata_service.add_replica(
                db=db,
                object_id=object_id,
                node_id=node_id,
                checksum=expected_checksum,
                version=version,
                status="HEALTHY"
            )
            
        is_fully_replicated = len(successful_nodes) >= replication_factor
        status_msg = "HEALTHY" if is_fully_replicated else "DEGRADED"
        metadata_service.update_object_status(db, object_id, status_msg)
        
        metadata_service.log_event(
            db,
            level="SUCCESS",
            component="REPLICATION_MANAGER",
            message=f"Object '{filename}' stored with {len(successful_nodes)} replicas on nodes: {', '.join(successful_nodes)}."
        )
        
        return True, obj, successful_nodes, "Object stored and replicated successfully."

replication_manager = ReplicationManager()
