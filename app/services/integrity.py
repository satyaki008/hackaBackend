"""Data Integrity Verification & Self-Healing Service.
Performs periodic and on-demand SHA-256 audits, detects bit rot or intentional tampering, and self-heals corrupted replicas.
"""
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session

from app.models.models import ObjectModel, ReplicaModel, NodeModel, IntegrityCheckModel
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.utils.hashing import verify_checksum, calculate_sha256

class IntegrityChecker:
    def __init__(self):
        pass

    async def verify_replica(
        self,
        db: Session,
        replica: ReplicaModel,
        auto_repair: bool = True
    ) -> IntegrityCheckModel:
        """Audit a single replica against the authoritative SHA-256 checksum."""
        obj = metadata_service.get_object(db, replica.object_id)
        node = metadata_service.get_node(db, replica.node_id)

        if not obj or not node:
            return None

        # If node is offline, we cannot check disk
        if node.status != "ONLINE" or node.is_partitioned:
            return None

        # Ask node for real-time checksum of replica on disk
        ok, res = await storage_node_client.check_checksum(node.url, replica.object_id)
        
        if not ok or not res.get("exists", False):
            # Replica is missing from disk!
            replica.status = "MISSING"
            db.commit()
            
            check_rec = metadata_service.log_integrity_check(
                db=db,
                object_id=obj.id,
                object_name=obj.name,
                node_id=node.id,
                expected_checksum=obj.checksum,
                actual_checksum="MISSING_FILE",
                is_valid=False,
                status="MISSING",
                action_taken="Scheduled for auto-repair" if auto_repair else None
            )
            
            metadata_service.log_event(
                db,
                level="ERROR",
                component="INTEGRITY_CHECKER",
                message=f"Missing replica detected for object '{obj.name}' on node {node.id}!"
            )
            
            if auto_repair:
                # Trigger repair
                from app.services.repair import repair_manager
                await repair_manager.repair_object(db, obj.id, reason="MISSING_REPLICA")
                
            return check_rec

        actual_checksum = res.get("checksum", "")
        expected_checksum = obj.checksum
        is_healthy = verify_checksum(actual_checksum, expected_checksum)

        if is_healthy:
            replica.status = "HEALTHY"
            replica.checksum = actual_checksum
            db.commit()
            
            check_rec = metadata_service.log_integrity_check(
                db=db,
                object_id=obj.id,
                object_name=obj.name,
                node_id=node.id,
                expected_checksum=expected_checksum,
                actual_checksum=actual_checksum,
                is_valid=True,
                status="HEALTHY",
                action_taken="Checksum verified intact."
            )
            return check_rec

        # CORRUPTED REPLICA DETECTED!
        replica.status = "CORRUPTED"
        db.commit()

        action_taken = None
        metadata_service.log_event(
            db,
            level="ERROR",
            component="INTEGRITY_CHECKER",
            message=f"CORRUPTED REPLICA DETECTED on node {node.id} for object '{obj.name}' ({obj.id})! Expected {expected_checksum[:12]}..., got {actual_checksum[:12]}..."
        )

        if auto_repair:
            # Self-heal immediately: find a healthy peer replica
            healthy_peers = [
                r for r in obj.replicas
                if r.node_id != node.id and r.status == "HEALTHY"
            ]

            repaired = False
            for peer_rep in healthy_peers:
                peer_node = metadata_service.get_node(db, peer_rep.node_id)
                if peer_node and peer_node.status == "ONLINE" and not peer_node.is_partitioned:
                    # Read healthy data
                    ok_read, peer_data, _ = await storage_node_client.read_replica(peer_node.url, obj.id)
                    if ok_read and peer_data and verify_checksum(calculate_sha256(peer_data), expected_checksum):
                        # Overwrite corrupted file on current node
                        ok_write, _ = await storage_node_client.write_replica(node.url, obj.id, peer_data)
                        if ok_write:
                            replica.status = "HEALTHY"
                            replica.checksum = expected_checksum
                            db.commit()
                            action_taken = f"Auto-repaired corrupted replica from peer {peer_node.id}"
                            metadata_service.log_event(
                                db,
                                level="SUCCESS",
                                component="INTEGRITY_CHECKER",
                                message=f"Successfully repaired corrupted replica on node {node.id} using peer {peer_node.id}."
                            )
                            repaired = True
                            break

            if not repaired:
                # If we couldn't overwrite directly, trigger global repair to allocate onto another healthy node
                from app.services.repair import repair_manager
                ok_rep, rep_msg = await repair_manager.repair_object(db, obj.id, reason="CORRUPTED_REPLICA")
                action_taken = f"Allocated new replacement replica: {rep_msg}"

        check_rec = metadata_service.log_integrity_check(
            db=db,
            object_id=obj.id,
            object_name=obj.name,
            node_id=node.id,
            expected_checksum=expected_checksum,
            actual_checksum=actual_checksum,
            is_valid=False,
            status="CORRUPTED",
            action_taken=action_taken
        )
        return check_rec

    async def run_audit(self, db: Session, auto_repair: bool = True) -> Dict[str, Any]:
        """Audit all replicas in the storage cluster."""
        replicas = db.query(ReplicaModel).all()
        checks: List[IntegrityCheckModel] = []
        
        healthy_count = 0
        corrupted_count = 0
        missing_count = 0
        repaired_count = 0

        for r in replicas:
            rec = await self.verify_replica(db, r, auto_repair=auto_repair)
            if rec:
                checks.append(rec)
                if rec.status == "HEALTHY":
                    healthy_count += 1
                elif rec.status == "CORRUPTED":
                    corrupted_count += 1
                    if rec.action_taken and "repaired" in rec.action_taken.lower():
                        repaired_count += 1
                elif rec.status == "MISSING":
                    missing_count += 1

        metadata_service.log_event(
            db,
            level="INFO",
            component="INTEGRITY_CHECKER",
            message=f"Completed cluster integrity audit: {healthy_count} healthy, {corrupted_count} corrupted, {missing_count} missing. Repaired: {repaired_count}."
        )

        return {
            "total_replicas_checked": len(checks),
            "healthy_replicas": healthy_count,
            "corrupted_replicas": corrupted_count,
            "missing_replicas": missing_count,
            "auto_repaired_count": repaired_count,
            "recent_checks": checks[:20]
        }

integrity_checker = IntegrityChecker()
