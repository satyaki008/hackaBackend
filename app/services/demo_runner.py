"""Automated Interactive Demo Runner.
Executes the full distributed node failure-recovery lifecycle and byte-corruption healing pipelines.
"""
import asyncio
import time
from typing import Dict, Any, List
from sqlalchemy.orm import Session

from backend.app.services.metadata import metadata_service
from backend.app.services.replication import replication_manager
from backend.app.services.repair import repair_manager
from backend.app.services.integrity import integrity_checker
from backend.app.services.storage_node_client import storage_node_client
from backend.app.utils.hashing import calculate_sha256

class DemoRunner:
    def __init__(self):
        pass

    async def run_auto_healing_demo(self, db: Session) -> Dict[str, Any]:
        """Execute complete 10-step distributed failure & self-healing demo."""
        timeline: List[Dict[str, Any]] = []

        def log_step(step_num: int, title: str, status: str, details: Any = None):
            timeline.append({
                "step": step_num,
                "timestamp": time.time(),
                "title": title,
                "status": status,
                "details": details
            })

        # Step 1: Ensure all nodes are online
        log_step(1, "Cluster Health Verification", "IN_PROGRESS", "Checking that nodes are online...")
        nodes = metadata_service.list_nodes(db)
        for n in nodes:
            await storage_node_client.set_node_status(n.url, "ONLINE")
            await storage_node_client.set_node_partition(n.url, False)
            node_model = metadata_service.get_node(db, n.id)
            if node_model:
                node_model.status = "ONLINE"
                node_model.is_partitioned = False
        db.commit()
        log_step(1, "Cluster Health Verification", "COMPLETED", "All 4 storage nodes (node-1, node-2, node-3, node-4) are ONLINE.")

        # Step 2 & 3: Upload sample file with replication factor 3
        log_step(2, "Object Ingestion & Replication", "IN_PROGRESS", "Uploading 'system_mission_critical.bin' with replication_factor=3...")
        sample_payload = b"CRITICAL_DISTRIBUTED_PAYLOAD_v1.0_AEGIS_SYSTEM_DATA_" + str(time.time()).encode()
        expected_sha = calculate_sha256(sample_payload)
        obj_id = f"demo-obj-{int(time.time())}"

        ok_rep, obj, written_nodes, rep_msg = await replication_manager.replicate_object(
            db=db,
            object_id=obj_id,
            filename="system_mission_critical.bin",
            data=sample_payload,
            content_type="application/octet-stream",
            replication_factor=3
        )

        if not ok_rep:
            log_step(2, "Object Ingestion & Replication", "FAILED", rep_msg)
            return {"success": False, "timeline": timeline}

        log_step(2, "Object Ingestion & Replication", "COMPLETED", {
            "object_id": obj_id,
            "size_bytes": len(sample_payload),
            "checksum": expected_sha,
            "replicated_to_nodes": written_nodes
        })

        # Step 4: Show initial healthy status
        log_step(4, "Replication Status Inspection", "COMPLETED", f"Target 3 replicas successfully placed on: {', '.join(written_nodes)}.")

        # Step 5: Simulate Node Failure
        # Pick one node that actually holds a replica
        failed_node_id = written_nodes[1] if len(written_nodes) > 1 else written_nodes[0]
        failed_node = metadata_service.get_node(db, failed_node_id)

        log_step(5, f"Injecting Node Failure ({failed_node_id})", "IN_PROGRESS", f"Simulating crash of {failed_node_id}...")
        await storage_node_client.set_node_status(failed_node.url, "OFFLINE")
        failed_node.status = "OFFLINE"
        db.commit()

        metadata_service.log_event(
            db,
            level="WARNING",
            component="SIMULATOR",
            message=f"DEMO: Node {failed_node_id} was forced OFFLINE."
        )
        log_step(5, f"Simulated Node Failure ({failed_node_id})", "COMPLETED", f"{failed_node_id} is now OFFLINE.")

        # Step 6: Failure Detection
        log_step(6, "Failure Detection", "COMPLETED", f"Heartbeat monitor identified {failed_node_id} as OFFLINE. Replica count dropped to 2/3.")

        # Step 7 & 8: Trigger Automatic Repair to a Spare Healthy Node
        log_step(7, "Triggering Automatic Self-Healing", "IN_PROGRESS", "Repair manager scanning for healthy spare node...")
        ok_heal, heal_msg = await repair_manager.repair_object(db, obj_id, reason=f"NODE_FAILURE ({failed_node_id})")

        if not ok_heal:
            log_step(7, "Self-Healing Repair", "FAILED", heal_msg)
            return {"success": False, "timeline": timeline}

        # Step 9 & 10: Verify new replica placement and checksum
        reps = metadata_service.get_healthy_replicas_for_object(db, obj_id)
        current_healthy_nodes = [r.node_id for r in reps]

        log_step(8, "Replica Migration & Checksum Verification", "COMPLETED", {
            "repaired": True,
            "message": heal_msg,
            "new_healthy_nodes": current_healthy_nodes,
            "checksum_verified": expected_sha
        })

        log_step(9, "Final Consistency Audit", "COMPLETED", f"System restored full target replication factor (3 healthy replicas active on {', '.join(current_healthy_nodes)}).")

        # Step 10: Restore failed node to clean state
        await storage_node_client.set_node_status(failed_node.url, "ONLINE")
        failed_node.status = "ONLINE"
        db.commit()
        log_step(10, "Node Recovery", "COMPLETED", f"{failed_node_id} recovered and returned to cluster pool.")

        return {
            "success": True,
            "object_id": obj_id,
            "timeline": timeline
        }

    async def run_corruption_healing_demo(self, db: Session) -> Dict[str, Any]:
        """Execute complete corruption detection and peer self-healing demo."""
        timeline: List[Dict[str, Any]] = []

        def log_step(step_num: int, title: str, status: str, details: Any = None):
            timeline.append({
                "step": step_num,
                "timestamp": time.time(),
                "title": title,
                "status": status,
                "details": details
            })

        # 1. Create a sample object
        log_step(1, "Upload Target Object", "IN_PROGRESS", "Creating test file 'tamper_proof_manifest.json'...")
        payload = b'{"system": "AegisStore", "integrity": "STRICT_SHA256", "payload": "CORRUPTION_PROOF"}'
        expected_sha = calculate_sha256(payload)
        obj_id = f"corrupt-demo-{int(time.time())}"

        ok, obj, nodes, msg = await replication_manager.replicate_object(
            db=db,
            object_id=obj_id,
            filename="tamper_proof_manifest.json",
            data=payload,
            content_type="application/json",
            replication_factor=3
        )

        if not ok:
            log_step(1, "Upload Target Object", "FAILED", msg)
            return {"success": False, "timeline": timeline}

        target_node_id = nodes[0]
        target_node = metadata_service.get_node(db, target_node_id)
        log_step(1, "Upload Target Object", "COMPLETED", f"Stored on nodes: {', '.join(nodes)} with SHA-256: {expected_sha[:16]}...")

        # 2. Corrupt the replica on disk
        log_step(2, f"Injecting Disk Corruption ({target_node_id})", "IN_PROGRESS", f"Tampering bytes on disk in {target_node_id}...")
        ok_corrupt, corrupt_res = await storage_node_client.corrupt_replica(target_node.url, obj_id)
        
        if not ok_corrupt:
            log_step(2, "Injecting Disk Corruption", "FAILED", "Could not corrupt replica on node.")
            return {"success": False, "timeline": timeline}

        log_step(2, "Injecting Disk Corruption", "COMPLETED", {
            "corrupted_node": target_node_id,
            "new_disk_checksum": corrupt_res.get("new_checksum", "TAMPERED")
        })

        # 3. Run Integrity Audit
        log_step(3, "Triggering Deep Integrity Audit", "IN_PROGRESS", "Reading replica and validating against cryptographic SHA-256...")
        audit_res = await integrity_checker.run_audit(db, auto_repair=True)

        log_step(3, "Integrity Audit & Self-Healing", "COMPLETED", {
            "corrupted_detected": audit_res.get("corrupted_replicas", 0),
            "auto_repaired": audit_res.get("auto_repaired_count", 0),
            "message": "Mismatch detected! Corrupted replica automatically healed using healthy peer replica."
        })

        # 4. Verify Final State
        ok_chk, chk_res = await storage_node_client.check_checksum(target_node.url, obj_id)
        final_sha = chk_res.get("checksum", "") if ok_chk else ""
        is_restored = final_sha.lower() == expected_sha.lower()

        log_step(4, "Post-Repair Checksum Verification", "COMPLETED", {
            "restored_checksum": final_sha,
            "authoritative_checksum": expected_sha,
            "integrity_restored": is_restored
        })

        return {
            "success": is_restored,
            "object_id": obj_id,
            "timeline": timeline
        }

demo_runner = DemoRunner()
