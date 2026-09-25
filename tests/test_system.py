"""Comprehensive Automated Test Suite for Self-Healing Distributed Object Storage System.
Tests all 15 required core distributed behaviors and self-healing mechanics.
"""
import asyncio
import io
import uuid
import pytest
import httpx
from sqlalchemy.orm import Session

from backend.app.main import app
from backend.app.models.models import ObjectModel, NodeModel, ReplicaModel
from backend.app.services.metadata import metadata_service
from backend.app.services.replication import replication_manager
from backend.app.services.repair import repair_manager
from backend.app.services.integrity import integrity_checker
from backend.app.services.rebalancer import storage_rebalancer
from backend.app.services.demo_runner import demo_runner
from backend.app.services.storage_node_client import storage_node_client
from backend.app.utils.hashing import calculate_sha256, verify_checksum

@pytest.mark.asyncio
async def test_01_upload_and_replication(db_session: Session):
    """Test 1 & 4: Upload object, verify SHA-256 calculation and 3 replicas created."""
    sample_content = b"Hello Aegis Distributed Storage System! This is a test file."
    expected_sha = calculate_sha256(sample_content)
    obj_id = f"test-obj-{uuid.uuid4().hex[:8]}"

    ok, obj, written_nodes, msg = await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="hello.txt",
        data=sample_content,
        content_type="text/plain",
        replication_factor=3
    )

    assert ok is True
    assert obj.id == obj_id
    assert obj.checksum == expected_sha
    assert len(written_nodes) == 3
    assert obj.status == "HEALTHY"

    # Verify replicas in database
    replicas = metadata_service.get_replicas_for_object(db_session, obj_id)
    assert len(replicas) == 3
    for r in replicas:
        assert r.status == "HEALTHY"
        assert r.checksum == expected_sha

@pytest.mark.asyncio
async def test_02_download_object(db_session: Session):
    """Test 2: Download object and verify retrieved bytes match exactly."""
    sample_content = b"Downloadable content payload"
    obj_id = f"test-dl-{uuid.uuid4().hex[:8]}"
    await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="download_me.txt",
        data=sample_content,
        content_type="text/plain",
        replication_factor=3
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(f"/api/objects/{obj_id}/download")
        assert resp.status_code == 200
        assert resp.content == sample_content
        assert resp.headers["X-Checksum-SHA256"] == calculate_sha256(sample_content)

@pytest.mark.asyncio
async def test_03_download_failover(db_session: Session):
    """Test 3: If one replica node is down, download automatically falls back to peer replica."""
    sample_content = b"Failover resilient content"
    obj_id = f"test-failover-{uuid.uuid4().hex[:8]}"
    await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="failover.txt",
        data=sample_content,
        content_type="text/plain",
        replication_factor=3
    )

    replicas = metadata_service.get_replicas_for_object(db_session, obj_id)
    first_node_id = replicas[0].node_id
    first_node = metadata_service.get_node(db_session, first_node_id)

    # Take down first node
    first_node.status = "OFFLINE"
    db_session.commit()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get(f"/api/objects/{obj_id}/download")
        assert resp.status_code == 200
        assert resp.content == sample_content

    # Restore node
    first_node.status = "ONLINE"
    db_session.commit()

@pytest.mark.asyncio
async def test_04_delete_object(db_session: Session):
    """Test 4: Delete object and verify deletion across all nodes and metadata."""
    sample_content = b"Temporary data to delete"
    obj_id = f"test-del-{uuid.uuid4().hex[:8]}"
    await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="temp.bin",
        data=sample_content,
        content_type="application/octet-stream",
        replication_factor=2
    )

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete(f"/api/objects/{obj_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "DELETED"

    # Verify object no longer exists in DB
    db_session.expire_all()
    obj = metadata_service.get_object(db_session, obj_id)
    assert obj is None
    reps = metadata_service.get_replicas_for_object(db_session, obj_id)
    assert len(reps) == 0

@pytest.mark.asyncio
async def test_05_node_failure_and_recovery(db_session: Session):
    """Test 5 & 6: Node failure and recovery endpoints."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # Fail node-2
        resp = await client.post("/api/nodes/node-2/fail")
        assert resp.status_code == 200
        assert resp.json()["status"] == "OFFLINE"

        db_session.expire_all()
        node = metadata_service.get_node(db_session, "node-2")
        assert node.status == "OFFLINE"

        # Recover node-2
        resp_rec = await client.post("/api/nodes/node-2/recover")
        assert resp_rec.status_code == 200
        assert resp_rec.json()["status"] == "ONLINE"

        db_session.expire_all()
        node_rec = metadata_service.get_node(db_session, "node-2")
        assert node_rec.status == "ONLINE"

@pytest.mark.asyncio
async def test_06_automatic_replica_repair(db_session: Session):
    """Test 7 & 8: Node failure triggers automatic self-healing repair to spare node."""
    content = b"Data requiring auto-healing upon node crash."
    obj_id = f"test-heal-{uuid.uuid4().hex[:8]}"
    ok, obj, written_nodes, _ = await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="heal_test.txt",
        data=content,
        content_type="text/plain",
        replication_factor=3
    )
    assert ok is True
    assert len(written_nodes) == 3

    # Fail one of the nodes holding a replica
    failed_node_id = written_nodes[0]
    failed_node = metadata_service.get_node(db_session, failed_node_id)
    failed_node.status = "OFFLINE"
    db_session.commit()

    # Trigger repair manager
    repair_results = await repair_manager.handle_node_failure(db_session, failed_node_id)
    assert len(repair_results) >= 1
    assert repair_results[0]["success"] is True

    # Check that healthy replica count is restored to 3
    db_session.expire_all()
    healthy_reps = metadata_service.get_healthy_replicas_for_object(db_session, obj_id)
    assert len(healthy_reps) == 3
    # Verify the repaired replica is on the 4th node (which didn't hold one before)
    repaired_node_ids = [r.node_id for r in healthy_reps]
    assert failed_node_id not in repaired_node_ids

    # Recover the failed node for subsequent tests
    failed_node.status = "ONLINE"
    db_session.commit()

@pytest.mark.asyncio
async def test_07_corrupted_replica_detection_and_repair(db_session: Session):
    """Test 9 & 10: Inject disk byte corruption, detect via SHA-256 audit, and auto-repair from peer."""
    content = b"Pristine authentic payload before corruption test."
    expected_sha = calculate_sha256(content)
    obj_id = f"test-corrupt-{uuid.uuid4().hex[:8]}"

    ok, obj, nodes, _ = await replication_manager.replicate_object(
        db=db_session,
        object_id=obj_id,
        filename="corrupt_test.txt",
        data=content,
        content_type="text/plain",
        replication_factor=3
    )
    assert ok is True

    # Inject corruption into node 1's replica
    target_node_id = nodes[0]
    node = metadata_service.get_node(db_session, target_node_id)
    ok_corrupt, _ = await storage_node_client.corrupt_replica(node.url, obj_id)
    assert ok_corrupt is True

    # Run integrity check with auto-repair
    audit_res = await integrity_checker.run_audit(db_session, auto_repair=True)
    assert audit_res["corrupted_replicas"] >= 1
    assert audit_res["auto_repaired_count"] >= 1

    # Verify checksum on the node is restored to pristine SHA-256
    ok_chk, chk_res = await storage_node_client.check_checksum(node.url, obj_id)
    assert ok_chk is True
    assert chk_res["checksum"].lower() == expected_sha.lower()

@pytest.mark.asyncio
async def test_08_concurrent_operations(db_session: Session):
    """Test 11 & 12: Concurrent parallel uploads and reads without race conditions."""
    async def upload_worker(idx: int):
        data = f"Concurrent stream payload #{idx}".encode()
        oid = f"concurrent-obj-{uuid.uuid4().hex[:8]}-{idx}"
        ok, _, _, _ = await replication_manager.replicate_object(
            db=db_session,
            object_id=oid,
            filename=f"concurrent_{idx}.txt",
            data=data,
            content_type="text/plain",
            replication_factor=2
        )
        return ok, oid

    tasks = [upload_worker(i) for i in range(8)]
    results = await asyncio.gather(*tasks)
    assert all(r[0] for r in results)

    # Concurrent downloads
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        async def download_worker(idx: int, oid: str):
            resp = await client.get(f"/api/objects/{oid}/download")
            return resp.status_code == 200 and resp.content == f"Concurrent stream payload #{idx}".encode()

        read_tasks = [download_worker(i, results[i][1]) for i in range(8)]
        read_results = await asyncio.gather(*read_tasks)
        assert all(read_results)

@pytest.mark.asyncio
async def test_09_storage_rebalancing(db_session: Session):
    """Test 13: Storage rebalancer identifies capacity skew and safely migrates replicas."""
    analysis = storage_rebalancer.analyze_balance(db_session)
    assert "is_balanced" in analysis
    assert "node_utilizations" in analysis

    rebalance_res = await storage_rebalancer.execute_rebalance(db_session, max_migrations=2)
    assert "status" in rebalance_res

@pytest.mark.asyncio
async def test_10_network_partition(db_session: Session):
    """Test 14: Network partition simulation isolates node gracefully."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.post("/api/nodes/partition", json={
            "node_a": "node-3",
            "node_b": "controller",
            "enable_partition": True
        })
        assert resp.status_code == 200
        assert resp.json()["is_partitioned"] is True

        db_session.expire_all()
        node = metadata_service.get_node(db_session, "node-3")
        assert node.is_partitioned is True

        # Reconnect
        resp_unpart = await client.post("/api/nodes/partition", json={
            "node_a": "node-3",
            "node_b": "controller",
            "enable_partition": False
        })
        assert resp_unpart.status_code == 200
        assert resp_unpart.json()["is_partitioned"] is False

@pytest.mark.asyncio
async def test_11_interactive_demo_scenarios(db_session: Session):
    """Test 15 & 16: Complete automated 10-step self-healing and corruption demo pipelines."""
    # Test Auto-Healing Demo
    auto_heal_result = await demo_runner.run_auto_healing_demo(db_session)
    assert auto_heal_result["success"] is True
    assert len(auto_heal_result["timeline"]) >= 8

    # Test Corruption Demo
    corruption_result = await demo_runner.run_corruption_healing_demo(db_session)
    assert corruption_result["success"] is True
    assert len(corruption_result["timeline"]) >= 4
