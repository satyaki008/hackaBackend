"""Unit Tests for Advanced Upgrades: S3 Gateway, Chaos Monkey, Benchmark & Erasure Matrix."""
import pytest
from fastapi.testclient import TestClient

from backend.app.main import app

client = TestClient(app)

def test_s3_gateway_workflow():
    """Test S3 API: CreateBucket, PutObject, HeadObject, GetObject, DeleteObject."""
    # 1. Create Bucket
    res = client.put("/api/s3/test-bucket")
    assert res.status_code == 200

    # 2. PutObject
    test_content = b"Hello from AWS S3 Compatible API on ResiStore!"
    res = client.put(
        "/api/s3/test-bucket/docs/hello.txt",
        content=test_content,
        headers={"Content-Type": "text/plain"}
    )
    assert res.status_code == 200
    assert "ETag" in res.headers

    # 3. HeadObject
    res = client.head("/api/s3/test-bucket/docs/hello.txt")
    assert res.status_code == 200
    assert res.headers["Content-Length"] == str(len(test_content))

    # 4. GetObject
    res = client.get("/api/s3/test-bucket/docs/hello.txt")
    assert res.status_code == 200
    assert res.content == test_content

    # 5. List Objects in Bucket
    res = client.get("/api/s3/test-bucket")
    assert res.status_code == 200
    data = res.json()
    assert "ListBucketResult" in data
    assert any(item["Key"] == "docs/hello.txt" for item in data["ListBucketResult"]["Contents"])

    # 6. DeleteObject
    res = client.delete("/api/s3/test-bucket/docs/hello.txt")
    assert res.status_code == 204

def test_erasure_matrix_and_simulation():
    """Test Cauchy Matrix inspection and live 2-shard loss reconstruction."""
    # 1. Inspect Matrix
    res = client.get("/api/resistore/erasure/matrix")
    assert res.status_code == 200
    data = res.json()
    assert data["k_data_shards"] == 4
    assert data["m_parity_shards"] == 2
    assert len(data["generator_matrix"]) == 6

    # 2. Simulate losing Shard 0 (D0) and Shard 4 (P0)
    res = client.post(
        "/api/resistore/erasure/simulate-recovery",
        json={
            "message": "Testing Cauchy Reed-Solomon Recovery in Hackathon Demo!",
            "lost_shards": [0, 4]
        }
    )
    assert res.status_code == 200
    res_data = res.json()
    assert res_data["integrity_verified"] is True
    assert "SUCCESS" in res_data["verdict"]

def test_chaos_and_benchmark_endpoints():
    """Test Chaos Monkey trigger and performance benchmark runner."""
    # 1. Benchmark Run
    res = client.post("/api/resilience/benchmark/run")
    assert res.status_code == 200
    bm_data = res.json()
    assert "iops" in bm_data
    assert "throughput_mb_s" in bm_data
    assert "eco_aware_metrics" in bm_data
    assert bm_data["eco_aware_metrics"]["erasure_coding_storage_reduction_pct"] == 50.0

    # 2. Chaos Trigger
    res = client.post("/api/resilience/chaos/trigger?kill_nodes_count=1&corrupt_replicas=true")
    assert res.status_code == 200
    chaos_data = res.json()
    assert "mttr_ms" in chaos_data
    assert chaos_data["data_loss_percentage"] == 0.0
    assert len(chaos_data["timeline"]) >= 2
