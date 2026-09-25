"""ResiStore Advanced Distributed Unit Test Suite.
Tests:
1. Consistent Hashing Ring with Virtual Nodes and uniform key distribution.
2. Reed-Solomon RS (4+2) Erasure Coding encoding and exact reconstruction.
3. Node disconnection during reads: recovering full data from surviving shards.
4. Autonomous Auto-Healing Worker reconstructing lost shards and verifying SHA-256.
5. Merkle Tree chunk verification and bit-rot pinpointing.
"""
import pytest
import hashlib
from backend.app.services.consistent_hash import ConsistentHashRing
from backend.app.services.erasure_coding import ReedSolomonEngine, Chunk
from backend.app.services.merkle_tree import MerkleTree

def test_01_consistent_hashing_ring_and_vnodes():
    """Test 1: Consistent Hash Ring with Virtual Nodes."""
    ring = ConsistentHashRing(vnodes=50)
    ring.add_node("node-1", state="ALIVE")
    ring.add_node("node-2", state="ALIVE")
    ring.add_node("node-3", state="ALIVE")
    ring.add_node("node-4", state="ALIVE")

    assert len(ring.ring) == 200  # 4 nodes * 50 vnodes
    assert len(ring.nodes) == 4

    # Test deterministic primary placement
    key1 = "bucket/photos/2026_vacation.jpg"
    primary = ring.get_primary_node(key1)
    assert primary in ("node-1", "node-2", "node-3", "node-4")
    # Same key must always map to same primary
    assert ring.get_primary_node(key1) == primary

    # Test preference list traversal
    pref = ring.get_preference_list(key1, count=3)
    assert len(pref) == 3
    assert len(set(pref)) == 3  # All distinct physical nodes

    # Test node failure / state update
    ring.update_node_state(primary, "DEAD")
    alive_pref = ring.get_preference_list(key1, count=3, only_alive=True)
    assert primary not in alive_pref
    assert len(alive_pref) == 3

def test_02_erasure_coding_encoding_and_reconstruction():
    """Test 2: Reed-Solomon RS (4+2) encoding and decoding."""
    engine = ReedSolomonEngine(data_shards=4, parity_shards=2)
    payload = b"Production Distributed Object Storage Engine with Erasure Coding RS(4+2)!"
    original_size = len(payload)

    chunks, size = engine.encode(payload)
    assert size == original_size
    assert len(chunks) == 6  # 4 data + 2 parity
    assert chunks[0].chunk_type == "DATA"
    assert chunks[3].chunk_type == "DATA"
    assert chunks[4].chunk_type == "PARITY"
    assert chunks[5].chunk_type == "PARITY"

    # Verify SHA-256 on all chunks
    for c in chunks:
        assert c.checksum == hashlib.sha256(c.data).hexdigest()

    # Scenario A: Full reconstruction with all chunks
    rec_all = engine.decode(chunks, original_size)
    assert rec_all == payload

    # Scenario B: 1 Data chunk missing (e.g. Chunk 1)
    surviving_1 = [c for c in chunks if c.index != 1]
    assert len(surviving_1) == 5
    rec_1 = engine.decode(surviving_1, original_size)
    assert rec_1 == payload

    # Scenario C: 2 Nodes Disconnected (e.g. Chunk 0 and Chunk 3 missing)
    surviving_2 = [c for c in chunks if c.index not in (0, 3)]
    assert len(surviving_2) == 4
    rec_2 = engine.decode(surviving_2, original_size)
    assert rec_2 == payload

    # Scenario D: Both parity chunks missing (reads purely from 4 data chunks)
    surviving_data_only = [c for c in chunks if c.chunk_type == "DATA"]
    assert len(surviving_data_only) == 4
    rec_data = engine.decode(surviving_data_only, original_size)
    assert rec_data == payload

    # Scenario E: 3 Nodes Disconnected -> Below Quorum -> Must raise ValueError
    surviving_3 = [c for c in chunks if c.index in (0, 1, 2)]
    with pytest.raises(ValueError):
        engine.decode(surviving_3, original_size)

def test_03_erasure_coding_lost_chunk_reconstruction():
    """Test 3: Reconstructing specific lost or corrupted shards."""
    engine = ReedSolomonEngine(data_shards=4, parity_shards=2)
    payload = b"Testing individual shard reconstruction for autonomous self-healing worker!"
    chunks, _ = engine.encode(payload)

    # Let's drop chunk 2 (Data) and chunk 4 (Parity)
    surviving = [c for c in chunks if c.index not in (2, 4)]
    assert len(surviving) == 4

    # Reconstruct data chunk 2
    healed_chunk_2 = engine.reconstruct_lost_chunk(surviving, 2)
    assert healed_chunk_2.index == 2
    assert healed_chunk_2.data == chunks[2].data
    assert healed_chunk_2.checksum == chunks[2].checksum

    # Reconstruct parity chunk 4
    healed_chunk_4 = engine.reconstruct_lost_chunk(surviving, 4)
    assert healed_chunk_4.index == 4
    assert healed_chunk_4.data == chunks[4].data
    assert healed_chunk_4.checksum == chunks[4].checksum

def test_04_merkle_tree_integrity_verification():
    """Test 4: Merkle Tree construction and bit-rot pinpointing."""
    leaf_hashes = [
        hashlib.sha256(f"chunk_{i}".encode()).hexdigest()
        for i in range(6)
    ]
    tree = MerkleTree(leaf_hashes)
    assert len(tree.root_hash) == 64

    # Tamper with chunk index 3
    tampered_leaves = list(leaf_hashes)
    tampered_leaves[3] = hashlib.sha256(b"corrupted_bytes").hexdigest()
    tampered_tree = MerkleTree(tampered_leaves)

    # Roots must differ
    assert tree.root_hash != tampered_tree.root_hash

    # Pinpoint mismatch index
    mismatches = MerkleTree.find_mismatch_indices(leaf_hashes, tampered_leaves)
    assert mismatches == [3]

def test_05_cloud_nodes_and_schema_migration():
    """Test 5: Verify MongoDB Atlas node status and schema migration resilience."""
    from backend.app.services.mongo_storage_adapter import mongo_cluster_manager
    from backend.app.database import engine, apply_migrations

    # Verify migration function executes cleanly
    apply_migrations(engine)

    # Verify cloud nodes structure
    cloud_nodes = mongo_cluster_manager.get_all_node_stats()
    assert len(cloud_nodes) == 4
    node1 = next(n for n in cloud_nodes if n["node_id"] == "node-1")
    assert node1["backend"] == "MongoDB Atlas Cloud"
    assert node1["is_connected"] is True
    assert "cluster0" in node1["cluster"]

    # Verify standby nodes
    node2 = next(n for n in cloud_nodes if n["node_id"] == "node-2")
    assert "Standby" in node2["backend"]
