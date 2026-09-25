"""ResiStore Advanced API Router.
Consistent Hash Ring, Reed-Solomon RS (4+2) Erasure Coding, Merkle Trees, and MongoDB Atlas Cloud Node inspection.
"""
import uuid
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, Response, status
from sqlalchemy.orm import Session

from pydantic import BaseModel
from backend.app.database import get_db
from backend.app.services.consistent_hash import consistent_hash_ring
from backend.app.services.auto_healer import auto_healer
from backend.app.services.mongo_storage_adapter import mongo_storage, mongo_cluster_manager
from backend.app.services.metadata import metadata_service
from backend.app.models.models import ObjectModel, ChunkModel, BucketModel, NodeModel

class CloudNodeConfigRequest(BaseModel):
    uri: str

router = APIRouter(prefix="/resistore", tags=["ResiStore"])

@router.get("/ring")
def get_hash_ring_topology():
    """Get the live Consistent Hash Ring topology with virtual node mapping and ring distribution."""
    return consistent_hash_ring.get_ring_topology()

@router.get("/node1/mongo")
def get_mongo_node_stats():
    """Inspect Node 1 live MongoDB Atlas Cloud status and stored chunks."""
    return mongo_storage.get_stats()

@router.get("/nodes/cloud")
def get_all_cloud_nodes():
    """Get status of all 4 nodes showing MongoDB Atlas cloud vs standby status."""
    return mongo_cluster_manager.get_all_node_stats()

@router.post("/nodes/{node_id}/cloud-config")
def configure_cloud_node(node_id: str, config: CloudNodeConfigRequest, db: Session = Depends(get_db)):
    """Dynamically connect node 1, 2, 3, or 4 to a MongoDB Atlas cluster URI."""
    if node_id not in ["node-1", "node-2", "node-3", "node-4"]:
        raise HTTPException(status_code=400, detail=f"Invalid node ID: {node_id}")

    ok, message, latency = mongo_cluster_manager.register_node(node_id, config.uri)
    if not ok:
        raise HTTPException(status_code=400, detail=f"Failed to connect to MongoDB cluster: {message}")

    # Update node name in SQLite database
    node = db.query(NodeModel).filter(NodeModel.id == node_id).first()
    if node:
        node.name = f"{node_id.title()} [MongoDB Atlas Cloud - Active]"
        node.status = "ONLINE"
        db.commit()

    metadata_service.log_event(
        db,
        level="SUCCESS",
        component="CLOUD_ADAPTER",
        message=f"Connected {node_id} to MongoDB Atlas cloud database. Ping: {latency}ms."
    )

    return {
        "node_id": node_id,
        "is_connected": ok,
        "latency_ms": latency,
        "message": message
    }

@router.post("/objects")
async def upload_erasure_object(
    file: UploadFile = File(...),
    bucket_name: str = Form("default-bucket"),
    storage_policy: str = Form("ERASURE_CODING_RS_4_2"),  # or REPLICA_3X
    db: Session = Depends(get_db)
):
    """Upload object using Reed-Solomon RS (4+2) Erasure Coding or 3x Replication.
    Calculates Merkle tree, generates 6 shards (4 data + 2 parity), and maps to consistent hash ring.
    Node 1 stores shards directly in MongoDB Atlas Cloud!
    """
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Cannot upload an empty file.")

    object_id = f"resi-{uuid.uuid4().hex[:12]}"
    filename = file.filename or f"file_{object_id[:8]}"

    if storage_policy == "ERASURE_CODING_RS_4_2":
        ok, obj, shards, msg = await auto_healer.store_erasure_object(
            db=db,
            object_id=object_id,
            filename=filename,
            data=content,
            content_type=file.content_type or "application/octet-stream",
            bucket_name=bucket_name
        )
        if not ok:
            raise HTTPException(status_code=500, detail=msg)

        return {
            "object_id": obj.id,
            "name": obj.name,
            "size_bytes": obj.size_bytes,
            "checksum": obj.checksum,
            "storage_policy": obj.storage_policy,
            "merkle_root": obj.merkle_root,
            "shards_count": len(shards),
            "shards": shards,
            "message": "Object successfully erasure coded with RS(4+2) and dispersed across consistent hash ring."
        }
    else:
        # Fallback to standard 3x replica
        from backend.app.services.replication import replication_manager
        ok, obj, nodes, msg = await replication_manager.replicate_object(
            db=db,
            object_id=object_id,
            filename=filename,
            data=content,
            content_type=file.content_type or "application/octet-stream",
            replication_factor=3
        )
        if not ok:
            raise HTTPException(status_code=500, detail=msg)
        return {
            "object_id": obj.id,
            "name": obj.name,
            "size_bytes": obj.size_bytes,
            "checksum": obj.checksum,
            "storage_policy": "REPLICA_3X",
            "nodes_written": nodes,
            "message": msg
        }

@router.get("/objects/{object_id}/chunks")
def inspect_object_chunks(object_id: str, db: Session = Depends(get_db)):
    """Inspect all RS(4+2) chunks of an object with their assigned nodes and integrity statuses."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")

    chunks = db.query(ChunkModel).filter(ChunkModel.object_id == object_id).order_by(ChunkModel.chunk_index).all()
    return {
        "object_id": obj.id,
        "name": obj.name,
        "size_bytes": obj.size_bytes,
        "checksum": obj.checksum,
        "merkle_root": obj.merkle_root,
        "storage_policy": obj.storage_policy,
        "chunks": [
            {
                "chunk_id": c.id,
                "index": c.chunk_index,
                "type": c.chunk_type,
                "node_id": c.node_id,
                "size_bytes": c.size_bytes,
                "checksum": c.checksum,
                "status": c.status
            }
            for c in chunks
        ]
    }

@router.get("/objects/{object_id}/download")
async def download_erasure_object(object_id: str, db: Session = Depends(get_db)):
    """Download and reconstruct erasure-coded object using ANY 4 surviving chunks."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")

    if obj.storage_policy == "ERASURE_CODING_RS_4_2":
        ok, data, msg = await auto_healer.read_erasure_object(db, object_id)
        if not ok or not data:
            raise HTTPException(status_code=503, detail=msg)
        return Response(
            content=data,
            media_type=obj.content_type,
            headers={
                "Content-Disposition": f'attachment; filename="{obj.name}"',
                "X-Decoded-Via": "Reed-Solomon-RS-4-2",
                "X-Checksum-SHA256": obj.checksum
            }
        )
    else:
        # Standard replica download
        from backend.app.api.objects import download_object
        return await download_object(object_id, db)

@router.post("/objects/{object_id}/heal")
async def trigger_erasure_heal(object_id: str):
    """Trigger the Auto-Healing Worker to reconstruct missing/corrupted chunks using Reed-Solomon math."""
    ok, msg = await auto_healer.heal_erasure_object(object_id)
    return {"success": ok, "message": msg}

@router.get("/buckets")
def list_buckets(db: Session = Depends(get_db)):
    """List S3-style storage buckets."""
    buckets = db.query(BucketModel).all()
    if not buckets:
        # Seed default bucket
        default_b = BucketModel(name="default-bucket", storage_policy="ERASURE_CODING_RS_4_2")
        db.add(default_b)
        db.commit()
        buckets = [default_b]
    return [
        {
            "name": b.name,
            "storage_policy": b.storage_policy,
            "object_count": b.object_count,
            "used_bytes": b.used_bytes,
            "created_at": b.created_at
        }
        for b in buckets
    ]

@router.get("/erasure/matrix")
def get_erasure_matrix_info():
    """Get the active Galois Field GF(2^8) Cauchy Generator Matrix for RS(4+2)."""
    from backend.app.services.erasure_coding import build_cauchy_matrix
    gen_matrix = build_cauchy_matrix(4, 2)
    return {
        "k_data_shards": 4,
        "m_parity_shards": 2,
        "total_shards": 6,
        "field": "Galois Field GF(2^8) [x^8 + x^4 + x^3 + x^2 + 1]",
        "generator_matrix": gen_matrix,
        "matrix_labels": ["D0", "D1", "D2", "D3", "P0", "P1"],
        "max_tolerated_losses": 2,
        "storage_efficiency_pct": 66.7,
        "overhead_pct": 50.0
    }

class ShardRecoverySimRequest(BaseModel):
    message: str = "ResiStore: Fault-Tolerant Distributed Storage Engine with Reed-Solomon Erasure Coding"
    lost_shards: List[int] = [0, 4]  # e.g. D0 and P0 lost

@router.post("/erasure/simulate-recovery")
def simulate_erasure_recovery(req: ShardRecoverySimRequest):
    """Simulate loss of any 1 or 2 shards and watch Galois Field Cauchy matrix invert and reconstruct."""
    from backend.app.services.erasure_coding import erasure_coding_engine
    if len(req.lost_shards) > 2:
        raise HTTPException(status_code=400, detail="RS(4+2) can only tolerate up to 2 lost shards.")

    data_bytes = req.message.encode("utf-8")
    chunks, orig_size = erasure_coding_engine.encode(data_bytes)
    
    # Simulate loss by filtering surviving chunks
    surviving_chunks = []
    shards_status = []
    for idx, c in enumerate(chunks):
        if idx in req.lost_shards:
            shards_status.append({"index": idx, "label": f"D{idx}" if idx < 4 else f"P{idx-4}", "status": "DESTROYED"})
        else:
            surviving_chunks.append(c)
            shards_status.append({"index": idx, "label": f"D{idx}" if idx < 4 else f"P{idx-4}", "status": "HEALTHY", "bytes_len": len(c.data)})

    # Reconstruct
    reconstructed = erasure_coding_engine.decode(surviving_chunks, original_size=orig_size)
    is_match = (reconstructed == data_bytes)

    return {
        "original_message": req.message,
        "lost_shards": req.lost_shards,
        "shards": shards_status,
        "reconstructed_message": reconstructed.decode("utf-8", errors="replace"),
        "integrity_verified": is_match,
        "math_engine": "Cauchy GF(2^8) Matrix Inversion",
        "verdict": "SUCCESS: 100% of data recovered without loss" if is_match else "FAILED"
    }

