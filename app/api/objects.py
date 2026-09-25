"""Objects API Router.
Handles client uploads, downloads with failover, deletions, and inspection.
"""
import uuid
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Query, Response, status
from fastapi.responses import StreamingResponse
import io
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.services.metadata import metadata_service
from backend.app.services.replication import replication_manager
from backend.app.services.storage_node_client import storage_node_client
from backend.app.schemas.schemas import ObjectResponse, ObjectUploadResponse, ReplicaResponse
from backend.app.config import settings

router = APIRouter(prefix="/objects", tags=["Objects"])

@router.post("", response_model=ObjectUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_object(
    file: UploadFile = File(...),
    replication_factor: int = Form(settings.DEFAULT_REPLICATION_FACTOR),
    db: Session = Depends(get_db)
):
    """Upload a new object. Automatically generates ID, calculates SHA-256, selects healthy nodes, and replicates."""
    content = await file.read()
    if len(content) == 0:
        raise HTTPException(status_code=400, detail="Cannot upload an empty file.")

    object_id = str(uuid.uuid4())
    filename = file.filename or f"file_{object_id[:8]}"
    content_type = file.content_type or "application/octet-stream"

    ok, obj, written_nodes, message = await replication_manager.replicate_object(
        db=db,
        object_id=object_id,
        filename=filename,
        data=content,
        content_type=content_type,
        replication_factor=replication_factor
    )

    if not ok:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=message
        )

    return ObjectUploadResponse(
        object_id=obj.id,
        name=obj.name,
        size_bytes=obj.size_bytes,
        checksum=obj.checksum,
        version=obj.version,
        replication_factor=obj.replication_factor,
        status=obj.status,
        nodes_written=written_nodes,
        message=message
    )

@router.get("", response_model=List[ObjectResponse])
def list_objects(
    search: Optional[str] = Query(None, description="Search by object name"),
    db: Session = Depends(get_db)
):
    """List all stored objects with replica details and health status."""
    objects = metadata_service.list_objects(db, search=search)
    results = []
    for obj in objects:
        healthy_reps = metadata_service.get_healthy_replicas_for_object(db, obj.id)
        rep_responses = [ReplicaResponse.from_orm(r) for r in obj.replicas]
        results.append(ObjectResponse(
            id=obj.id,
            name=obj.name,
            content_type=obj.content_type,
            size_bytes=obj.size_bytes,
            checksum=obj.checksum,
            version=obj.version,
            replication_factor=obj.replication_factor,
            status=obj.status,
            created_at=obj.created_at,
            updated_at=obj.updated_at,
            healthy_replica_count=len(healthy_reps),
            replicas=rep_responses
        ))
    return results

@router.get("/{object_id}", response_model=ObjectResponse)
def get_object_details(object_id: str, db: Session = Depends(get_db)):
    """Fetch metadata and replica layout for a specific object."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")
    
    healthy_reps = metadata_service.get_healthy_replicas_for_object(db, obj.id)
    rep_responses = [ReplicaResponse.from_orm(r) for r in obj.replicas]
    return ObjectResponse(
        id=obj.id,
        name=obj.name,
        content_type=obj.content_type,
        size_bytes=obj.size_bytes,
        checksum=obj.checksum,
        version=obj.version,
        replication_factor=obj.replication_factor,
        status=obj.status,
        created_at=obj.created_at,
        updated_at=obj.updated_at,
        healthy_replica_count=len(healthy_reps),
        replicas=rep_responses
    )

@router.get("/{object_id}/download")
async def download_object(object_id: str, db: Session = Depends(get_db)):
    """Download an object with automatic failover across healthy storage nodes."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")

    # Find healthy replicas on online nodes
    healthy_replicas = metadata_service.get_healthy_replicas_for_object(db, object_id)
    if not healthy_replicas:
        # Check if any replica exists at all
        any_replicas = metadata_service.get_replicas_for_object(db, object_id)
        if not any_replicas:
            raise HTTPException(status_code=404, detail="No replicas found for this object.")
        raise HTTPException(
            status_code=503,
            detail="All storage nodes hosting replicas for this object are currently offline or partitioned."
        )

    # Attempt download with transparent failover
    last_error = ""
    for replica in healthy_replicas:
        node = metadata_service.get_node(db, replica.node_id)
        if not node:
            continue

        ok, data, err = await storage_node_client.read_replica(node.url, object_id)
        if ok and data is not None:
            # Successfully retrieved!
            return Response(
                content=data,
                media_type=obj.content_type,
                headers={
                    "Content-Disposition": f'attachment; filename="{obj.name}"',
                    "X-Replica-Source-Node": node.id,
                    "X-Checksum-SHA256": obj.checksum
                }
            )
        else:
            last_error = err
            metadata_service.log_event(
                db,
                level="WARNING",
                component="STORAGE_GATEWAY",
                message=f"Failover reading object {object_id} from {node.id}: {err}. Trying next replica..."
            )

    raise HTTPException(
        status_code=502,
        detail=f"Failed to read from any replica. Last error: {last_error}"
    )

@router.delete("/{object_id}")
async def delete_object(object_id: str, db: Session = Depends(get_db)):
    """Delete all replicas of an object across all nodes and remove metadata."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")

    replicas = metadata_service.get_replicas_for_object(db, object_id)
    for r in replicas:
        node = metadata_service.get_node(db, r.node_id)
        if node:
            await storage_node_client.delete_replica(node.url, object_id)

    metadata_service.delete_object(db, object_id)
    metadata_service.log_event(
        db,
        level="INFO",
        component="STORAGE_GATEWAY",
        message=f"Deleted object '{obj.name}' ({object_id}) and all associated replicas."
    )

    return {"status": "DELETED", "object_id": object_id, "message": "Object and all replicas deleted successfully."}

@router.post("/{object_id}/corrupt-replica")
async def inject_corruption(
    object_id: str,
    node_id: Optional[str] = Query(None, description="Optional specific node to corrupt"),
    db: Session = Depends(get_db)
):
    """Test endpoint: Corrupts bytes on disk of an object replica to test self-healing."""
    obj = metadata_service.get_object(db, object_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Object not found.")

    replicas = metadata_service.get_replicas_for_object(db, object_id)
    if not replicas:
        raise HTTPException(status_code=404, detail="No replicas found.")

    target_rep = None
    if node_id:
        target_rep = next((r for r in replicas if r.node_id == node_id), None)
    if not target_rep:
        target_rep = replicas[0]

    node = metadata_service.get_node(db, target_rep.node_id)
    ok, res = await storage_node_client.corrupt_replica(node.url, object_id)
    if not ok:
        raise HTTPException(status_code=500, detail=f"Failed to corrupt replica: {res}")

    return {
        "status": "CORRUPTED",
        "object_id": object_id,
        "node_id": node.id,
        "message": f"Successfully tampered with replica on {node.id}. Run integrity check to observe self-healing!"
    }
