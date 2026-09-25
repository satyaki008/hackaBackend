"""Amazon S3-Compatible REST Gateway API for ResiStore.
Allows standard S3 SDKs (AWS CLI, Boto3, MinIO Client, cURL) to interact directly
with ResiStore using standard S3 protocol semantics.

Endpoints:
- GET /s3                    -> ListBuckets
- PUT /s3/{bucket}           -> CreateBucket
- GET /s3/{bucket}           -> ListObjects
- PUT /s3/{bucket}/{key}     -> PutObject (streaming replication with ETag)
- GET /s3/{bucket}/{key}     -> GetObject (failover read with ETag)
- HEAD /s3/{bucket}/{key}    -> HeadObject (metadata headers)
- DELETE /s3/{bucket}/{key}  -> DeleteObject
"""
from datetime import datetime, timezone
import hashlib
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.models import ObjectModel, BucketModel
from app.services.metadata import metadata_service
from app.services.replication import ReplicationManager
from app.services.storage_node_client import storage_node_client

router = APIRouter(prefix="/s3", tags=["S3-Compatible Gateway"])
replication_manager = ReplicationManager()

@router.get("", summary="S3: List Buckets")
def s3_list_buckets(db: Session = Depends(get_db)):
    """List all storage buckets in the cluster."""
    buckets = db.query(BucketModel).all()
    if not buckets:
        # Auto-create default-bucket if none exists
        default_b = BucketModel(name="default-bucket")
        db.add(default_b)
        db.commit()
        buckets = [default_b]

    return {
        "ListAllMyBucketsResult": {
            "Buckets": [
                {
                    "Name": b.name,
                    "CreationDate": b.created_at.isoformat() if b.created_at else datetime.now(timezone.utc).isoformat()
                } for b in buckets
            ],
            "Owner": {
                "DisplayName": "ResiStore-Admin",
                "ID": "resistore-cluster-01"
            }
        }
    }

@router.put("/{bucket}", summary="S3: Create Bucket")
def s3_create_bucket(bucket: str, db: Session = Depends(get_db)):
    """Create a new S3 storage bucket."""
    existing = db.query(BucketModel).filter(BucketModel.name == bucket).first()
    if existing:
        return Response(status_code=200)

    b = BucketModel(name=bucket)
    db.add(b)
    db.commit()
    return Response(status_code=200, headers={"Location": f"/{bucket}"})

@router.get("/{bucket}", summary="S3: List Objects in Bucket")
def s3_list_objects(bucket: str, prefix: Optional[str] = None, db: Session = Depends(get_db)):
    """List objects stored in a specific bucket."""
    query = db.query(ObjectModel).filter(ObjectModel.bucket_name == bucket)
    if prefix:
        query = query.filter(ObjectModel.name.startswith(prefix))
    objects = query.all()

    return {
        "ListBucketResult": {
            "Name": bucket,
            "Prefix": prefix or "",
            "KeyCount": len(objects),
            "MaxKeys": 1000,
            "Contents": [
                {
                    "Key": obj.name,
                    "LastModified": obj.created_at.isoformat() if obj.created_at else datetime.now(timezone.utc).isoformat(),
                    "ETag": f'"{obj.checksum}"',
                    "Size": obj.size_bytes,
                    "StorageClass": "STANDARD" if obj.storage_policy != "ERASURE_CODING_RS_4_2" else "GLACIER_FLEXIBLE"
                } for obj in objects
            ]
        }
    }

@router.put("/{bucket}/{key:path}", summary="S3: PutObject")
async def s3_put_object(bucket: str, key: str, request: Request, db: Session = Depends(get_db)):
    """Upload an object via standard S3 PUT protocol."""
    data = await request.body()
    if not data:
        raise HTTPException(status_code=400, detail="Empty payload")

    content_type = request.headers.get("content-type", "application/octet-stream")
    md5_hash = hashlib.md5(data).hexdigest()
    sha256_hash = hashlib.sha256(data).hexdigest()

    # Ensure bucket exists
    b = db.query(BucketModel).filter(BucketModel.name == bucket).first()
    if not b:
        b = BucketModel(name=bucket)
        db.add(b)
        db.commit()

    # Replicate object across nodes
    success, obj, placement_nodes, err = await replication_manager.replicate_object(
        db=db,
        object_id=sha256_hash[:16],
        filename=key,
        data=data,
        content_type=content_type,
        replication_factor=3
    )

    if not success:
        raise HTTPException(status_code=500, detail=f"S3 PutObject replication failed: {err}")

    # Set bucket name on object
    obj.bucket_name = bucket
    db.commit()

    return Response(
        status_code=200,
        headers={
            "ETag": f'"{sha256_hash}"',
            "x-amz-version-id": str(obj.version),
            "x-amz-server-side-encryption": "AES256"
        }
    )

@router.get("/{bucket}/{key:path}", summary="S3: GetObject")
async def s3_get_object(bucket: str, key: str, db: Session = Depends(get_db)):
    """Retrieve an object via standard S3 GET protocol with automatic failover."""
    obj = db.query(ObjectModel).filter(
        ObjectModel.bucket_name == bucket,
        ObjectModel.name == key
    ).first()

    if not obj:
        # Fallback search by key alone
        obj = db.query(ObjectModel).filter(ObjectModel.name == key).first()

    if not obj:
        raise HTTPException(status_code=404, detail="NoSuchKey")

    # Read from nodes with failover
    for rep in obj.replicas:
        if rep.status != "CORRUPTED" and rep.node and rep.node.status == "ONLINE":
            ok, data, err = await storage_node_client.read_replica(rep.node.url, obj.id)
            if ok and data:
                return Response(
                    content=data,
                    media_type=obj.content_type,
                    headers={
                        "ETag": f'"{obj.checksum}"',
                        "Content-Length": str(len(data)),
                        "Accept-Ranges": "bytes",
                        "x-amz-version-id": str(obj.version)
                    }
                )

    raise HTTPException(status_code=503, detail="ServiceUnavailable: All replicas inaccessible")

@router.head("/{bucket}/{key:path}", summary="S3: HeadObject")
def s3_head_object(bucket: str, key: str, db: Session = Depends(get_db)):
    """Inspect object headers without downloading body."""
    obj = db.query(ObjectModel).filter(
        ObjectModel.bucket_name == bucket,
        ObjectModel.name == key
    ).first()

    if not obj:
        obj = db.query(ObjectModel).filter(ObjectModel.name == key).first()

    if not obj:
        raise HTTPException(status_code=404, detail="NoSuchKey")

    return Response(
        status_code=200,
        headers={
            "ETag": f'"{obj.checksum}"',
            "Content-Length": str(obj.size_bytes),
            "Content-Type": obj.content_type,
            "x-amz-version-id": str(obj.version)
        }
    )

@router.delete("/{bucket}/{key:path}", summary="S3: DeleteObject")
def s3_delete_object(bucket: str, key: str, db: Session = Depends(get_db)):
    """Delete an object from bucket."""
    obj = db.query(ObjectModel).filter(
        ObjectModel.bucket_name == bucket,
        ObjectModel.name == key
    ).first()

    if not obj:
        obj = db.query(ObjectModel).filter(ObjectModel.name == key).first()

    if obj:
        metadata_service.delete_object(db, obj.id)

    return Response(status_code=204)
