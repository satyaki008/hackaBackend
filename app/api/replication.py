"""Replication & Self-Healing API Router."""
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.services.metadata import metadata_service
from backend.app.services.repair import repair_manager
from backend.app.schemas.schemas import RepairJobResponse, RepairTriggerRequest
from backend.app.config import settings

router = APIRouter(prefix="/replication", tags=["Replication"])

@router.get("/status")
def get_replication_status(db: Session = Depends(get_db)):
    """Get cluster replication health, under-replicated objects, and active repair status."""
    objects = metadata_service.list_objects(db)
    
    total_objects = len(objects)
    healthy_objects = 0
    degraded_objects = 0
    corrupted_objects = 0
    
    under_replicated_list = []
    
    for obj in objects:
        healthy_reps = metadata_service.get_healthy_replicas_for_object(db, obj.id)
        count = len(healthy_reps)
        if count >= obj.replication_factor:
            healthy_objects += 1
        elif count == 0:
            corrupted_objects += 1
            under_replicated_list.append({
                "object_id": obj.id,
                "name": obj.name,
                "current_replicas": count,
                "target_replicas": obj.replication_factor,
                "status": "CRITICAL"
            })
        else:
            degraded_objects += 1
            under_replicated_list.append({
                "object_id": obj.id,
                "name": obj.name,
                "current_replicas": count,
                "target_replicas": obj.replication_factor,
                "status": "DEGRADED"
            })

    return {
        "target_replication_factor": settings.DEFAULT_REPLICATION_FACTOR,
        "total_objects": total_objects,
        "healthy_objects": healthy_objects,
        "degraded_objects": degraded_objects,
        "corrupted_objects": corrupted_objects,
        "under_replicated_objects": under_replicated_list
    }

@router.post("/repair")
async def trigger_repair(
    payload: Optional[RepairTriggerRequest] = None,
    db: Session = Depends(get_db)
):
    """Trigger manual or batch self-healing repair for under-replicated objects."""
    if payload and payload.object_id:
        ok, msg = await repair_manager.repair_object(db, payload.object_id, reason="MANUAL_TRIGGER")
        return {"status": "SUCCESS" if ok else "FAILED", "message": msg}
    else:
        results = await repair_manager.scan_and_repair_all(db)
        return {
            "status": "COMPLETED",
            "repaired_count": sum(1 for r in results if r["success"]),
            "details": results
        }

@router.get("/jobs", response_model=List[RepairJobResponse])
def list_repair_jobs(limit: int = Query(50, ge=1, le=200), db: Session = Depends(get_db)):
    """List recent and active background repair jobs with progress percentages."""
    jobs = metadata_service.list_repair_jobs(db, limit=limit)
    return jobs
