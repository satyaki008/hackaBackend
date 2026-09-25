"""System Health, Events, and Statistics API Router."""
import time
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models.models import ObjectModel, ReplicaModel, NodeModel, RepairJobModel
from app.services.metadata import metadata_service
from app.schemas.schemas import SystemStatsResponse, SystemEventResponse
from app.config import settings

router = APIRouter(tags=["System"])

SERVER_START_TIME = time.time()

@router.get("/health")
def get_system_health(db: Session = Depends(get_db)):
    """General health check endpoint."""
    nodes = metadata_service.list_nodes(db)
    online_count = sum(1 for n in nodes if n.status == "ONLINE" and not n.is_partitioned)
    return {
        "status": "healthy" if online_count >= settings.MIN_QUORUM_WRITE else "degraded",
        "online_nodes": online_count,
        "total_nodes": len(nodes),
        "version": settings.VERSION,
        "app_name": settings.APP_NAME
    }

@router.get("/stats", response_model=SystemStatsResponse)
def get_system_stats(db: Session = Depends(get_db)):
    """Fetch aggregated system metrics for the dashboard overview."""
    nodes = metadata_service.list_nodes(db)
    objects = db.query(ObjectModel).all()
    replicas = db.query(ReplicaModel).all()
    
    total_storage = sum(n.total_capacity_bytes for n in nodes)
    used_storage = sum(n.used_capacity_bytes for n in nodes)
    free_storage = max(0, total_storage - used_storage)
    
    online_nodes = sum(1 for n in nodes if n.status == "ONLINE" and not n.is_partitioned)
    failed_nodes = sum(1 for n in nodes if n.status == "OFFLINE" or n.is_partitioned)
    degraded_nodes = sum(1 for n in nodes if n.status == "DEGRADED")
    
    active_repairs = db.query(RepairJobModel).filter(RepairJobModel.status == "IN_PROGRESS").count()
    corrupted_reps = sum(1 for r in replicas if r.status == "CORRUPTED")
    
    # Compute overall cluster status
    if online_nodes == 0 or (online_nodes < settings.MIN_QUORUM_WRITE):
        sys_status = "CRITICAL"
    elif active_repairs > 0 or corrupted_reps > 0 or degraded_nodes > 0:
        sys_status = "REPAIRING" if active_repairs > 0 else "DEGRADED"
    else:
        sys_status = "OPERATIONAL"

    return SystemStatsResponse(
        total_objects=len(objects),
        total_replicas=len(replicas),
        total_storage_bytes=total_storage,
        used_storage_bytes=used_storage,
        free_storage_bytes=free_storage,
        total_nodes=len(nodes),
        healthy_nodes=online_nodes,
        failed_nodes=failed_nodes,
        degraded_nodes=degraded_nodes,
        active_repairs=active_repairs,
        corrupted_replicas=corrupted_reps,
        replication_factor=settings.DEFAULT_REPLICATION_FACTOR,
        uptime_seconds=round(time.time() - SERVER_START_TIME, 1),
        system_status=sys_status
    )

@router.get("/events", response_model=List[SystemEventResponse])
def get_system_events(
    limit: int = Query(100, ge=1, le=500),
    level: Optional[str] = Query(None, description="Filter by event level (INFO, WARNING, ERROR, SUCCESS)"),
    db: Session = Depends(get_db)
):
    """Retrieve live audit log of system operations and self-healing actions."""
    events = metadata_service.list_events(db, limit=limit)
    if level:
        events = [e for e in events if e.level.upper() == level.upper()]
    return [SystemEventResponse.from_orm(e) for e in events]
