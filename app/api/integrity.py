"""Integrity Verification API Router."""
from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.metadata import metadata_service
from app.services.integrity import integrity_checker
from app.schemas.schemas import IntegrityStatusResponse, IntegrityCheckResponse

router = APIRouter(prefix="/integrity", tags=["Integrity"])

@router.post("/check", response_model=IntegrityStatusResponse)
async def run_integrity_check(
    auto_repair: bool = Query(True, description="Automatically repair corrupted replicas found"),
    db: Session = Depends(get_db)
):
    """Trigger a deep cluster-wide SHA-256 cryptographic audit across all nodes. Automatically self-heals corrupted replicas."""
    result = await integrity_checker.run_audit(db, auto_repair=auto_repair)
    return result

@router.get("/status", response_model=IntegrityStatusResponse)
def get_integrity_status(db: Session = Depends(get_db)):
    """Fetch current cluster integrity status and recent audit history."""
    recent_checks = metadata_service.list_integrity_checks(db, limit=20)
    
    healthy_cnt = sum(1 for c in recent_checks if c.status == "HEALTHY")
    corrupt_cnt = sum(1 for c in recent_checks if c.status == "CORRUPTED")
    missing_cnt = sum(1 for c in recent_checks if c.status == "MISSING")
    repaired_cnt = sum(1 for c in recent_checks if c.action_taken and "repaired" in c.action_taken.lower())

    return IntegrityStatusResponse(
        total_replicas_checked=len(recent_checks),
        healthy_replicas=healthy_cnt,
        corrupted_replicas=corrupt_cnt,
        missing_replicas=missing_cnt,
        auto_repaired_count=repaired_cnt,
        recent_checks=[IntegrityCheckResponse.from_orm(c) for c in recent_checks]
    )

@router.get("/logs", response_model=List[IntegrityCheckResponse])
def get_integrity_logs(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db)
):
    """Get chronological integrity scan logs."""
    checks = metadata_service.list_integrity_checks(db, limit=limit)
    return [IntegrityCheckResponse.from_orm(c) for c in checks]
