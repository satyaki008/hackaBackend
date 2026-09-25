"""Storage Rebalancing API Router."""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.rebalancer import storage_rebalancer
from app.schemas.schemas import RebalanceStatusResponse

router = APIRouter(prefix="/rebalance", tags=["Rebalance"])

@router.get("/status", response_model=RebalanceStatusResponse)
def get_rebalance_status(db: Session = Depends(get_db)):
    """Analyze cluster capacity skew and return recommendations for data rebalancing."""
    return storage_rebalancer.analyze_balance(db)

@router.post("")
async def trigger_rebalance(
    max_moves: int = Query(5, ge=1, le=20, description="Maximum replicas to migrate in this pass"),
    db: Session = Depends(get_db)
):
    """Execute safe verified replica migrations from overloaded to underutilized nodes."""
    result = await storage_rebalancer.execute_rebalance(db, max_migrations=max_moves)
    return result
