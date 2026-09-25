"""Interactive Demo Scenarios API Router."""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.services.demo_runner import demo_runner
from backend.app.services.metadata import metadata_service
from backend.app.services.storage_node_client import storage_node_client

router = APIRouter(prefix="/demo", tags=["Demo"])

@router.post("/auto-repair")
async def run_auto_repair_demo(db: Session = Depends(get_db)):
    """Execute the full 10-step distributed node failure and automatic self-healing recovery pipeline."""
    result = await demo_runner.run_auto_healing_demo(db)
    return result

@router.post("/corruption-heal")
async def run_corruption_demo(db: Session = Depends(get_db)):
    """Execute the corruption injection, SHA-256 detection, and peer self-healing pipeline."""
    result = await demo_runner.run_corruption_healing_demo(db)
    return result

@router.post("/reset")
async def reset_cluster_state(db: Session = Depends(get_db)):
    """Reset all nodes to ONLINE, clear latency and partitions for a fresh testing state."""
    nodes = metadata_service.list_nodes(db)
    for n in nodes:
        await storage_node_client.set_node_status(n.url, "ONLINE")
        await storage_node_client.set_node_latency(n.url, 0)
        await storage_node_client.set_node_partition(n.url, False)
        node_rec = metadata_service.get_node(db, n.id)
        if node_rec:
            node_rec.status = "ONLINE"
            node_rec.simulated_latency_ms = 0
            node_rec.is_partitioned = False
    db.commit()

    metadata_service.log_event(
        db,
        level="INFO",
        component="SYSTEM",
        message="Demo state reset: All nodes restored to ONLINE and partitions cleared."
    )
    return {"status": "SUCCESS", "message": "Cluster state successfully reset to all ONLINE."}
