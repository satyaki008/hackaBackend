"""Storage Nodes Management & Simulation API Router."""
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.services.metadata import metadata_service
from app.services.storage_node_client import storage_node_client
from app.services.repair import repair_manager
from app.schemas.schemas import (
    NodeResponse, NodeStatusUpdate, NodeLatencyUpdate, PartitionToggleRequest
)

router = APIRouter(prefix="/nodes", tags=["Nodes"])

@router.get("", response_model=List[NodeResponse])
def list_nodes(db: Session = Depends(get_db)):
    """List all registered storage nodes with live status, latency, and disk metrics."""
    return metadata_service.list_nodes(db)

@router.get("/{node_id}", response_model=NodeResponse)
def get_node(node_id: str, db: Session = Depends(get_db)):
    """Get details for a specific storage node."""
    node = metadata_service.get_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found.")
    return node

@router.post("/{node_id}/fail", response_model=NodeResponse)
async def simulate_node_fail(node_id: str, db: Session = Depends(get_db)):
    """Simulate node crash/failure. Changes node status to OFFLINE and triggers self-healing replica repair."""
    node = metadata_service.get_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found.")

    await storage_node_client.set_node_status(node.url, "OFFLINE")
    node = metadata_service.set_node_status(db, node_id, "OFFLINE")

    # Trigger automatic replica repair immediately for testing/demo responsiveness
    await repair_manager.handle_node_failure(db, node_id)

    return node

@router.post("/{node_id}/recover", response_model=NodeResponse)
async def simulate_node_recover(node_id: str, db: Session = Depends(get_db)):
    """Simulate node recovery. Returns node to ONLINE status and checks for re-replication."""
    node = metadata_service.get_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found.")

    await storage_node_client.set_node_status(node.url, "ONLINE")
    await storage_node_client.set_node_partition(node.url, False)
    node.is_partitioned = False
    node = metadata_service.set_node_status(db, node_id, "ONLINE")

    return node

@router.post("/{node_id}/start", response_model=NodeResponse)
async def start_node(node_id: str, db: Session = Depends(get_db)):
    """Start node."""
    return await simulate_node_recover(node_id, db)

@router.post("/{node_id}/stop", response_model=NodeResponse)
async def stop_node(node_id: str, db: Session = Depends(get_db)):
    """Stop node."""
    return await simulate_node_fail(node_id, db)

@router.post("/{node_id}/slow", response_model=NodeResponse)
async def simulate_slow_node(
    node_id: str,
    payload: NodeLatencyUpdate,
    db: Session = Depends(get_db)
):
    """Simulate a degraded slow storage node with injected latency."""
    node = metadata_service.get_node(db, node_id)
    if not node:
        raise HTTPException(status_code=404, detail="Node not found.")

    await storage_node_client.set_node_latency(node.url, payload.latency_ms)
    node.simulated_latency_ms = payload.latency_ms
    if payload.latency_ms > 0:
        node.status = "DEGRADED"
    else:
        node.status = "ONLINE"
    db.commit()
    db.refresh(node)
    
    metadata_service.log_event(
        db,
        level="WARNING" if payload.latency_ms > 0 else "INFO",
        component="SIMULATOR",
        message=f"Node {node_id} latency set to {payload.latency_ms}ms (status: {node.status})."
    )
    return node

@router.post("/partition")
async def toggle_network_partition(
    payload: PartitionToggleRequest,
    db: Session = Depends(get_db)
):
    """Simulate network partition between controller/nodes or isolated node."""
    target_node = metadata_service.get_node(db, payload.node_a)
    if not target_node:
        raise HTTPException(status_code=404, detail=f"Node {payload.node_a} not found.")

    await storage_node_client.set_node_partition(target_node.url, payload.enable_partition)
    target_node.is_partitioned = payload.enable_partition
    db.commit()

    action = "isolated (partitioned)" if payload.enable_partition else "reconnected"
    metadata_service.log_event(
        db,
        level="WARNING" if payload.enable_partition else "INFO",
        component="SIMULATOR",
        message=f"Network partition toggled: {payload.node_a} is now {action}."
    )

    return {
        "node_id": payload.node_a,
        "is_partitioned": payload.enable_partition,
        "message": f"Node {payload.node_a} {action} successfully."
    }
