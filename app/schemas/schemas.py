from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict

# --- Node Schemas ---
class NodeBase(BaseModel):
    id: str
    name: str
    url: str
    storage_path: str
    status: str
    total_capacity_bytes: int
    used_capacity_bytes: int
    free_capacity_bytes: int
    object_count: int
    simulated_latency_ms: int = 0
    is_partitioned: bool = False

class NodeResponse(NodeBase):
    last_heartbeat: Optional[datetime] = None
    response_time_ms: float = 0.0
    model_config = ConfigDict(from_attributes=True)

class NodeStatusUpdate(BaseModel):
    status: str = Field(..., description="ONLINE, OFFLINE, DEGRADED, RECOVERING")

class NodeLatencyUpdate(BaseModel):
    latency_ms: int = Field(..., ge=0, le=10000)

class PartitionToggleRequest(BaseModel):
    node_a: str
    node_b: str
    enable_partition: bool

# --- Replica Schemas ---
class ReplicaResponse(BaseModel):
    id: str
    object_id: str
    node_id: str
    version: int
    checksum: str
    status: str
    created_at: datetime
    last_verified_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

# --- Object Schemas ---
class ObjectResponse(BaseModel):
    id: str
    name: str
    content_type: str
    size_bytes: int
    checksum: str
    version: int
    replication_factor: int
    status: str
    created_at: datetime
    updated_at: datetime
    healthy_replica_count: Optional[int] = 0
    replicas: List[ReplicaResponse] = []
    model_config = ConfigDict(from_attributes=True)

class ObjectUploadResponse(BaseModel):
    object_id: str
    name: str
    size_bytes: int
    checksum: str
    version: int
    replication_factor: int
    status: str
    nodes_written: List[str]
    message: str

# --- Repair Schemas ---
class RepairJobResponse(BaseModel):
    id: str
    object_id: str
    object_name: str
    source_node_id: Optional[str] = None
    target_node_id: str
    reason: str
    status: str
    progress_percent: int
    error_message: Optional[str] = None
    bytes_transferred: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    model_config = ConfigDict(from_attributes=True)

class RepairTriggerRequest(BaseModel):
    object_id: Optional[str] = None
    target_node_id: Optional[str] = None

# --- Integrity Schemas ---
class IntegrityCheckResponse(BaseModel):
    id: str
    object_id: str
    object_name: str
    node_id: str
    expected_checksum: str
    actual_checksum: str
    is_valid: bool
    status: str
    action_taken: Optional[str] = None
    checked_at: datetime
    model_config = ConfigDict(from_attributes=True)

class IntegrityStatusResponse(BaseModel):
    total_replicas_checked: int
    healthy_replicas: int
    corrupted_replicas: int
    missing_replicas: int
    auto_repaired_count: int
    recent_checks: List[IntegrityCheckResponse]

# --- Rebalance Schemas ---
class RebalanceStatusResponse(BaseModel):
    is_balanced: bool
    skew_percentage: float
    overloaded_nodes: List[str]
    underutilized_nodes: List[str]
    node_utilizations: Dict[str, float]
    recommended_moves: List[Dict[str, Any]]
    last_rebalance_time: Optional[datetime] = None

# --- System Stats Schemas ---
class SystemStatsResponse(BaseModel):
    total_objects: int
    total_replicas: int
    total_storage_bytes: int
    used_storage_bytes: int
    free_storage_bytes: int
    total_nodes: int
    healthy_nodes: int
    failed_nodes: int
    degraded_nodes: int
    active_repairs: int
    corrupted_replicas: int
    replication_factor: int
    uptime_seconds: float
    system_status: str

class SystemEventResponse(BaseModel):
    id: str
    timestamp: datetime
    level: str
    component: str
    message: str
    details: Optional[str] = None
    model_config = ConfigDict(from_attributes=True)
