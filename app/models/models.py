import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, Integer, BigInteger, Float, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from app.database import Base

def generate_uuid() -> str:
    return str(uuid.uuid4())

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

class NodeModel(Base):
    __tablename__ = "nodes"
    
    id = Column(String(50), primary_key=True)
    name = Column(String(100), nullable=False)
    url = Column(String(255), nullable=False)
    storage_path = Column(String(255), nullable=False)
    status = Column(String(20), nullable=False, default="ONLINE")  # ONLINE, OFFLINE, DEGRADED, RECOVERING
    total_capacity_bytes = Column(BigInteger, default=524288000)   # 500 MB default
    used_capacity_bytes = Column(BigInteger, default=0)
    free_capacity_bytes = Column(BigInteger, default=524288000)
    object_count = Column(Integer, default=0)
    last_heartbeat = Column(DateTime, default=utc_now)
    response_time_ms = Column(Float, default=0.0)
    is_partitioned = Column(Boolean, default=False)
    simulated_latency_ms = Column(Integer, default=0)
    
    replicas = relationship("ReplicaModel", back_populates="node", cascade="all, delete-orphan")

class ObjectModel(Base):
    __tablename__ = "objects"
    
    id = Column(String(64), primary_key=True, default=generate_uuid)
    name = Column(String(255), nullable=False, index=True)
    content_type = Column(String(100), default="application/octet-stream")
    size_bytes = Column(BigInteger, nullable=False, default=0)
    checksum = Column(String(64), nullable=False, index=True)  # SHA-256
    version = Column(Integer, nullable=False, default=1)
    replication_factor = Column(Integer, nullable=False, default=3)
    storage_policy = Column(String(30), nullable=False, default="REPLICA_3X")  # REPLICA_3X or ERASURE_CODING_RS_4_2
    bucket_name = Column(String(100), nullable=False, default="default-bucket")
    merkle_root = Column(String(64), nullable=True)
    status = Column(String(20), nullable=False, default="HEALTHY")  # HEALTHY, DEGRADED, CORRUPTED, REPAIRING, DELETED
    created_at = Column(DateTime, default=utc_now)
    updated_at = Column(DateTime, default=utc_now, onupdate=utc_now)
    
    replicas = relationship("ReplicaModel", back_populates="object", cascade="all, delete-orphan")
    chunks = relationship("ChunkModel", back_populates="object", cascade="all, delete-orphan")
    repair_jobs = relationship("RepairJobModel", back_populates="object", cascade="all, delete-orphan")

class ReplicaModel(Base):
    __tablename__ = "replicas"
    
    id = Column(String(64), primary_key=True, default=generate_uuid)
    object_id = Column(String(64), ForeignKey("objects.id", ondelete="CASCADE"), nullable=False, index=True)
    node_id = Column(String(50), ForeignKey("nodes.id", ondelete="CASCADE"), nullable=False, index=True)
    version = Column(Integer, nullable=False, default=1)
    checksum = Column(String(64), nullable=False)
    status = Column(String(20), nullable=False, default="HEALTHY")  # HEALTHY, CORRUPTED, MISSING, STALE
    created_at = Column(DateTime, default=utc_now)
    last_verified_at = Column(DateTime, default=utc_now)
    
    object = relationship("ObjectModel", back_populates="replicas")
    node = relationship("NodeModel", back_populates="replicas")

class RepairJobModel(Base):
    __tablename__ = "repair_jobs"
    
    id = Column(String(64), primary_key=True, default=generate_uuid)
    object_id = Column(String(64), ForeignKey("objects.id", ondelete="CASCADE"), nullable=False, index=True)
    object_name = Column(String(255), nullable=False)
    source_node_id = Column(String(50), nullable=True)
    target_node_id = Column(String(50), nullable=False)
    reason = Column(String(50), nullable=False)  # NODE_FAILURE, CORRUPTED_REPLICA, UNDER_REPLICATED, REBALANCE
    status = Column(String(20), nullable=False, default="PENDING")  # PENDING, IN_PROGRESS, COMPLETED, FAILED
    progress_percent = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    bytes_transferred = Column(BigInteger, default=0)
    started_at = Column(DateTime, default=utc_now)
    completed_at = Column(DateTime, nullable=True)
    
    object = relationship("ObjectModel", back_populates="repair_jobs")

class IntegrityCheckModel(Base):
    __tablename__ = "integrity_checks"
    
    id = Column(String(64), primary_key=True, default=generate_uuid)
    object_id = Column(String(64), nullable=False, index=True)
    object_name = Column(String(255), nullable=False)
    node_id = Column(String(50), nullable=False, index=True)
    expected_checksum = Column(String(64), nullable=False)
    actual_checksum = Column(String(64), nullable=False)
    is_valid = Column(Boolean, nullable=False, default=True)
    status = Column(String(20), nullable=False, default="HEALTHY")  # HEALTHY, CORRUPTED, MISSING
    action_taken = Column(String(255), nullable=True)
    checked_at = Column(DateTime, default=utc_now)

class SystemEventModel(Base):
    __tablename__ = "system_events"
    
    id = Column(String(64), primary_key=True, default=generate_uuid)
    timestamp = Column(DateTime, default=utc_now, index=True)
    level = Column(String(20), nullable=False, default="INFO")  # INFO, WARNING, ERROR, SUCCESS
    component = Column(String(50), nullable=False, index=True)
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)

class NetworkPartitionModel(Base):
    __tablename__ = "network_partitions"
    
    id = Column(String(100), primary_key=True)
    node_a = Column(String(50), nullable=False)
    node_b = Column(String(50), nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=utc_now)

class ChunkModel(Base):
    __tablename__ = "chunks"

    id = Column(String(100), primary_key=True, default=generate_uuid)
    object_id = Column(String(64), ForeignKey("objects.id", ondelete="CASCADE"), nullable=False, index=True)
    chunk_index = Column(Integer, nullable=False)  # 0..3 (Data), 4..5 (Parity)
    chunk_type = Column(String(20), nullable=False)  # DATA or PARITY
    node_id = Column(String(50), nullable=False, index=True)
    checksum = Column(String(64), nullable=False)
    size_bytes = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="HEALTHY")  # HEALTHY, CORRUPTED, MISSING
    created_at = Column(DateTime, default=utc_now)
    last_verified_at = Column(DateTime, default=utc_now)

    object = relationship("ObjectModel", back_populates="chunks")

class BucketModel(Base):
    __tablename__ = "buckets"

    name = Column(String(100), primary_key=True)
    storage_policy = Column(String(30), nullable=False, default="REPLICA_3X")  # REPLICA_3X or ERASURE_CODING_RS_4_2
    object_count = Column(Integer, default=0)
    used_bytes = Column(BigInteger, default=0)
    created_at = Column(DateTime, default=utc_now)
