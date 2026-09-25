import os
from pathlib import Path
from typing import List, Dict, Any
from pydantic_settings import BaseSettings, SettingsConfigDict

BACKEND_DIR = Path(__file__).resolve().parent.parent
BASE_DIR = BACKEND_DIR.parent

# Resolve storage directory: check BACKEND_DIR/storage then BASE_DIR/storage
if (BACKEND_DIR / "storage").exists():
    STORAGE_BASE_DIR = BACKEND_DIR / "storage"
elif (BASE_DIR / "storage").exists():
    STORAGE_BASE_DIR = BASE_DIR / "storage"
else:
    STORAGE_BASE_DIR = BACKEND_DIR / "storage"

STORAGE_BASE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_DB_PATH = BACKEND_DIR / "metadata.db"

class Settings(BaseSettings):
    APP_NAME: str = "ResiStore - Self-Healing Distributed Object Storage"
    VERSION: str = "2.0.0"
    API_PREFIX: str = "/api"
    
    # Database
    DATABASE_URL: str = os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")
    
    # Replication & Topology
    DEFAULT_REPLICATION_FACTOR: int = 3
    MIN_QUORUM_WRITE: int = 2
    
    # Health Monitoring & Failure Detection
    HEARTBEAT_INTERVAL_SECONDS: float = 3.0
    NODE_TIMEOUT_SECONDS: float = 2.0
    
    # Background Integrity & Repair
    INTEGRITY_CHECK_INTERVAL_SECONDS: float = 30.0
    REPAIR_INTERVAL_SECONDS: float = 5.0
    REBALANCE_INTERVAL_SECONDS: float = 45.0
    REBALANCE_SKEW_THRESHOLD: float = 0.20  # 20% capacity difference triggers rebalance recommendation
    
    # Simulated per-node storage capacity (e.g. 500 MB for demo purposes)
    DEFAULT_NODE_CAPACITY_BYTES: int = 500 * 1024 * 1024
    
    # Storage Nodes configuration
    DEFAULT_NODES: List[Dict[str, Any]] = [
        {
            "id": "node-1",
            "name": "Node 1 [MongoDB Atlas Cloud - Active]",
            "url": "http://127.0.0.1:8001",
            "storage_path": str(STORAGE_BASE_DIR / "node1"),
            "port": 8001,
            "cloud_provider": "MongoDB Atlas Cloud",
            "cloud_cluster": "cluster0.i7avjwe.mongodb.net",
            "is_cloud_active": True
        },
        {
            "id": "node-2",
            "name": "Node 2 [Standby Local - Add Cloud Later]",
            "url": "http://127.0.0.1:8002",
            "storage_path": str(STORAGE_BASE_DIR / "node2"),
            "port": 8002,
            "cloud_provider": "Local Emulated",
            "cloud_cluster": None,
            "is_cloud_active": False
        },
        {
            "id": "node-3",
            "name": "Node 3 [Standby Local - Add Cloud Later]",
            "url": "http://127.0.0.1:8003",
            "storage_path": str(STORAGE_BASE_DIR / "node3"),
            "port": 8003,
            "cloud_provider": "Local Emulated",
            "cloud_cluster": None,
            "is_cloud_active": False
        },
        {
            "id": "node-4",
            "name": "Node 4 [Standby Local - Add Cloud Later]",
            "url": "http://127.0.0.1:8004",
            "storage_path": str(STORAGE_BASE_DIR / "node4"),
            "port": 8004,
            "cloud_provider": "Local Emulated",
            "cloud_cluster": None,
            "is_cloud_active": False
        },
    ]

    model_config = SettingsConfigDict(env_file=".env", extra="allow")

settings = Settings()
