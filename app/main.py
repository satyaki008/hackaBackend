"""Main Application Entry Point.
FastAPI Gateway controller for the Self-Healing Distributed Object Storage System.
"""
import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import List
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.config import settings, STORAGE_BASE_DIR, BASE_DIR
from app.database import engine, Base, SessionLocal, apply_migrations
from app.models.models import NodeModel
from app.services.metadata import metadata_service
from app.services.health_monitor import health_monitor
from app.services.storage_node_client import storage_node_client
from app.node_server import create_node_app
from app.api import api_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("resistore_gateway")

import socket

# Background node server instances when managed internally
_node_servers: List[uvicorn.Server] = []
_node_tasks: List[asyncio.Task] = []

def is_port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a TCP port is already open and in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.2)
        return s.connect_ex((host, port)) == 0

async def start_embedded_node_servers_if_needed():
    """Check if node servers are already running on ports 8001-8004. If not, start them asynchronously."""
    for node_cfg in settings.DEFAULT_NODES:
        port = node_cfg["port"]
        node_id = node_cfg["id"]
        storage_path = node_cfg["storage_path"]
        
        # Test if port is already active via direct TCP probe
        if is_port_in_use(port):
            logger.info(f"Storage Node {node_id} already listening on http://127.0.0.1:{port}")
            continue

        try:
            logger.info(f"Storage node daemon on port {port} ({node_id}) not running. Launching managed daemon...")
            Path(storage_path).mkdir(parents=True, exist_ok=True)
            node_app = create_node_app(node_id, storage_path, settings.DEFAULT_NODE_CAPACITY_BYTES)
            
            config = uvicorn.Config(
                app=node_app,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                access_log=False
            )
            server = uvicorn.Server(config)
            _node_servers.append(server)
            task = asyncio.create_task(server.serve())
            _node_tasks.append(task)
            logger.info(f"Storage Node {node_id} active on http://127.0.0.1:{port}")
        except Exception as e:
            logger.warning(f"Could not start managed node {node_id} on port {port}: {e}")

    # Small pause to allow servers to bind sockets
    await asyncio.sleep(0.5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup:
    logger.info("Initializing AegisStore metadata database...")
    Base.metadata.create_all(bind=engine)
    apply_migrations(engine)
    
    # Ensure storage paths exist
    STORAGE_BASE_DIR.mkdir(parents=True, exist_ok=True)
    for n in settings.DEFAULT_NODES:
        Path(n["storage_path"]).mkdir(parents=True, exist_ok=True)

    # Initialize default nodes in DB
    db = SessionLocal()
    try:
        metadata_service.init_default_nodes(db)
    finally:
        db.close()

    # Launch node microservices if not already active externally
    await start_embedded_node_servers_if_needed()

    # Start background failure detector and health monitor
    await health_monitor.start()
    logger.info("ResiStore Distributed Storage Gateway ready.")
    
    yield

    # Shutdown:
    logger.info("Shutting down ResiStore background tasks...")
    await health_monitor.stop()
    for s in _node_servers:
        s.should_exit = True
    for t in _node_tasks:
        if not t.done():
            t.cancel()
    logger.info("ResiStore Gateway successfully stopped.")

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.VERSION,
    description="Fault-tolerant distributed object storage with automatic replica repair, integrity auditing, and rebalancing.",
    lifespan=lifespan
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(api_router, prefix=settings.API_PREFIX)

# Static dashboard mounting if frontend has been built
FRONTEND_DIST_DIR = BASE_DIR / "frontend" / "dist"
if FRONTEND_DIST_DIR.exists():
    app.mount("/dashboard", StaticFiles(directory=str(FRONTEND_DIST_DIR), html=True), name="frontend_dist")
    assets_dir = FRONTEND_DIST_DIR / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="frontend_assets")

from fastapi.responses import RedirectResponse
from fastapi import Request

@app.get("/")
def get_root(request: Request):
    accept = request.headers.get("accept", "")
    if "text/html" in accept:
        return RedirectResponse(url="/dashboard/")
    return {
        "system": settings.APP_NAME,
        "version": settings.VERSION,
        "status": "OPERATIONAL",
        "web_dashboard": "/dashboard/",
        "api_docs": "/docs",
        "architecture": {
            "controller": "API Gateway / Metadata Manager",
            "nodes": [n["id"] for n in settings.DEFAULT_NODES],
            "default_replication_factor": settings.DEFAULT_REPLICATION_FACTOR
        }
    }

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
