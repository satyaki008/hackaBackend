"""Standalone Storage Node Server (FastAPI).
Represents an independent distributed storage daemon storing object replicas on disk.
"""
import argparse
import asyncio
import os
from pathlib import Path
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, Response, UploadFile, File, Query, status
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn

from app.utils.hashing import calculate_file_sha256, calculate_sha256

class NodeState:
    def __init__(self, node_id: str, storage_dir: Path, capacity_bytes: int = 524288000):
        self.node_id = node_id
        self.storage_dir = Path(storage_dir).resolve()
        self.objects_dir = self.storage_dir / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self.status = "ONLINE"  # ONLINE, OFFLINE, DEGRADED, RECOVERING
        self.simulated_latency_ms = 0
        self.is_partitioned = False
        self.capacity_bytes = capacity_bytes

    def get_used_bytes(self) -> int:
        total = 0
        if self.objects_dir.exists():
            for f in self.objects_dir.iterdir():
                if f.is_file():
                    total += f.stat().st_size
        return total

    def get_object_count(self) -> int:
        if not self.objects_dir.exists():
            return 0
        return sum(1 for f in self.objects_dir.iterdir() if f.is_file())

def create_node_app(node_id: str, storage_dir: str, capacity_bytes: int = 524288000) -> FastAPI:
    state = NodeState(node_id, Path(storage_dir), capacity_bytes)
    app = FastAPI(title=f"AegisStore Storage Node - {node_id}")

    async def check_node_availability():
        if state.is_partitioned:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail=f"Network partition active: node {state.node_id} is unreachable."
            )
        if state.status == "OFFLINE":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Storage node {state.node_id} is OFFLINE."
            )
        if state.simulated_latency_ms > 0:
            await asyncio.sleep(state.simulated_latency_ms / 1000.0)

    @app.get("/health")
    async def get_health():
        await check_node_availability()
        used = state.get_used_bytes()
        free = max(0, state.capacity_bytes - used)
        return {
            "node_id": state.node_id,
            "status": state.status,
            "total_bytes": state.capacity_bytes,
            "used_bytes": used,
            "free_bytes": free,
            "object_count": state.get_object_count(),
            "simulated_latency_ms": state.simulated_latency_ms,
            "is_partitioned": state.is_partitioned
        }

    def get_safe_object_path(obj_id: str) -> Path:
        clean_id = os.path.basename(obj_id).strip()
        if not clean_id or clean_id in ('.', '..') or '/' in clean_id or '\\' in clean_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Security Error: Invalid object ID or path traversal detected."
            )
        resolved = (state.objects_dir / clean_id).resolve()
        if not str(resolved).startswith(str(state.objects_dir.resolve())):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Security Error: Path traversal attempt blocked."
            )
        return resolved

    @app.post("/objects/{object_id}")
    async def store_object(object_id: str, request: Request, file: Optional[UploadFile] = File(None)):
        await check_node_availability()
        file_path = get_safe_object_path(object_id)
        
        # Read payload
        if file is not None:
            content = await file.read()
        else:
            content = await request.body()
            
        file_size = len(content)
        used = state.get_used_bytes()
        if (used + file_size) > state.capacity_bytes:
            raise HTTPException(
                status_code=status.HTTP_507_INSUFFICIENT_STORAGE,
                detail=f"Node {state.node_id} capacity exceeded."
            )
            
        # Write to temporary file first, then atomic rename
        temp_path = state.objects_dir / f"{file_path.name}.tmp"
        with open(temp_path, "wb") as f:
            f.write(content)
            
        temp_path.replace(file_path)
        
        checksum = calculate_sha256(content)
        return {
            "node_id": state.node_id,
            "object_id": object_id,
            "checksum": checksum,
            "size_bytes": file_size,
            "status": "STORED"
        }

    @app.get("/objects/{object_id}")
    async def get_object(object_id: str):
        await check_node_availability()
        file_path = get_safe_object_path(object_id)
        if not file_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Object {object_id} not found on node {state.node_id}"
            )
        return FileResponse(path=file_path, filename=file_path.name)

    @app.delete("/objects/{object_id}")
    async def delete_object(object_id: str):
        await check_node_availability()
        file_path = get_safe_object_path(object_id)
        if file_path.is_file():
            file_path.unlink()
            return {"node_id": state.node_id, "object_id": object_id, "status": "DELETED"}
        return {"node_id": state.node_id, "object_id": object_id, "status": "NOT_FOUND"}

    @app.get("/objects/{object_id}/checksum")
    async def get_checksum(object_id: str):
        await check_node_availability()
        file_path = get_safe_object_path(object_id)
        if not file_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Object {object_id} not found on node {state.node_id}"
            )
        checksum, size = calculate_file_sha256(file_path)
        return {
            "node_id": state.node_id,
            "object_id": object_id,
            "checksum": checksum,
            "size_bytes": size,
            "exists": True
        }

    @app.post("/objects/{object_id}/corrupt")
    async def corrupt_object(object_id: str):
        """Testing endpoint: corrupts bytes of the stored replica on disk."""
        await check_node_availability()
        file_path = state.objects_dir / object_id
        if not file_path.is_file():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Object {object_id} not found on node {state.node_id}"
            )
            
        with open(file_path, "r+b") as f:
            data = f.read()
            if len(data) == 0:
                f.write(b"CORRUPT_INJECTED_DATA")
            else:
                # Corrupt first few bytes
                f.seek(0)
                corrupted_header = b"BAD_DATA!"[:min(9, len(data))]
                f.write(corrupted_header)
                
        new_checksum, new_size = calculate_file_sha256(file_path)
        return {
            "node_id": state.node_id,
            "object_id": object_id,
            "status": "CORRUPTED",
            "new_checksum": new_checksum,
            "message": "Injected byte corruption into disk replica for testing"
        }

    @app.get("/objects")
    async def list_objects():
        await check_node_availability()
        items = []
        if state.objects_dir.exists():
            for f in state.objects_dir.iterdir():
                if f.is_file() and not f.name.endswith(".tmp"):
                    items.append({
                        "object_id": f.name,
                        "size_bytes": f.stat().st_size
                    })
        return {"node_id": state.node_id, "objects": items, "count": len(items)}

    # Admin / Simulation endpoints
    class StatusPayload(BaseModel):
        status: str

    class LatencyPayload(BaseModel):
        latency_ms: int

    class PartitionPayload(BaseModel):
        is_partitioned: bool

    @app.post("/admin/status")
    async def set_status(payload: StatusPayload):
        state.status = payload.status.upper()
        return {"node_id": state.node_id, "status": state.status}

    @app.post("/admin/latency")
    async def set_latency(payload: LatencyPayload):
        state.simulated_latency_ms = max(0, payload.latency_ms)
        return {"node_id": state.node_id, "simulated_latency_ms": state.simulated_latency_ms}

    @app.post("/admin/partition")
    async def set_partition(payload: PartitionPayload):
        state.is_partitioned = payload.is_partitioned
        return {"node_id": state.node_id, "is_partitioned": state.is_partitioned}

    return app

def main():
    parser = argparse.ArgumentParser(description="AegisStore Storage Node Server")
    parser.add_argument("--node-id", type=str, required=True, help="Storage Node ID (e.g. node-1)")
    parser.add_argument("--port", type=int, required=True, help="Port to bind (e.g. 8001)")
    parser.add_argument("--storage-dir", type=str, required=True, help="Directory to store files")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host interface to bind")
    parser.add_argument("--capacity", type=int, default=524288000, help="Capacity in bytes")
    args = parser.parse_args()

    app = create_node_app(args.node_id, args.storage_dir, args.capacity)
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")

if __name__ == "__main__":
    main()
