"""HTTP Client for communicating with distributed storage nodes."""
import time
from typing import Dict, Any, Optional, Tuple
import httpx
from backend.app.config import settings

class StorageNodeClient:
    def __init__(self, timeout: float = 3.0):
        self.timeout = timeout

    async def get_health(self, node_url: str) -> Tuple[bool, Dict[str, Any], float]:
        """Ping a storage node's /health endpoint and return (is_healthy, payload, latency_ms)."""
        start_time = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(f"{node_url}/health")
                latency_ms = (time.perf_counter() - start_time) * 1000.0
                if resp.status_code == 200:
                    return True, resp.json(), latency_ms
                else:
                    return False, {"error": resp.text, "status_code": resp.status_code}, latency_ms
        except Exception as e:
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            return False, {"error": str(e)}, latency_ms

    async def write_replica(self, node_url: str, object_id: str, data: bytes) -> Tuple[bool, Dict[str, Any]]:
        """Write an object replica to a storage node via HTTP POST."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout + 5.0) as client:
                resp = await client.post(
                    f"{node_url}/objects/{object_id}",
                    content=data,
                    headers={"Content-Type": "application/octet-stream"}
                )
                if resp.status_code in (200, 201):
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def read_replica(self, node_url: str, object_id: str) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """Read an object replica from a storage node via HTTP GET."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout + 5.0) as client:
                resp = await client.get(f"{node_url}/objects/{object_id}")
                if resp.status_code == 200:
                    return True, resp.content, None
                return False, None, f"Node returned status {resp.status_code}: {resp.text}"
        except Exception as e:
            return False, None, str(e)

    async def delete_replica(self, node_url: str, object_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Delete an object replica from a storage node via HTTP DELETE."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.delete(f"{node_url}/objects/{object_id}")
                if resp.status_code in (200, 204, 404):
                    return True, resp.json() if resp.text else {}
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def check_checksum(self, node_url: str, object_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Request the storage node to calculate and return the SHA-256 of a stored replica."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(f"{node_url}/objects/{object_id}/checksum")
                if resp.status_code == 200:
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def corrupt_replica(self, node_url: str, object_id: str) -> Tuple[bool, Dict[str, Any]]:
        """Trigger simulated byte corruption on a node's stored replica."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(f"{node_url}/objects/{object_id}/corrupt")
                if resp.status_code == 200:
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def set_node_status(self, node_url: str, status: str) -> Tuple[bool, Dict[str, Any]]:
        """Admin: Set node status (ONLINE, OFFLINE, DEGRADED)."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(f"{node_url}/admin/status", json={"status": status})
                if resp.status_code == 200:
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def set_node_latency(self, node_url: str, latency_ms: int) -> Tuple[bool, Dict[str, Any]]:
        """Admin: Inject artificial network/disk latency."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(f"{node_url}/admin/latency", json={"latency_ms": latency_ms})
                if resp.status_code == 200:
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

    async def set_node_partition(self, node_url: str, is_partitioned: bool) -> Tuple[bool, Dict[str, Any]]:
        """Admin: Toggle network partition state on the node."""
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(f"{node_url}/admin/partition", json={"is_partitioned": is_partitioned})
                if resp.status_code == 200:
                    return True, resp.json()
                return False, {"error": resp.text, "status_code": resp.status_code}
        except Exception as e:
            return False, {"error": str(e)}

storage_node_client = StorageNodeClient(timeout=settings.NODE_TIMEOUT_SECONDS)
