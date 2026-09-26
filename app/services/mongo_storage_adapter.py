"""MongoDB Atlas Multi-Node Storage Adapter for ResiStore.
Supports connecting Node 1 (and dynamically Nodes 2, 3, 4) to MongoDB Atlas cloud clusters.
User provided cluster:
mongodb+srv://satyakiti008_db_user:3214@cluster0.i7avjwe.mongodb.net/?appName=Cluster0
"""
import os
import time
from typing import Dict, Any, Optional, Tuple, List
import hashlib
import pymongo
from pymongo import MongoClient

DEFAULT_NODE1_URI = os.getenv(
    "MONGODB_URI",
    "mongodb+srv://satyakiti008_db_user:3214@cluster0.i7avjwe.mongodb.net/?appName=Cluster0"
)

class MongoStorageAdapter:
    def __init__(self, node_id: str = "node-1", uri: str = DEFAULT_NODE1_URI, db_name: str = "resistore_storage"):
        self.node_id = node_id
        self.uri = uri
        self.db_name = db_name
        self._client: Optional[MongoClient] = None
        self._db = None
        self._collection = None
        self.is_connected = False
        self._connect()

    def _connect(self):
        """Establish lazy connection with connection pooling and 5s timeout."""
        try:
            self._client = MongoClient(
                self.uri,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=10000
            )
            self._db = self._client[self.db_name]
            coll_name = f"{self.node_id.replace('-', '_')}_chunks"
            self._collection = self._db[coll_name]
            # Create unique index on chunk/object id
            self._collection.create_index("id", unique=True)
            self.is_connected = True
        except Exception:
            self.is_connected = False

    def ping(self) -> Tuple[bool, float]:
        """Check connection health and return (is_healthy, latency_ms)."""
        start = time.perf_counter()
        try:
            if not self._client:
                self._connect()
            self._client.admin.command('ping')
            latency = (time.perf_counter() - start) * 1000.0
            self.is_connected = True
            return True, round(latency, 2)
        except Exception:
            latency = (time.perf_counter() - start) * 1000.0
            self.is_connected = False
            return False, round(latency, 2)

    def write_chunk(self, chunk_id: str, data: bytes, metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """Store chunk or replica bytes directly into MongoDB Atlas."""
        try:
            checksum = hashlib.sha256(data).hexdigest()
            doc = {
                "id": chunk_id,
                "data": data,
                "size_bytes": len(data),
                "checksum": checksum,
                "metadata": metadata or {},
                "updated_at": time.time()
            }
            self._collection.replace_one({"id": chunk_id}, doc, upsert=True)
            return True, checksum
        except Exception as e:
            return False, str(e)

    def read_chunk(self, chunk_id: str) -> Tuple[bool, Optional[bytes], Optional[str]]:
        """Retrieve chunk or replica bytes from MongoDB Atlas."""
        try:
            doc = self._collection.find_one({"id": chunk_id})
            if not doc:
                return False, None, f"Chunk {chunk_id} not found in MongoDB Atlas"
            return True, doc["data"], doc.get("checksum")
        except Exception as e:
            return False, None, str(e)

    def delete_chunk(self, chunk_id: str) -> bool:
        """Delete chunk from MongoDB Atlas."""
        try:
            res = self._collection.delete_one({"id": chunk_id})
            return res.deleted_count > 0
        except Exception:
            return False

    def corrupt_chunk(self, chunk_id: str) -> Tuple[bool, str]:
        """Test endpoint: Corrupts stored bytes in MongoDB Atlas to verify self-healing."""
        try:
            doc = self._collection.find_one({"id": chunk_id})
            if not doc:
                return False, "Chunk not found"
            
            orig = doc["data"]
            tampered = b"BAD_BYTES!" + (orig[10:] if len(orig) > 10 else b"")
            new_checksum = hashlib.sha256(tampered).hexdigest()
            self._collection.update_one(
                {"id": chunk_id},
                {"$set": {"data": tampered, "checksum": new_checksum}}
            )
            return True, new_checksum
        except Exception as e:
            return False, str(e)

    def get_stats(self) -> Dict[str, Any]:
        """Get storage usage statistics from MongoDB Atlas."""
        cluster_host = "cluster0.i7avjwe.mongodb.net"
        if "@" in self.uri:
            try:
                cluster_host = self.uri.split("@")[1].split("/")[0].split("?")[0]
            except Exception:
                pass

        try:
            ok, latency = self.ping()
            count = self._collection.count_documents({}) if ok else 0
            pipeline = [{"$group": {"_id": None, "total": {"$sum": "$size_bytes"}}}]
            res = list(self._collection.aggregate(pipeline)) if ok else []
            total_bytes = res[0]["total"] if res else 0

            return {
                "node_id": self.node_id,
                "backend": "MongoDB Atlas Cloud",
                "cluster": cluster_host,
                "is_connected": ok,
                "latency_ms": latency if ok else 0.0,
                "stored_chunks": count,
                "used_bytes": total_bytes
            }
        except Exception:
            return {
                "node_id": self.node_id,
                "backend": "MongoDB Atlas Cloud",
                "cluster": cluster_host,
                "is_connected": False,
                "latency_ms": 0.0,
                "stored_chunks": 0,
                "used_bytes": 0
            }


class MongoClusterManager:
    """Manages cloud-connected MongoDB storage adapters across nodes."""
    def __init__(self):
        self._adapters: Dict[str, MongoStorageAdapter] = {}
        # Pre-register user's Node 1 cluster
        self.register_node("node-1", DEFAULT_NODE1_URI)

    def register_node(self, node_id: str, uri: str, db_name: str = "resistore_storage") -> Tuple[bool, str, float]:
        adapter = MongoStorageAdapter(node_id=node_id, uri=uri, db_name=db_name)
        ok, latency = adapter.ping()
        self._adapters[node_id] = adapter
        if ok:
            return True, f"Successfully connected {node_id} to MongoDB Atlas!", latency
        return False, f"Could not connect {node_id} to MongoDB Atlas (check URI/firewall).", latency

    def get_adapter(self, node_id: str) -> Optional[MongoStorageAdapter]:
        return self._adapters.get(node_id)

    def has_cloud_node(self, node_id: str) -> bool:
        ad = self._adapters.get(node_id)
        return ad is not None and ad.is_connected

    def get_all_node_stats(self) -> List[Dict[str, Any]]:
        res = []
        for n_id in ["node-1", "node-2", "node-3", "node-4"]:
            ad = self._adapters.get(n_id)
            if ad and ad.is_connected:
                res.append(ad.get_stats())
            else:
                res.append({
                    "node_id": n_id,
                    "backend": "Local / Standby (Awaiting Cloud URI)",
                    "cluster": None,
                    "is_connected": False,
                    "latency_ms": 0.0,
                    "stored_chunks": 0,
                    "used_bytes": 0
                })
        return res

mongo_cluster_manager = MongoClusterManager()
mongo_storage = mongo_cluster_manager.get_adapter("node-1")
