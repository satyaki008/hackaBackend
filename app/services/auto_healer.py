"""ResiStore Autonomous Erasure Coding Pipeline & Auto-Healing Worker.
Coordinates Reed-Solomon RS (4+2) chunking, Merkle Tree integrity verification,
Consistent Hash Ring chunk placement, and background reconstruction of missing/corrupted shards.
"""
import asyncio
import hashlib
from typing import List, Tuple, Dict, Any, Optional
from sqlalchemy.orm import Session

from backend.app.models.models import ObjectModel, ChunkModel, NodeModel
from backend.app.services.metadata import metadata_service
from backend.app.services.erasure_coding import erasure_coding_engine, Chunk
from backend.app.services.consistent_hash import consistent_hash_ring
from backend.app.services.merkle_tree import MerkleTree
from backend.app.services.storage_node_client import storage_node_client
from backend.app.services.mongo_storage_adapter import mongo_storage, mongo_cluster_manager
from backend.app.config import settings

class AutoHealer:
    def __init__(self):
        # Register default 4 nodes into consistent hash ring
        for node in settings.DEFAULT_NODES:
            consistent_hash_ring.add_node(node["id"], state="ALIVE", weight=1)

    async def _write_chunk_to_node(self, node_id: str, chunk_key: str, data: bytes) -> Tuple[bool, str]:
        """Write chunk to either MongoDB Atlas cloud node or local storage daemon."""
        ad = mongo_cluster_manager.get_adapter(node_id)
        if ad and ad.is_connected:
            ok, checksum = ad.write_chunk(chunk_key, data, {"key": chunk_key})
            return ok, checksum
        else:
            # Nodes with local HTTP Storage Daemons
            node_cfg = next((n for n in settings.DEFAULT_NODES if n["id"] == node_id), None)
            if not node_cfg:
                return False, f"Unknown node {node_id}"
            ok, resp = await storage_node_client.write_replica(node_cfg["url"], chunk_key, data)
            checksum = resp.get("checksum", "") if ok else ""
            return ok, checksum

    async def _read_chunk_from_node(self, node_id: str, chunk_key: str) -> Tuple[bool, Optional[bytes], str]:
        """Read chunk from either MongoDB Atlas cloud node or local storage daemon."""
        ad = mongo_cluster_manager.get_adapter(node_id)
        if ad and ad.is_connected:
            ok, data, sha = ad.read_chunk(chunk_key)
            return ok, data, sha or ""
        else:
            node_cfg = next((n for n in settings.DEFAULT_NODES if n["id"] == node_id), None)
            if not node_cfg:
                return False, None, "Unknown node"
            ok, data, err = await storage_node_client.read_replica(node_cfg["url"], chunk_key)
            sha = hashlib.sha256(data).hexdigest() if ok and data else ""
            return ok, data, sha

    async def store_erasure_object(
        self,
        db: Session,
        object_id: str,
        filename: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        bucket_name: str = "default-bucket"
    ) -> Tuple[bool, ObjectModel, List[Dict[str, Any]], str]:
        """Encode raw object into RS (4+2) shards, build Merkle tree, and place on consistent hash ring."""
        # 1. Reed-Solomon RS 4+2 encoding
        chunks, original_size = erasure_coding_engine.encode(data)
        object_checksum = hashlib.sha256(data).hexdigest()

        # 2. Build Merkle Tree over chunk hashes
        leaf_hashes = [c.checksum for c in chunks]
        merkle = MerkleTree(leaf_hashes)

        # 3. Disperse chunks across nodes using Consistent Hash Ring preference list
        ring_nodes = consistent_hash_ring.get_preference_list(object_id, count=len(chunks), only_alive=False)
        if not ring_nodes:
            ring_nodes = [n["id"] for n in settings.DEFAULT_NODES]

        chunk_records = []
        write_tasks = []

        for i, chunk in enumerate(chunks):
            assigned_node = ring_nodes[i % len(ring_nodes)]
            chunk_key = f"{object_id}_chunk_{chunk.index}"
            write_tasks.append(self._write_chunk_to_node(assigned_node, chunk_key, chunk.data))

        results = await asyncio.gather(*write_tasks)

        successful_chunks = 0
        for i, (ok, chk) in enumerate(results):
            assigned_node = ring_nodes[i % len(ring_nodes)]
            chunk = chunks[i]
            chunk_key = f"{object_id}_chunk_{chunk.index}"

            c_model = ChunkModel(
                id=chunk_key,
                object_id=object_id,
                chunk_index=chunk.index,
                chunk_type=chunk.chunk_type,
                node_id=assigned_node,
                checksum=chunk.checksum,
                size_bytes=chunk.size,
                status="HEALTHY" if ok else "MISSING"
            )
            db.add(c_model)
            if ok:
                successful_chunks += 1
            chunk_records.append({
                "chunk_index": chunk.index,
                "type": chunk.chunk_type,
                "node_id": assigned_node,
                "size_bytes": chunk.size,
                "checksum": chunk.checksum,
                "status": "HEALTHY" if ok else "FAILED"
            })

        # Need at least K=4 shards written for durability quorum
        if successful_chunks < erasure_coding_engine.k:
            db.rollback()
            return False, None, [], f"Erasure coding write failed quorum: only {successful_chunks}/{erasure_coding_engine.k} shards stored."

        # Create Object metadata
        obj = ObjectModel(
            id=object_id,
            name=filename,
            content_type=content_type,
            size_bytes=original_size,
            checksum=object_checksum,
            version=1,
            replication_factor=6,  # 4 data + 2 parity
            storage_policy="ERASURE_CODING_RS_4_2",
            bucket_name=bucket_name,
            merkle_root=merkle.root_hash,
            status="HEALTHY" if successful_chunks == len(chunks) else "DEGRADED"
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)

        metadata_service.log_event(
            db,
            level="SUCCESS",
            component="ERASURE_CODING",
            message=f"Object '{filename}' encoded with RS(4+2). {successful_chunks}/6 shards stored. Merkle Root: {merkle.root_hash[:12]}..."
        )

        return True, obj, chunk_records, "Object erasure-coded and dispersed successfully."

    async def read_erasure_object(self, db: Session, object_id: str) -> Tuple[bool, Optional[bytes], str]:
        """Read and decode object from any 4 surviving chunks, even if nodes are down or disconnected."""
        obj = metadata_service.get_object(db, object_id)
        if not obj:
            return False, None, "Object not found"

        chunks = db.query(ChunkModel).filter(ChunkModel.object_id == object_id).all()
        if not chunks:
            return False, None, "No chunk metadata found for object"

        # Read available chunks concurrently
        read_tasks = [
            self._read_chunk_from_node(c.node_id, c.id)
            for c in chunks
        ]
        results = await asyncio.gather(*read_tasks)

        available_chunks: List[Chunk] = []
        missing_indices: List[int] = []

        for i, (ok, data, sha) in enumerate(results):
            c_meta = chunks[i]
            if ok and data:
                # Verify chunk integrity
                if hashlib.sha256(data).hexdigest() == c_meta.checksum:
                    available_chunks.append(Chunk(c_meta.chunk_index, c_meta.chunk_type, data, c_meta.checksum))
                else:
                    c_meta.status = "CORRUPTED"
                    missing_indices.append(c_meta.chunk_index)
            else:
                c_meta.status = "MISSING"
                missing_indices.append(c_meta.chunk_index)

        db.commit()

        if len(available_chunks) < erasure_coding_engine.k:
            return False, None, f"Catastrophic failure: Only {len(available_chunks)}/4 shards survive. Data unrecoverable."

        # Reconstruct payload using Reed-Solomon math
        try:
            payload = erasure_coding_engine.decode(available_chunks, obj.size_bytes)
            
            # If any chunks were missing or corrupted, trigger autonomous background healing!
            if missing_indices:
                asyncio.create_task(self.heal_erasure_object(object_id))

            return True, payload, "Reconstructed successfully via Reed-Solomon decoder."
        except Exception as e:
            return False, None, f"Reed-Solomon decoding error: {e}"

    async def heal_erasure_object(self, object_id: str) -> Tuple[bool, str]:
        """Autonomous Repair Worker: Reads surviving chunks, uses RS math to reconstruct lost chunks,
        and re-places them on healthy nodes.
        """
        from backend.app.database import SessionLocal
        db: Session = SessionLocal()
        try:
            obj = metadata_service.get_object(db, object_id)
            if not obj:
                return False, "Object not found"

            chunks = db.query(ChunkModel).filter(ChunkModel.object_id == object_id).all()
            
            # Collect available chunks
            read_tasks = [self._read_chunk_from_node(c.node_id, c.id) for c in chunks]
            results = await asyncio.gather(*read_tasks)

            available: List[Chunk] = []
            damaged_chunks: List[ChunkModel] = []

            for i, (ok, data, sha) in enumerate(results):
                c_meta = chunks[i]
                if ok and data and hashlib.sha256(data).hexdigest() == c_meta.checksum:
                    available.append(Chunk(c_meta.chunk_index, c_meta.chunk_type, data, c_meta.checksum))
                else:
                    damaged_chunks.append(c_meta)

            if not damaged_chunks:
                return True, "All chunks already healthy."

            if len(available) < erasure_coding_engine.k:
                return False, f"Insufficient surviving shards to reconstruct damaged chunks ({len(available)}/4)"

            # Reconstruct each damaged shard
            repaired_count = 0
            for damaged in damaged_chunks:
                # 1. Reed-Solomon reconstruction
                repaired = erasure_coding_engine.reconstruct_lost_chunk(available, damaged.chunk_index)

                # 2. Find healthy replacement node
                replacement_nodes = consistent_hash_ring.get_preference_list(f"{object_id}-{damaged.chunk_index}", count=3, only_alive=True)
                target_node = replacement_nodes[0] if replacement_nodes else "node-1"

                # 3. Write reconstructed chunk
                ok_write, _ = await self._write_chunk_to_node(target_node, damaged.id, repaired.data)
                if ok_write:
                    damaged.node_id = target_node
                    damaged.status = "HEALTHY"
                    damaged.checksum = repaired.checksum
                    repaired_count += 1

            db.commit()
            metadata_service.log_event(
                db,
                level="SUCCESS",
                component="AUTO_HEALER",
                message=f"Autonomous Auto-Healer reconstructed {repaired_count} damaged shard(s) for object '{obj.name}'."
            )
            return True, f"Successfully healed {repaired_count} shard(s)."
        finally:
            db.close()

auto_healer = AutoHealer()
