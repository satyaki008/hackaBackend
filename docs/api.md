# AegisStore REST API Reference

All requests accept and return JSON unless otherwise noted.
Base URL: `http://localhost:8000/api`

---

## 1. Object Storage APIs

### `POST /api/objects`
Upload and replicate a new object across healthy nodes.
* **Content-Type**: `multipart/form-data`
* **Form Parameters**:
  * `file`: Binary file upload (required)
  * `replication_factor`: Integer (optional, default: 3, range: 1-4)
* **Response (201 Created)**:
```json
{
  "object_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "name": "sample_document.pdf",
  "size_bytes": 1048576,
  "checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "version": 1,
  "replication_factor": 3,
  "status": "HEALTHY",
  "nodes_written": ["node-1", "node-2", "node-3"],
  "message": "Object stored and replicated successfully."
}
```

### `GET /api/objects`
List all stored objects with replica details.
* **Query Parameters**:
  * `search`: String (optional, case-insensitive filter by filename)
* **Response (200 OK)**:
```json
[
  {
    "id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "name": "sample_document.pdf",
    "content_type": "application/pdf",
    "size_bytes": 1048576,
    "checksum": "e3b0c442...",
    "version": 1,
    "replication_factor": 3,
    "status": "HEALTHY",
    "created_at": "2026-09-26T00:30:00Z",
    "updated_at": "2026-09-26T00:30:00Z",
    "healthy_replica_count": 3,
    "replicas": [
      {
        "id": "rep-uuid-1",
        "object_id": "9b1deb4d...",
        "node_id": "node-1",
        "version": 1,
        "checksum": "e3b0c442...",
        "status": "HEALTHY",
        "created_at": "2026-09-26T00:30:00Z"
      }
    ]
  }
]
```

### `GET /api/objects/{object_id}/download`
Download an object with transparent failover.
* **Response (200 OK)**: File stream
* **Response Headers**:
  * `Content-Disposition`: `attachment; filename="..."`
  * `X-Replica-Source-Node`: Node ID serving the download
  * `X-Checksum-SHA256`: SHA-256 checksum

### `DELETE /api/objects/{object_id}`
Delete all replicas of an object across nodes and remove metadata.
* **Response (200 OK)**:
```json
{
  "status": "DELETED",
  "object_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "message": "Object and all replicas deleted successfully."
}
```

### `POST /api/objects/{object_id}/corrupt-replica`
Test endpoint: Intentionally flips/corrupts bytes of a replica on disk to test self-healing.
* **Query Parameters**:
  * `node_id`: String (optional, targets specific node)

---

## 2. Storage Node Management APIs

### `GET /api/nodes`
List all registered storage nodes with live status, latency, and disk metrics.
* **Response (200 OK)**:
```json
[
  {
    "id": "node-1",
    "name": "Storage Node 1 (Alpha)",
    "url": "http://127.0.0.1:8001",
    "storage_path": "storage/node1",
    "status": "ONLINE",
    "total_capacity_bytes": 524288000,
    "used_capacity_bytes": 1048576,
    "free_capacity_bytes": 523239424,
    "object_count": 1,
    "response_time_ms": 2.1,
    "is_partitioned": false,
    "simulated_latency_ms": 0
  }
]
```

### `POST /api/nodes/{node_id}/fail`
Simulate node crash. Flags node as `OFFLINE` and triggers automatic replica repair.

### `POST /api/nodes/{node_id}/recover`
Simulate node recovery. Returns node to `ONLINE` status.

### `POST /api/nodes/{node_id}/slow`
Inject simulated network/disk latency into a node.
* **Body**: `{"latency_ms": 500}`

### `POST /api/nodes/partition`
Simulate network partition between controller and a node.
* **Body**: `{"node_a": "node-2", "node_b": "controller", "enable_partition": true}`

---

## 3. Replication & Self-Healing APIs

### `GET /api/replication/status`
Returns replication health, under-replicated objects, and active repair status.

### `POST /api/replication/repair`
Trigger self-healing repair for under-replicated objects.
* **Body (optional)**: `{"object_id": "..."}` (or empty for all under-replicated objects)

### `GET /api/replication/jobs`
List background replica repair jobs with progress percentages.

---

## 4. Cryptographic Integrity APIs

### `POST /api/integrity/check`
Trigger deep SHA-256 audit across all nodes. Automatically replaces corrupted replicas from healthy peers.
* **Query Parameters**: `auto_repair=true` (default: true)

### `GET /api/integrity/status`
Returns summary statistics of the most recent integrity audit.

---

## 5. Storage Rebalancing APIs

### `GET /api/rebalance/status`
Calculates capacity skew percentage across cluster nodes.

### `POST /api/rebalance`
Executes safe non-destructive replica migrations from overloaded to underutilized nodes.
* **Query Parameters**: `max_moves=5`

---

## 6. System & Interactive Demo APIs

### `GET /api/stats`
Aggregated dashboard statistics (total objects, storage used, healthy nodes, repairs).

### `GET /api/events`
Live audit log of all system actions (with `?limit=100&level=INFO`).

### `POST /api/demo/auto-repair`
Executes the full 10-step distributed failure and auto-recovery demonstration.

### `POST /api/demo/corruption-heal`
Executes the byte-tampering detection and peer self-healing demonstration.

### `POST /api/demo/reset`
Resets all cluster nodes to clean ONLINE state.
