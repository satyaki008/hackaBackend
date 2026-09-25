# AegisStore Architecture & Distributed Design

## 1. System Overview

**AegisStore** is a fault-tolerant, self-healing distributed object storage system inspired by production storage architectures such as the Google File System (GFS), Ceph, and Apache Ozone. It enables reliable storage, redundant replication, automatic failure detection, and autonomous self-healing across independent storage nodes running on commodity hardware.

```
                            [ Client / Web Browser ]
                                       │
                                       ▼ (REST / HTTP)
                     ┌───────────────────────────────────┐
                     │   AegisStore Controller Gateway   │
                     │  (FastAPI • Port 8000 • Metadata) │
                     └─────────────────┬─────────────────┘
                                       │
            ┌──────────────────────────┼──────────────────────────┐
            ▼                          ▼                          ▼
    ┌──────────────┐           ┌──────────────┐           ┌──────────────┐
    │ Storage Node │           │ Storage Node │           │ Storage Node │
    │ 1 (Alpha)    │           │ 2 (Beta)     │           │ 3 (Gamma)    │
    │ Port 8001    │           │ Port 8002    │           │ Port 8003    │
    │ storage/node1│           │ storage/node2│           │ storage/node3│
    └──────────────┘           └──────────────┘           └──────────────┘
            │                          │                          │
            └──────────────────────────┴──────────────────────────┘
                                       ▲
                                       │ (Autonomous Repair Transfer)
                               ┌──────────────┐
                               │ Storage Node │
                               │ 4 (Delta)    │
                               │ Port 8004    │
                               │ storage/node4│
                               └──────────────┘
```

---

## 2. Core Components

### 2.1 API Gateway & Controller (`backend/app/main.py`)
* Acts as the unified ingress point for external clients.
* Provides REST endpoints for object ingestion, retrieval with transparent failover, deletion, and system administration.
* Coordinates between the Metadata Manager, Replication Manager, and Failure Detector.

### 2.2 Metadata Manager (`backend/app/services/metadata.py`)
* Maintains global system state, object schemas, replica locations, and cluster event logs.
* Backed by SQLite in Write-Ahead Logging (WAL) mode with transactional consistency.
* Enforces per-object asynchronous locks (`asyncio.Lock`) to serialize concurrent mutations on identical objects without stalling the cluster.

### 2.3 Storage Node Microservices (`backend/app/node_server.py`)
* Each node runs as an independent daemon on a dedicated port (`8001`, `8002`, `8003`, `8004`).
* Manages an isolated disk directory (`storage/node1/`, etc.).
* Exposes endpoints for streaming byte ingestion, chunk downloads, cryptographic SHA-256 calculation, and simulated failure injection (crash, latency, network partition, byte corruption).

### 2.4 Replication Manager (`backend/app/services/replication.py`)
* Implements configurable $N$-way replication (default: $3$).
* Selects candidate nodes based on health, network reachability, and lowest storage utilization.
* Issues parallel write requests using `asyncio.gather`.
* Verifies cryptographic SHA-256 checksums returned by storage nodes post-write.
* Implements write-quorum enforcement with atomic rollback on quorum failure.

### 2.5 Failure Detector & Health Monitor (`backend/app/services/health_monitor.py`)
* Runs as a non-blocking background worker polling each node every `HEARTBEAT_INTERVAL_SECONDS` (3s).
* Measures round-trip ping time and records node capacity metrics.
* Detects node timeouts or connection refusals, immediately flagging the node as `OFFLINE`.
* Automatically signals the Replica Repair Manager upon detecting node loss.

### 2.6 Automatic Replica Repair Manager (`backend/app/services/repair.py`)
* The autonomous self-healing engine of the cluster.
* When a node fails:
  1. Identifies all affected object replicas.
  2. Verifies remaining healthy replicas on survivor nodes.
  3. Selects an unallocated healthy node with sufficient disk space.
  4. Streams object bytes directly from a healthy survivor to the replacement node.
  5. Computes and validates the target SHA-256 checksum against authoritative metadata.
  6. Atomically registers the new replica in metadata, restoring the target replication factor.

### 2.7 Data Integrity Auditor (`backend/app/services/integrity.py`)
* Runs periodic deep background audits and on-demand scans.
* Requests each storage node to recalculate the SHA-256 of stored files on disk.
* Compares actual disk checksums with authoritative metadata checksums.
* Detects bit rot or intentional file tampering, marking the replica as `CORRUPTED`.
* Immediately triggers self-healing by fetching a pristine copy from a peer replica and overwriting the corrupted file.

### 2.8 Storage Rebalancer (`backend/app/services/rebalancer.py`)
* Computes capacity utilization skew across cluster nodes.
* If skew exceeds the threshold (20%), proposes non-destructive migrations from overloaded nodes to underutilized nodes.
* Enforces the **Safe Rule**: Source replicas are *never* deleted until the target replica is fully written, checksum-verified, and recorded in metadata.

---

## 3. Consistency Model

AegisStore implements a **Single-Primary Metadata Architecture with Quorum Write Consistency**:
* **Metadata Operations**: Strong consistency via ACID transactions in SQLite with WAL mode.
* **Write Path**: Strong consistency ($W = \min(2, N)$ quorum). Writes must be acknowledged by at least a quorum of nodes with matching cryptographic checksums before client response.
* **Read Path**: High availability ($R = 1$). Reads are served from any healthy replica. If the chosen node times out or errors, the gateway transparently fails over to alternative replicas.
* **Network Partitions**: The system favors **Consistency** over partial writes (CP model under the CAP theorem). If a partitioned node cannot reach the controller, writes are routed only to the connected majority.
