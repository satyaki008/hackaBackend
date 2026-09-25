# AegisStore: Engineering Design Decisions & Trade-Offs

## 1. Separation of Control Plane and Data Plane

Following the proven architecture of Google File System (GFS) and Ceph:
* **The Control Plane (Metadata Manager)** manages namespaces, replica mappings, and node states. It does not handle bulk file payload transfers during normal operation.
* **The Data Plane (Storage Nodes)** handles binary chunk storage, checksum recalculation, and streaming IO.
* **Benefit**: The metadata service remains lightweight and responsive, preventing metadata operations from being bottlenecked by large client payload transfers.

---

## 2. Concurrency Model & Locking Strategy

In distributed object stores, concurrent reads, writes, and repairs can cause race conditions:
* **Per-Object Granular Locking (`asyncio.Lock`)**:
  Instead of locking the entire database, AegisStore maintains an asynchronous lock per `object_id`.
  - Multiple clients can upload or read different objects in parallel without any blocking.
  - Concurrent mutations to the *same* object (e.g. concurrent upload, repair, or deletion) are strictly serialized.
* **Transactional SQLite with Write-Ahead Logging (WAL)**:
  SQLite in WAL mode allows concurrent readers to operate without waiting for writers (`PRAGMA journal_mode=WAL`). Foreign key constraints and atomic commits prevent orphaned replicas.

---

## 3. Failure Detection: Heartbeats vs Leases

* **Heartbeat Mechanism**: The background health monitor polls each storage node every $3$ seconds (`HEARTBEAT_INTERVAL_SECONDS`).
* **Timeout Threshold**: A node is declared `OFFLINE` if it fails to respond within $2$ seconds (`NODE_TIMEOUT_SECONDS`).
* **Instant Recovery**: When a node recovers, the health monitor detects its heartbeat, resets its status to `ONLINE`, and triggers a global scan to inspect if any degraded objects can restore their full redundancy.

---

## 4. Automatic Replica Repair Algorithm

When a node failure or corruption occurs:
1. **Quorum Check**: Identify all objects whose healthy replica count $H < \text{replication\_factor}$.
2. **Survival Guarantee**: Verify that at least one healthy replica exists ($H \ge 1$). If $H = 0$, mark the object `CRITICAL` and raise an alert.
3. **Target Node Selection**:
   - Filter candidate nodes that are `ONLINE` and not partitioned.
   - Exclude any node already hosting a replica of this object.
   - Select the node with the lowest used storage capacity and lowest replica count to maintain load balance.
4. **Verified Transfer**:
   - Stream bytes from a healthy survivor node.
   - Calculate SHA-256 in flight.
   - Write to the target replacement node.
   - Verify the target node's returned SHA-256 matches the authoritative checksum.
5. **Metadata Update**: Atomically commit the new replica record in the database.

---

## 5. End-to-End Cryptographic Data Integrity

Bit rot (silent data corruption due to hardware degradation or disk errors) cannot be detected by filesystem timestamps:
* Every object stores its authoritative **SHA-256** checksum calculated upon initial ingestion.
* During periodic or on-demand audits, the storage node streams the replica bytes from disk through a SHA-256 hasher.
* If `Actual Checksum != Authoritative Checksum`:
  - The replica is immediately flagged `CORRUPTED`.
  - The system automatically retrieves a verified replica from a healthy peer node and overwrites the corrupted file on disk.
  - The repaired file is verified again before being restored to `HEALTHY`.

---

## 6. Safe Rebalancing & Migration Rule

* **The Problem**: Naive rebalancers delete the source replica immediately when initiating a move, leading to permanent data loss if the target fails mid-transfer.
* **The Safe Rule**:
  $$\text{Target Written} \longrightarrow \text{Target SHA-256 Verified} \longrightarrow \text{Metadata Committed} \longrightarrow \text{Source Replica Safely Deleted}$$
* At no point in time does the cluster have fewer than the current healthy replica count.
