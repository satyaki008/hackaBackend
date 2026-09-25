# AegisStore College Viva & Project Defense Guide

This document contains 25 questions and answers designed to help you explain the project to professors, external examiners, and evaluators during a project viva or technical defense.

---

### Q1: What is the core goal of this project?
**Answer:** The goal of AegisStore is to build a production-style, fault-tolerant distributed object storage system that continues serving files when storage nodes fail, detects corrupted replicas (bit rot), automatically repairs missing replicas to spare nodes in the background, and dynamically rebalances storage loads.

---

### Q2: How does Object Storage differ from Block Storage and Traditional File Systems?
**Answer:**
* **File Storage (e.g., NTFS, ext4, NFS)** organizes data in a hierarchical tree of directories and files.
* **Block Storage (e.g., SAN, AWS EBS)** treats storage as raw unformatted blocks accessed by sector offset without metadata.
* **Object Storage (e.g., Amazon S3, AegisStore)** manages data as discrete, immutable units called "objects" identified by a unique ID, accompanied by rich metadata and cryptographic checksums, and accessible via flat REST APIs.

---

### Q3: What is the architecture of AegisStore?
**Answer:** AegisStore follows a two-tier architecture:
1. **Control Plane (Metadata Controller Gateway)**: Powered by FastAPI and SQLite with WAL mode. It manages object metadata, replica locations, health heartbeats, failure detection, and self-healing jobs.
2. **Data Plane (Storage Nodes)**: Four independent storage daemons (`node-1` to `node-4`) running on ports 8001–8004. Each node stores binary files in isolated directories on disk and calculates cryptographic hashes on demand.

---

### Q4: How is fault tolerance achieved?
**Answer:** Through **N-way replication** (default: 3 replicas per object). Every uploaded file is written across 3 distinct healthy storage nodes. If any single node (or even two nodes in a 3-replica setup) crashes, the remaining healthy replica immediately serves reads without data loss.

---

### Q5: How does the system detect that a storage node has failed?
**Answer:** AegisStore uses a background **Health Monitoring Service** that sends periodic HTTP heartbeat pings (`GET /health`) to every registered storage node every 3 seconds. If a node fails to respond within a 2-second timeout window, or returns connection refused, the Failure Detector immediately marks the node as `OFFLINE` and triggers autonomous self-healing.

---

### Q6: Walk me through what happens when Node 2 crashes.
**Answer:**
1. **Detection**: The health monitor fails to reach Node 2 and marks it `OFFLINE`.
2. **Impact Assessment**: The Repair Manager queries the database for all objects that held a replica on Node 2.
3. **Survivor Inspection**: For each affected object, the system verifies that healthy replicas remain on Node 1 and Node 3.
4. **Target Allocation**: The system searches for an online node that does not already hold this object (Node 4).
5. **Data Transfer**: The system streams the file from Node 1 or Node 3 to Node 4.
6. **Integrity Validation**: Node 4 calculates the SHA-256 of the received file and confirms it matches the authoritative checksum.
7. **Metadata Commit**: The database records Node 4 as a healthy replica, restoring the object to 3 healthy replicas.

---

### Q7: What is "Bit Rot" and how does AegisStore detect and fix it?
**Answer:**
* **Bit Rot** (silent data corruption) occurs when physical disk degradation, electromagnetic interference, or accidental tampering flips bits in a stored file without OS error.
* **Detection**: The Background Integrity Auditor asks storage nodes to recalculate the SHA-256 of stored files on disk and compares them against the authoritative checksum recorded in metadata.
* **Repair**: When a mismatch is detected, the replica is marked `CORRUPTED`. AegisStore pulls a clean replica from a healthy peer node and overwrites the corrupted file on disk, restoring data integrity automatically.

---

### Q8: What consistency model does AegisStore implement under the CAP Theorem?
**Answer:** AegisStore implements a **CP (Consistency + Partition Tolerance)** model:
* Metadata operations are ACID-compliant and strongly consistent via SQLite with Write-Ahead Logging.
* Writes require a quorum ($W \ge 2$) of healthy nodes to acknowledge successful write and matching SHA-256 before confirming success.
* During network partitions, partitioned nodes are isolated and rejected, preventing split-brain writes and inconsistent replica versions.

---

### Q9: How are concurrent reads and writes handled without race conditions?
**Answer:**
1. **Per-Object Asynchronous Locks (`asyncio.Lock`)**: The Metadata Manager maintains a lock dictionary keyed by `object_id`. Concurrent uploads or downloads of *different* objects proceed in parallel. Operations on the *same* object are serialized.
2. **SQLite WAL Mode (`PRAGMA journal_mode=WAL`)**: Allows concurrent reads while writes are being committed to the write-ahead log.

---

### Q10: What happens if a storage node crashes while a client is downloading a file?
**Answer:** The API Gateway implements **Transparent Failover**. When serving `GET /api/objects/{id}/download`, if the primary replica node times out or returns an error, the gateway catches the exception, logs a warning event, and immediately retrieves the stream from the next healthy replica node without failing the client's request.

---

### Q11: What is Write Quorum and why is it important?
**Answer:** Write Quorum ($W$) is the minimum number of storage nodes that must successfully acknowledge a write for the transaction to be considered successful. In AegisStore, $W = 2$. If fewer than 2 nodes acknowledge the write, the system automatically rolls back any partial writes on the survivor nodes to prevent orphaned data.

---

### Q12: Why did you use SHA-256 instead of CRC32 or MD5?
**Answer:**
* **CRC32** is fast but has high collision rates and cannot protect against intentional tampering.
* **MD5** has known cryptographic collision vulnerabilities.
* **SHA-256** is an industry standard offering 256 bits of cryptographic security, practically zero collision probability, and absolute proof of data integrity.

---

### Q13: What is the Storage Rebalancing feature and what is the "Safe Rule"?
**Answer:**
* Rebalancing detects storage capacity skew across nodes (e.g., Node 1 is 90% full while Node 2 is 20% full) and migrates replicas to balance cluster utilization.
* **The Safe Rule**: AegisStore *never* deletes the source replica on the overloaded node until the new replica is completely written, checksum-verified on the destination node, and committed in metadata. This guarantees zero data loss during migrations.

---

### Q14: How does AegisStore handle Object Versioning?
**Answer:** Every stored object has an integer `version` field (starting at 1). When an object is updated, the version increments atomically. Replicas record their corresponding version number. An older replica (e.g., from a recovered node that was offline during an update) is flagged as `STALE` and cannot overwrite newer data.

---

### Q15: How can this system run on a single laptop?
**Answer:** The 4 distributed storage nodes are simulated as independent FastAPI daemons listening on ports `8001`, `8002`, `8003`, and `8004`, each storing files in isolated disk directories (`storage/node1`, `storage/node2`, etc.). All communication occurs over standard HTTP REST requests. The entire cluster can be launched with a single command (`python scripts/run_system.py`) or via Docker Compose.
