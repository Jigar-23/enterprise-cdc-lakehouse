# Architecture & Technical Design: Enterprise CDC & Delta Lakehouse Platform

## 1. Executive Summary & Problem Context
In enterprise supply chains and manufacturing systems, Enterprise Resource Planning (ERP) databases process high volumes of order, inventory, and fulfillment state transitions daily. 

Traditional batch ETL (polling databases periodically via JDBC) suffers from three critical architectural flaws:
1. **Query Overhead & Lock Contention:** Polling operational OLTP databases (`SELECT * WHERE updated_at > ?`) triggers table scans, increases index bloat, and risks lock contention on transaction hot-paths.
2. **Missing Hard Deletes & Ephemeral States:** Polling misses row deletions and intermediate transitions (e.g., `PENDING -> PROCESSING -> CONFIRMED` occurring within milliseconds).
3. **High Latency:** Downstream warehouse and logistics systems operate on stale data (hours old), preventing real-time inventory allocation.

This project implements an **end-to-end, sub-second Log-Based Change Data Capture (CDC)** streaming lakehouse that replicates changes from PostgreSQL WAL (Write-Ahead Log) into an ACID Medallion Lakehouse powered by native **Delta Lake** (`deltalake`) and **DuckDB**, without impacting OLTP throughput.

---

## 2. End-to-End Architectural Topology

```
+-------------------+        +--------------------+        +---------------------+
| PostgreSQL 15     |  WAL   | Debezium Connector | Kafka  | Redpanda / Kafka    |
| (OLTP ERP Database|------->| (Kafka Connect)    |------->| (Event Streaming)   |
| Orders, Inventory)|        | pgoutput plugin    |        | Partitioned Topics  |
+-------------------+        +--------------------+        +----------+----------+
                                                                      |
                                                                      | CDC Stream
                                                                      v
+---------------------------------------------------------------------+------------------+
| Stream Ingestion & Delta Processing Engine (Python / Pydantic / deltalake)             |
|                                                                                        |
|   1. Envelope & Contract Validation (Pydantic V2)                                      |
|      +--> Schema Violations / Poison Pills ---------> [ Dead Letter Queue / Quarantine]|
|                                                                                        |
|   2. Bronze Layer: Append-Only Changelog (Native Delta Table partitioned by date)      |
|      +--> Recorded with _delta_log transaction commit entries                          |
|                                                                                        |
|   3. Silver Layer: ACID MERGE INTO (Deduplication + Monotonic Timestamp/LSN Resolution)|
|      +--> Explicit manual offset commit ONLY after successful durable writes           |
+---------------------------------------------------------------------+------------------+
                                                                      |
                                                                      v
+---------------------------------------------------------------------+------------------+
| Gold Analytics & OLAP Layer (DuckDB In-Process Engine)                                 |
|   - Real-Time Critical Stock Alerts (< 500 units / net free stock allocation)          |
|   - Revenue & Customer Order Velocity (GMV, AOV by Customer & Status)                  |
|   - CDC Throughput & Replay Audits directly over Delta tables                          |
+----------------------------------------------------------------------------------------+
```

---

## 3. Core Design Decisions & Implementation Mechanics

### 3.1 Log-Based CDC via PostgreSQL WAL & `pgoutput`
- **Replication Mechanism:** Utilizes PostgreSQL logical decoding with the native `pgoutput` plugin and `REPLICA IDENTITY FULL`.
- **Zero Query Overhead:** Debezium reads directly from the PostgreSQL Write-Ahead Log (WAL) via replication slot `debezium_slot`, completely eliminating `SELECT` query overhead on production tables.
- **Full Historical Context:** `REPLICA IDENTITY FULL` guarantees that both the previous row image (`before`) and current row image (`after`) are broadcast, enabling accurate downstream diffing and audit trails.

### 3.2 Medallion Storage Architecture
| Layer | Name | Format | Semantics | Purpose |
|---|---|---|---|---|
| **Bronze** | Raw Changelog | Delta Lake (Rust engine) | Append-Only with `_delta_log` | Complete immutable audit log containing 15 raw CDC attributes (`event_id`, `source_topic`, `partition`, `offset`, `key`, `op`, `before`, `after`, `source`, `transaction`, `ts_ms`, `cdc_lsn`, `tx_id`, `ingested_at`, `_partition_date`) partitioned by `_partition_date`. Duplicate-tolerant. |
| **Silver** | Curated Enterprise Tables | Delta Lake (Rust engine) | ACID Upsert (`MERGE INTO`) | Deduplicated, current state of entities (`orders`, `inventory`). Monotonic `(COALESCE(source._lsn, 0) > COALESCE(target._lsn, 0)) OR (source._lsn = target._lsn AND source.ts_ms >= target.ts_ms)` predicate. Tombstone deletion (`_is_deleted = True, _cdc_op = 'd'`). |
| **Gold** | Business Aggregates | DuckDB Views over Arrow | Analytical OLAP | Zero-copy columnar queries over Delta tables: Low-stock triggers, customer revenue GMV, change velocity audits. Automatically filters out tombstoned rows (`COALESCE(_is_deleted, false) = false`). |
| **DLQ** | Quarantine | JSON Lines | Error Isolation | Poison pills, entity mismatches, and schema validation failures isolated with diagnostic fields (`error_type`, `error_message`, `raw_payload`, `topic`, `partition`, `offset`, `quarantined_at`). |

### 3.3 ACID `MERGE INTO`, Monotonic Ordering & Tombstone Deletes
In distributed streaming systems, network retries and consumer restarts cause out-of-order message delivery. In PostgreSQL, the **Log Sequence Number (LSN)** is the monotonic, strictly sequential ground truth of all transaction commits. Wall-clock timestamps (`ts_ms`) can drift across servers or share identical values within the same millisecond.

- **Monotonic Resolution Predicate:**
  Delta Lake MERGE applies updates and deletes only when the incoming record is strictly newer in LSN, or equal in LSN with a non-regressive timestamp:
  ```sql
  (COALESCE(source._lsn, 0) > COALESCE(target._lsn, 0))
  OR (COALESCE(source._lsn, 0) = COALESCE(target._lsn, 0) AND source.ts_ms >= target.ts_ms)
  ```

- **9-Permutation Out-of-Order Resolution Matrix:**
  1. *Newer ts, newer LSN:* Applied (standard forward progression).
  2. *Older ts, older LSN:* Rejected (stale replayed event).
  3. *Same ts, higher LSN:* Applied (rapid sequential transactions within same millisecond).
  4. *Same ts, lower LSN:* Rejected (out-of-order within same millisecond).
  5. *Newer ts, lower LSN:* Rejected (clock drift; LSN is authoritative ground truth).
  6. *Older ts, higher LSN:* Applied (clock drift; LSN is authoritative ground truth).
  7. *Duplicate event (same ts, same LSN):* Idempotent no-op (state preserved).
  8. *Stale update after tombstone delete:* Rejected (prevents deleted row resurrection).
  9. *Newer update after tombstone delete:* Applied (valid re-insertion / entity resurrection).

- **Tombstone Deletion Semantics:**
  Physical row deletion leaves a vulnerability: if a row is deleted from Silver and a stale update arrives later, it finds no matching row and gets inserted as a "resurrected" record. Silver prevents resurrection by updating `_is_deleted = True, _cdc_op = 'd'`, preserving the delete's LSN. Stale updates with lower LSN are rejected by the monotonic predicate. Downstream consumers query active records via `get_silver_table(include_deleted=False)`.

### 3.4 Discrete Failure Semantics & Crash Recovery Matrix
- **Schema Validation Failure / Entity Mismatch:** Message routed to Dead Letter Queue (`quarantine/`) with diagnostic fields (`error_type`, `error_message`, `raw_payload`, `topic`, `partition`, `offset`, `quarantined_at`). Kafka offset is committed to prevent consumer crash loops.
- **Delta Storage Failure (I/O, Lock, Disk):** Exception is raised immediately and Kafka offset is **NOT** committed. On restart, Kafka replays the uncommitted message. Idempotent Silver merge ensures safe replay without duplicate state.

### 3.5 Empirical Performance & Latency Benchmarks
Empirical measurements recorded via `scripts/benchmark_latency.py` (macOS Apple Silicon ARM64, Python 3.14, local NVMe SSD):
- **Latency (p50):** 46.9 - 66.2 ms
- **Latency (p95):** 64.2 - 99.5 ms
- **Latency (p99):** 98.9 - 105.0 ms
- **Throughput (unbatched):** 15 - 20 events/sec (single-threaded, atomic per-event Delta Lake write and `_delta_log` transaction commit).

---

## 4. Verification & Testing Strategy
- **Unit Suite (`tests/test_unit_pipeline.py`):**
  - Validates strict Pydantic schema contracts (`extra="forbid"`, positive price, valid status enums).
  - Verifies WAL hex LSN parsing to numeric 64-bit integers.
  - Verifies Bronze Delta table creation, version increments, schema evolution, and full 15-attribute raw CDC context preservation.
  - Tests ACID Silver merge, idempotency, all 9 monotonic out-of-order permutations, and tombstone delete lifecycle.
  - Confirms poison pill and entity mismatch routing into DLQ with full metadata.
  - Validates storage failure blocking offset commits.
  - Validates Gold DuckDB OLAP aggregation logic.
- **Integration Suite (`tests/test_integration_cdc.py`):**
  - Full end-to-end event stream replay through `CDCPipelineConsumer` asserting Bronze and Silver Delta logs and DLQ outputs.
  - Consumer crash matrix simulating Silver storage failure blocking commits and verifying replay idempotence.
  - Live PostgreSQL database connectivity and publication checks (`scripts/verify_live_stack_e2e.py`).

---

## 5. Production Use Cases
In supply chain and manufacturing operations:
- **Just-in-Time (JIT) Fulfillment:** When purchase orders are placed, CDC replicates order confirmations to downstream distribution and fulfillment services with sub-second latency.
- **Inventory Allocation & Stockout Prevention:** Continuous inventory CDC tracks component reservations in real time, triggering automated alerts when available stock dips below safety thresholds.
