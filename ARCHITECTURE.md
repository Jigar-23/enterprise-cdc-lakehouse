# Architecture & Technical Design: Enterprise CDC & Delta Lakehouse Platform

## 1. Executive Summary & Problem Context
In enterprise manufacturing and automotive tier-1 supply chains (e.g., Continental ContiTech JIT/JIS plants), Enterprise Resource Planning (ERP) databases process millions of order, inventory, and shipment state transitions daily. 

Traditional batch ETL (polling databases every hour or day via JDBC) suffers from critical limitations:
1. **Query Overhead & Lock Contention:** Polling operational OLTP databases (`SELECT * WHERE updated_at > ?`) triggers full table scans, increases index bloat, and risks lock contention on transaction hot-paths.
2. **Missing Hard Deletes & Ephemeral States:** Polling misses row deletions and intermediate transitions (e.g., `PENDING -> PROCESSING -> CONFIRMED` in seconds).
3. **High Latency:** Downstream warehouse and logistics systems operate on stale data (hours old), preventing real-time inventory allocation.

This project delivers an **end-to-end, sub-second Log-Based Change Data Capture (CDC)** streaming lakehouse that replicates changes from PostgreSQL WAL (Write-Ahead Log) into an ACID Medallion Lakehouse without impacting OLTP throughput.

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
| Stream Ingestion & Delta Processing Engine (Python / Pydantic / PyArrow)               |
|                                                                                        |
|   1. Envelope & Contract Validation (Pydantic V2)                                      |
|      +--> Schema Violations / Poison Pills ---------> [ Dead Letter Queue / Quarantine]|
|                                                                                        |
|   2. Bronze Layer: Append-Only Immutable Changelog (Parquet partitioned by date)       |
|                                                                                        |
|   3. Silver Layer: ACID MERGE INTO (Deduplication + Out-of-Order LSN Resolution)       |
+---------------------------------------------------------------------+------------------+
                                                                      |
                                                                      v
+---------------------------------------------------------------------+------------------+
| Gold Analytics & OLAP Layer (DuckDB In-Process Engine)                                 |
|   - Real-Time Critical Stock Alerts (< 500 units)                                      |
|   - Revenue & Customer Order Velocity                                                  |
|   - CDC Throughput & Replay Audits                                                     |
+----------------------------------------------------------------------------------------+
```

---

## 3. Core Design Decisions & Implementation Mechanics

### 3.1 Log-Based CDC via PostgreSQL WAL & `pgoutput`
- **Replication Mechanism:** Utilizes PostgreSQL logical decoding with the native `pgoutput` plugin and `REPLICA IDENTITY FULL`.
- **Zero Query Overhead:** Debezium reads directly from the PostgreSQL Write-Ahead Log (WAL) via replication slots, completely eliminating `SELECT` query overhead on production tables.
- **Full Historical Context:** `REPLICA IDENTITY FULL` guarantees that both the previous row image (`before`) and current row image (`after`) are broadcast, enabling accurate downstream diffing and audit trails.

### 3.2 Medallion Storage Architecture
| Layer | Name | Format | Semantics | Purpose |
|---|---|---|---|---|
| **Bronze** | Raw Changelog | Parquet (Snappy) | Append-Only | Complete immutable audit log containing every `c` (insert), `u` (update), `d` (delete) and `r` (snapshot) event with ingestion timestamp and LSN metadata. |
| **Silver** | Curated Enterprise Tables | Parquet / Delta | ACID Upsert (`MERGE INTO`) | Deduplicated, current state of entities (`orders`, `inventory`). Late-arriving events are reconciled using timestamp ordering. |
| **Gold** | Business Aggregates | Parquet / DuckDB Views | Analytical OLAP | KPI metrics: Low-stock triggers, revenue by customer, change velocity. |
| **DLQ** | Quarantine | JSON Lines | Error Isolated | Poison pills, schema mismatches, and malformed JSON payloads isolated with diagnostic stack traces. |

### 3.3 ACID `MERGE INTO` & Out-of-Order Event Handling
In distributed streaming networks, network partitions or consumer restarts can cause events to arrive out of order (e.g., an order update with timestamp `t=1000` arriving after an update with `t=2000`).
- **Resolution Algorithm:**
  ```python
  if incoming.primary_key in existing_table:
      if incoming.ts_ms >= existing_table[pk].ts_ms:
          # Incoming event is newer: apply update
          existing_table[pk] = incoming
      else:
          # Stale event arriving late: ignore update to preserve consistency
          metrics.ignored_count += 1
  ```
- **Atomicity:** Updates to the Silver dataset are committed via atomic file replacement (`os.replace(temp_target, final_target)`), ensuring reader processes never observe partial or torn writes.

### 3.4 Poison Pill Isolation & Dead Letter Queue (DLQ)
Corrupt payloads or schema contract violations must never crash long-running consumer processes.
- The consumer implements a two-stage validation filter:
  1. Structural parsing (valid JSON syntax).
  2. Pydantic contract validation (`OrderPayload`, `InventoryPayload`).
- If either check fails, the record is immediately rerouted to the Quarantine store with metadata (`quarantined_at`, `error_type`, `error_message`, `source_topic`) without dropping message consumer offsets.

---

## 4. Verification & Testing Strategy
- **Unit & Integration Suite (`tests/test_cdc_pipeline.py`):**
  - Validates schema contract constraints (positive price, valid status enums).
  - Verifies Bronze append fidelity and schema evolution.
  - Tests ACID Silver merge, idempotency (re-playing identical streams produces identical state), and out-of-order rejection.
  - Confirms poison pill routing into the DLQ.
  - Validates Gold DuckDB OLAP aggregation logic.
- **Transaction Simulator (`scripts/simulate_erp_transactions.py`):**
  - Replays a realistic supply chain lifecycle: snapshot read -> order placement -> status update -> inventory reserve -> poison pill injection.

---

## 5. Continental ContiTech Application
In an industrial manufacturing environment:
- **Tire & Hose Manufacturing:** When automotive OEMs (e.g., BMW, Mercedes, Daimler) submit EDI orders, CDC updates inventory availability in real time across European distribution centers.
- **Preventing Stockouts:** Sub-second inventory visibility allows plants to trigger automated raw material procurement before safety stock drops below critical thresholds.
