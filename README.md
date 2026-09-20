# Enterprise CDC & Delta Lakehouse Ingestion Platform

[![CI Pipeline](https://github.com/Jigar-23/enterprise-cdc-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Jigar-23/enterprise-cdc-lakehouse/actions)
[![Python Version](https://img.shields.io/badge/Python-3.11%20%7C%203.12%20%7C%203.14-blue.svg)](https://www.python.org/)
[![Storage](https://img.shields.io/badge/Format-Parquet%20%2F%20Delta-orange.svg)](https://delta.io/)
[![CDC Engine](https://img.shields.io/badge/CDC-Debezium%20%2B%20Postgres%20WAL-red.svg)](https://debezium.io/)
[![Streaming Broker](https://img.shields.io/badge/Broker-Redpanda%20%2F%20Kafka-brightgreen.svg)](https://redpanda.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A high-performance, log-based **Change Data Capture (CDC)** streaming lakehouse pipeline engineered for enterprise ERP workloads. Replicates sub-second relational state changes from **PostgreSQL Write-Ahead Logs (WAL)** into an ACID **Medallion Lakehouse** architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) with **idempotent upsert semantics (`MERGE INTO`)**, **out-of-order event reconciliation**, and an automated **Dead Letter Queue (DLQ)** for poison pill isolation.

---

## Architecture Overview

```
+-------------------+        +--------------------+        +---------------------+
| PostgreSQL 15     |  WAL   | Debezium Connector | Kafka  | Redpanda / Kafka    |
| (OLTP ERP System) |------->| (Kafka Connect)    |------->| (Event Streaming)   |
| Orders, Inventory |        | pgoutput plugin    |        | Partitioned Topics  |
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

## Key Features

- **Log-Based CDC via PostgreSQL WAL:** Uses `pgoutput` and `REPLICA IDENTITY FULL` to capture insert, update, and hard-delete operations without triggering database lock contention or `SELECT` table scans.
- **Medallion Lakehouse Storage:**
  - **Bronze Layer:** Append-only raw changelog stored in Snappy-compressed Parquet with ingestion metadata (`_cdc_op`, `_cdc_ts_ms`, `_tx_id`, `_lsn`).
  - **Silver Layer:** Cleaned, deduplicated, current-state tables implementing ACID `MERGE INTO` semantics.
  - **Gold Layer:** Fast columnar analytical aggregations powered by **DuckDB** for executive KPIs and operational alerts.
- **Out-of-Order Event Handling:** Deterministic reconciliation using timestamp and LSN comparisons. Stale updates arriving late due to network partitions or consumer restarts are discarded to prevent state regression.
- **Poison Pill Isolation (DLQ):** Messages failing structural JSON checks or strict Pydantic V2 domain contracts are routed to an isolated quarantine repository with diagnostic stack traces, ensuring zero consumer downtime.
- **Zero Spark Overhead:** Optimized using **PyArrow** and **DuckDB**, delivering sub-second lakehouse ingestion and analytics on lightweight commodity hardware.

---

## Directory Structure

```
enterprise-cdc-lakehouse/
├── .github/
│   └── workflows/
│       └── ci.yml                 # GitHub Actions CI workflow (linting, tests, replay)
├── analytics/
│   ├── __init__.py
│   └── gold_analytics.py          # DuckDB OLAP queries over Silver & Bronze layers
├── connectors/
│   ├── postgres-connector.json    # Debezium PostgreSQL connector configuration
│   └── register-postgres.sh       # Automated Kafka Connect registration script
├── pipeline/
│   ├── __init__.py
│   ├── consumer.py                # Streaming CDC consumer & routing engine
│   ├── delta_engine.py            # Bronze append, Silver MERGE INTO & DLQ quarantine
│   └── models.py                  # Pydantic V2 schemas for Debezium envelopes & entities
├── scripts/
│   └── simulate_erp_transactions.py # High-fidelity CDC event stream generator
├── sql/
│   └── init.sql                   # Schema definitions, seed rows, and WAL publication
├── tests/
│   └── test_cdc_pipeline.py       # Comprehensive pytest suite
├── ARCHITECTURE.md                # In-depth architectural design and ADR documentation
├── docker-compose.yml             # Local infrastructure (Postgres, Redpanda, Debezium, MinIO)
├── requirements.txt               # Production dependencies
└── README.md
```

---

## Quickstart Guide

### 1. Prerequisites
- Python 3.11+ (or virtualenv)
- Docker & Docker Compose (optional for local infrastructure deployment)

### 2. Installation
```bash
git clone https://github.com/Jigar-23/enterprise-cdc-lakehouse.git
cd enterprise-cdc-lakehouse

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Run the Test Suite
```bash
pytest -v tests/
```

### 4. Execute the End-to-End CDC Simulation
Run the standalone transaction simulator to experience WAL ingestion, Silver upsert, and DLQ quarantine in action:
```bash
python scripts/simulate_erp_transactions.py
```

### 5. Query Gold Analytics
```bash
python analytics/gold_analytics.py
```

---

## Full Infrastructure Deployment (Optional)

To spin up the distributed infrastructure (Postgres, Redpanda, Debezium, MinIO):
```bash
# 1. Start containers
docker-compose up -d

# 2. Register Debezium connector
./connectors/register-postgres.sh

# 3. Access management UIs:
#    - Redpanda Console: http://localhost:8080
#    - Debezium Connect: http://localhost:8083
#    - MinIO Object Storage: http://localhost:9001 (minioadmin / minioadmin)
```

---

## Interview Talking Points & Design Rationale

### 1. Why Log-Based CDC over Polling (JDBC)?
Polling databases using timestamps (`WHERE updated_at > last_poll`) imposes significant query load, requires indexes on updated columns, misses hard `DELETE` operations, and fails to capture rapid intermediate state changes. Reading directly from the Write-Ahead Log (WAL) via Debezium eliminates query load on production OLTP databases and captures exact, atomic state changes.

### 2. How are Out-of-Order Events Handled?
Distributed event streaming can lead to out-of-order message delivery. The `DeltaLakehouseEngine.merge_silver()` method checks `incoming.ts_ms >= existing.ts_ms`. If a delayed event arrives with an older timestamp than the row's current state, it is safely ignored, guaranteeing data consistency.

### 3. How does this serve Continental ContiTech?
In high-throughput automotive manufacturing (e.g., ContiTech tire and hose plants), ERP systems process thousands of JIT/JIS inventory adjustments. Sub-second CDC streaming into an ACID lakehouse enables real-time supply chain visibility, automated supplier replenishment, and predictive parts allocation.

---

## License
Distributed under the Apache 2.0 License. See `LICENSE` for more information.
