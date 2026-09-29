# Enterprise CDC & Delta Lakehouse

A streaming Change Data Capture (CDC) pipeline that replicates changes from PostgreSQL Write-Ahead Logs (WAL) into an ACID Delta Lakehouse using a Medallion architecture (Bronze, Silver, Gold).

Built with Python, Debezium, Apache Kafka / Redpanda, `deltalake` (Rust-backed Delta Lake engine), and DuckDB.

---

## Overview

Traditional batch ETL pipelines that poll databases using `SELECT * WHERE updated_at > ?` have several drawbacks:
- Query overhead and lock contention on operational OLTP tables.
- Missed hard deletes (`DELETE` statements leave no updated timestamp).
- Inability to capture intermediate row updates occurring between poll intervals.

This project implements log-based CDC by reading PostgreSQL's Write-Ahead Log directly via Debezium and logical replication (`pgoutput`). Changes are streamed through Kafka topics and consumed into a Delta Lakehouse.

### Architecture

```
PostgreSQL (OLTP)
   │  WAL (pgoutput)
   ▼
Debezium Connector (Kafka Connect)
   │  CDC events
   ▼
Kafka / Redpanda
   │
   ▼
Python CDC Consumer
   ├── Validation (Pydantic) ──► Dead Letter Queue (quarantine/)
   ├── Bronze Layer: Append-only raw event changelog (Delta Lake)
   └── Silver Layer: Deduplicated state with ACID MERGE & monotonic LSN ordering (Delta Lake)
         │
         ▼
      Gold Layer: Analytical queries & reports (DuckDB over Delta / Arrow)
```

---

## How It Works

### 1. Medallion Lakehouse Structure
- **Bronze (Raw Changelog):** Append-only Delta table preserving raw CDC envelopes with transaction metadata (`_cdc_op`, `_cdc_ts_ms`, `_lsn`, `tx_id`, `before`, `after`). Partitioned by date.
- **Silver (Current State):** Cleaned, deduplicated entity tables (`orders`, `inventory`). Updated using Delta Lake `MERGE INTO` operations keyed by primary keys.
- **Gold (Analytics):** Business views and aggregations queried directly using DuckDB over Delta Parquet files (e.g. low-stock alerts, customer order velocity).

### 2. Out-of-Order Event Handling & LSN Ordering
In distributed streaming, network retries can cause messages to arrive out of order. Instead of relying solely on server timestamps (which can suffer from clock drift or share the same millisecond), the pipeline uses the PostgreSQL **Log Sequence Number (LSN)** as the primary monotonic ordering authority:
```sql
(COALESCE(source._lsn, 0) > COALESCE(target._lsn, 0))
OR (COALESCE(source._lsn, 0) = COALESCE(target._lsn, 0) AND source.ts_ms >= target.ts_ms)
```
If an incoming event is older than the current record in the Silver table, the update is ignored.

### 3. Tombstone Deletes
When a record is deleted in PostgreSQL (`op = 'd'`), physically deleting the row in the Silver table risks "resurrecting" the row if a delayed, out-of-order update arrives later. The pipeline instead writes a tombstone (`_is_deleted = True`) while updating the LSN. Stale updates with an older LSN are safely rejected, while Gold analytics filter out tombstoned records with `WHERE COALESCE(_is_deleted, false) = false`.

### 4. Error Handling & Dead Letter Queue (DLQ)
- Malformed payloads and schema validation failures are written to `quarantine/` with diagnostic metadata (error reason, topic, partition, offset). The Kafka offset is committed so bad messages do not block the consumer.
- For storage or Delta transaction failures, an exception is raised and the Kafka offset is **not** committed. On restart, the consumer replays the message idempotently.

---

## Project Structure

```
enterprise-cdc-lakehouse/
├── analytics/
│   └── gold_analytics.py            # DuckDB queries over Delta Lake tables
├── connectors/
│   ├── postgres-connector.json      # Debezium PostgreSQL connector config
│   └── register-postgres.sh         # Connector registration script
├── pipeline/
│   ├── consumer.py                  # Kafka streaming consumer with manual commits & DLQ
│   ├── delta_engine.py              # Bronze/Silver Delta Lake writer & MERGE logic
│   └── models.py                    # Pydantic schemas for CDC events & domain entities
├── scripts/
│   ├── benchmark_latency.py         # Latency and throughput benchmark
│   ├── bootstrap.sh                 # Environment startup script
│   ├── demo.sh                      # End-to-end pipeline demonstration
│   ├── simulate_erp_transactions.py # Generates test transactions
│   └── verify_live_stack_e2e.py     # Live integration test against Docker stack
├── sql/
│   └── init.sql                     # PostgreSQL schema and replication publication setup
├── tests/
│   ├── test_cdc_pipeline.py         # Core pipeline tests
│   ├── test_unit_pipeline.py        # Unit tests (schemas, ordering logic, DLQ)
│   └── test_integration_cdc.py      # End-to-end stream replay and crash recovery
├── docker-compose.yml               # Local infrastructure (Postgres, Redpanda, Debezium)
├── requirements.txt                 # Python dependencies
└── README.md
```

---

## Quickstart

### Prerequisites
- Python 3.11+
- Docker and Docker Compose (optional, for running live services)

### Installation
```bash
git clone https://github.com/Jigar-23/enterprise-cdc-lakehouse.git
cd enterprise-cdc-lakehouse

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running Tests
Run the test suite:
```bash
pytest -v
```

### Running the Demo
An offline end-to-end demo processes sample CDC events through Bronze, Silver, DLQ, and Gold analytics:
```bash
./scripts/demo.sh
```

### Running Benchmarks
Measure end-to-end ingestion latency and throughput:
```bash
python3 scripts/benchmark_latency.py --events 100
```

---

## Running with Docker Compose

To run the full stack locally with PostgreSQL, Redpanda, Debezium, and the consumer:

1. **Start infrastructure:**
   ```bash
   ./scripts/bootstrap.sh
   ```
   Or manually:
   ```bash
   docker compose up -d postgres-source kafka connect redpanda-console
   ./connectors/register-postgres.sh
   docker compose up -d cdc-consumer
   ```

2. **Web UIs:**
   - Redpanda Console: [http://localhost:8080](http://localhost:8080)
   - Debezium Connect API: [http://localhost:8083/connectors](http://localhost:8083/connectors)

3. **Verify live pipeline:**
   ```bash
   python3 scripts/verify_live_stack_e2e.py
   ```

---

## License

Apache 2.0
