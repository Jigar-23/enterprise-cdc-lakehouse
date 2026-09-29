"""
Live Infrastructure End-to-End Verification:
Proves the complete real infrastructure pipeline:
PostgreSQL (real SQL transaction)
  ↓
WAL / pgoutput
  ↓
Debezium Connector (debezium_slot)
  ↓
Redpanda / Kafka (erp_cdc.public.orders)
  ↓
Python Consumer
  ↓
Bronze Delta Table
  ↓
Silver Delta Table
  ↓
Gold DuckDB OLAP
"""

import os
import sys
import time
import json
import uuid
import logging
import argparse

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("live_e2e")


def run_live_pipeline_e2e(
    host: str = "localhost",
    port: int = 5432,
    user: str = "erp_admin",
    password: str = "erp_secure_password123",
    dbname: str = "erp_database",
    broker: str = "localhost:9092",
    topic_prefix: str = "erp_cdc",
    storage_dir: str = "./lakehouse_storage",
    timeout_sec: int = 30
) -> bool:
    print("======================================================================")
    print("      LIVE INFRASTRUCTURE END-TO-END VALIDATION (REAL WAL -> DELTA)  ")
    print("======================================================================")

    # 1. Connect to PostgreSQL
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 is required. Run 'pip install psycopg2-binary'.")
        return False

    logger.info(f"[1/6] Connecting to PostgreSQL at {host}:{port}/{dbname}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=3
        )
        conn.autocommit = False
    except Exception as e:
        logger.warning(f"[!] PostgreSQL offline or unreachable: {e}")
        logger.info("    This test requires Docker services running. To start: ./scripts/bootstrap.sh")
        return False

    # 2. Insert Real PostgreSQL Transaction
    unique_order_id = f"E2E-{int(time.time())}-{uuid.uuid4().hex[:6]}"
    customer_id = "CUST-CONTINENTAL-TEST"
    sku = "SKU-TIRE-001"
    qty = 25
    price = 2375.00

    logger.info(f"[2/6] Committing real SQL INSERT transaction: {unique_order_id}...")
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (order_id, customer_id, product_id, quantity, total_price, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'CONFIRMED', NOW(), NOW());
                """,
                (unique_order_id, customer_id, sku, qty, price)
            )
        conn.commit()
        logger.info(f"[✓] Transaction committed to PostgreSQL WAL. Order ID: {unique_order_id}")
    finally:
        conn.close()

    # 3. Poll Kafka for Debezium CDC Event
    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        logger.error("confluent-kafka required. Run 'pip install confluent-kafka'.")
        return False

    topic_name = f"{topic_prefix}.public.orders"
    logger.info(f"[3/6] Polling Kafka topic '{topic_name}' for CDC event via broker {broker}...")

    conf = {
        "bootstrap.servers": broker,
        "group.id": f"e2e-verifier-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([topic_name])
    except Exception as e:
        logger.warning(f"[!] Unable to connect to Kafka at {broker}: {e}")
        return False

    start_wait = time.time()
    captured_msg = None
    captured_payload = None

    try:
        while (time.time() - start_wait) < timeout_sec:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                continue

            val_str = msg.value().decode("utf-8")
            try:
                parsed = json.loads(val_str)
                body = parsed.get("payload", parsed)
                after = body.get("after") or {}
                if after.get("order_id") == unique_order_id:
                    captured_msg = msg
                    captured_payload = val_str
                    logger.info(f"[✓] Found corresponding CDC event in Kafka partition {msg.partition()} at offset {msg.offset()}!")
                    break
            except Exception:
                continue
    finally:
        consumer.close()

    if not captured_msg:
        logger.error(f"[FAIL] Did not receive CDC event for {unique_order_id} within {timeout_sec}s timeout.")
        return False

    # 4. Ingest via Python CDC Consumer into Bronze & Silver Delta Lake
    logger.info("[4/6] Processing message through CDCPipelineConsumer into Delta Lake...")
    from pipeline.delta_engine import DeltaLakehouseEngine
    from pipeline.consumer import CDCPipelineConsumer

    engine = DeltaLakehouseEngine(base_storage_dir=storage_dir)
    cdc_consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    res = cdc_consumer.process_raw_message(
        raw_data=captured_payload,
        topic_hint=captured_msg.topic(),
        partition=captured_msg.partition(),
        offset=captured_msg.offset()
    )
    assert res["status"] == "success", f"Consumer processing failed: {res}"
    logger.info(f"[✓] Materialized into Bronze and Silver Delta tables. Status: {res['status']}")

    # 5. Verify Silver Delta Table Materialization
    logger.info("[5/6] Verifying Silver Delta table content...")
    silver_df = engine.get_silver_table("orders")
    matching_rows = silver_df[silver_df["order_id"] == unique_order_id]
    assert len(matching_rows) == 1, f"Expected 1 active row in Silver, found {len(matching_rows)}"
    row = matching_rows.iloc[0]
    assert row["customer_id"] == customer_id
    assert row["quantity"] == qty
    assert float(row["total_price"]) == price
    logger.info(f"[✓] Silver Delta state verified: {unique_order_id} | Status: {row['status']}")

    # 6. Verify Gold DuckDB Analytics
    logger.info("[6/6] Verifying Gold DuckDB OLAP layer query...")
    from analytics.gold_analytics import GoldLakehouseAnalytics
    analytics = GoldLakehouseAnalytics(lakehouse_dir=storage_dir)
    rev_df = analytics.get_order_revenue_summary()
    assert not rev_df.empty
    logger.info(f"[✓] Gold OLAP verified:\n{rev_df}")

    print("======================================================================")
    print("  [SUCCESS] FULL LIVE INFRASTRUCTURE PIPELINE VERIFIED END-TO-END!    ")
    print("  PostgreSQL WAL -> Debezium -> Redpanda -> Bronze -> Silver -> Gold  ")
    print("======================================================================")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Live Infrastructure End-to-End Verifier")
    parser.add_argument("--host", default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")))
    parser.add_argument("--user", default=os.getenv("POSTGRES_USER", "erp_admin"))
    parser.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "erp_secure_password123"))
    parser.add_argument("--dbname", default=os.getenv("POSTGRES_DB", "erp_database"))
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--topic-prefix", default=os.getenv("KAFKA_TOPIC_PREFIX", "erp_cdc"))
    parser.add_argument("--storage-dir", default=os.getenv("LAKEHOUSE_STORAGE_DIR", "./lakehouse_storage"))
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    success = run_live_pipeline_e2e(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        broker=args.broker,
        topic_prefix=args.topic_prefix,
        storage_dir=args.storage_dir,
        timeout_sec=args.timeout
    )
    sys.exit(0 if success else 1)
