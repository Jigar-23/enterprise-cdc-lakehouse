"""
ERP Transaction & CDC Stream Simulator:
Generates high-fidelity transactions for end-to-end lakehouse validation.

Supports two operational modes:
1. --database-mode (Live DB):
   Connects to real PostgreSQL via psycopg2 and executes genuine transactional
   workloads (INSERT, rapid UPDATEs, inventory allocations, DELETE).
   PostgreSQL writes WAL records -> Debezium captures -> Kafka/Redpanda -> Consumer.

2. --stream-mode / --unit-test-mode (Offline Replay):
   Generates a deterministic sequence of Debezium CDC envelopes (snapshot reads,
   inserts, updates, out-of-order stale events, deletes, and poison pills) and passes
   them directly to CDCPipelineConsumer to validate Medallion lakehouse logic offline.
"""

import os
import sys
import json
import time
import uuid
import random
import logging
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("erp_simulator")


def generate_sample_cdc_stream(topic_prefix: str = "erp_cdc") -> List[Dict[str, Any]]:
    """
    Constructs a deterministic sequence of Debezium CDC events representing
    a realistic enterprise order-to-delivery lifecycle.
    """
    now_ms = int(time.time() * 1000)

    events = [
        # 1. Snapshot Read: Initial Inventory State
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.inventory.Envelope"},
            "payload": {
                "op": "r",
                "ts_ms": now_ms - 60000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 60000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "inventory",
                    "txId": 1001,
                    "lsn": 24891000
                },
                "before": None,
                "after": {
                    "sku": "SKU-TIRE-001",
                    "warehouse_id": "WH-FRANKFURT-1",
                    "available_stock": 1250,
                    "reserved_stock": 45,
                    "last_restocked_at": "2026-09-20T10:00:00Z"
                }
            }
        },
        # 2. INSERT: New Order Created
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.orders.Envelope"},
            "payload": {
                "op": "c",
                "ts_ms": now_ms - 50000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 50000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "orders",
                    "txId": 1002,
                    "lsn": 24891500
                },
                "before": None,
                "after": {
                    "order_id": "ORD-2026-9004",
                    "customer_id": "CUST-MERCEDES",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 80,
                    "total_price": 7600.00,
                    "status": "PENDING",
                    "created_at": "2026-09-20T10:05:00Z",
                    "updated_at": "2026-09-20T10:05:00Z"
                }
            }
        },
        # 3. UPDATE: Order Status Advanced to CONFIRMED
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.orders.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 40000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 40000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "orders",
                    "txId": 1003,
                    "lsn": 24892000
                },
                "before": {
                    "order_id": "ORD-2026-9004",
                    "customer_id": "CUST-MERCEDES",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 80,
                    "total_price": 7600.00,
                    "status": "PENDING"
                },
                "after": {
                    "order_id": "ORD-2026-9004",
                    "customer_id": "CUST-MERCEDES",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 80,
                    "total_price": 7600.00,
                    "status": "CONFIRMED",
                    "created_at": "2026-09-20T10:05:00Z",
                    "updated_at": "2026-09-20T10:06:30Z"
                }
            }
        },
        # 4. UPDATE: Inventory Reserved Stock Incremented
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.inventory.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 35000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 35000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "inventory",
                    "txId": 1004,
                    "lsn": 24892500
                },
                "before": {
                    "sku": "SKU-TIRE-001",
                    "warehouse_id": "WH-FRANKFURT-1",
                    "available_stock": 1250,
                    "reserved_stock": 45
                },
                "after": {
                    "sku": "SKU-TIRE-001",
                    "warehouse_id": "WH-FRANKFURT-1",
                    "available_stock": 1170,
                    "reserved_stock": 125,
                    "last_restocked_at": "2026-09-20T10:00:00Z"
                }
            }
        },
        # 5. OUT-OF-ORDER EVENT: Stale order status arriving late (should be ignored by Silver merge)
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.orders.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 45000,  # Older timestamp than current Silver state (-40000)
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 45000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "orders",
                    "txId": 1002,
                    "lsn": 24891800
                },
                "before": None,
                "after": {
                    "order_id": "ORD-2026-9004",
                    "customer_id": "CUST-MERCEDES",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 80,
                    "total_price": 7600.00,
                    "status": "PROCESSING",
                    "created_at": "2026-09-20T10:05:00Z",
                    "updated_at": "2026-09-20T10:05:30Z"
                }
            }
        },
        # 6. DELETE: Order ORD-2026-9004 deleted after fulfillment
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.orders.Envelope"},
            "payload": {
                "op": "d",
                "ts_ms": now_ms - 10000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": topic_prefix,
                    "ts_ms": now_ms - 10000,
                    "db": "erp_database",
                    "schema": "public",
                    "table": "orders",
                    "txId": 1005,
                    "lsn": 24893000
                },
                "before": {
                    "order_id": "ORD-2026-9004",
                    "customer_id": "CUST-MERCEDES",
                    "product_id": "SKU-TIRE-001",
                    "quantity": 80,
                    "total_price": 7600.00,
                    "status": "CONFIRMED"
                },
                "after": None
            }
        },
        # 7. POISON PILL: Negative price and invalid status (Schema contract violation -> DLQ)
        {
            "schema": {"type": "struct", "name": f"{topic_prefix}.public.orders.Envelope"},
            "payload": {
                "op": "c",
                "ts_ms": now_ms - 5000,
                "source": {"table": "orders"},
                "before": None,
                "after": {
                    "order_id": "ORD-POISON-001",
                    "customer_id": "CUST-CORRUPT",
                    "product_id": "SKU-TIRE-001",
                    "quantity": -5,
                    "total_price": -100.00,
                    "status": "INVALID_STATE_XYZ"
                }
            }
        }
    ]

    return events


def run_database_simulation(
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
    cycles: int = 5,
    delay: float = 1.0
):
    """
    Connects to real PostgreSQL and executes genuine transactional operations.
    PostgreSQL logical decoding emits WAL events for Debezium to stream.
    """
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 is required for database mode. Run 'pip install psycopg2-binary'.")
        return False

    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{dbname} as {user}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=5
        )
        conn.autocommit = False
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return False

    customers = ["CUST-BMW-GROUP", "CUST-MERCEDES", "CUST-VOLVO", "CUST-PORSCHE", "CUST-AUDI"]
    products = [
        ("SKU-TIRE-001", 95.00),
        ("SKU-BELT-002", 18.50),
        ("SKU-HOSE-003", 31.00),
        ("SKU-BRAKE-004", 120.00),
    ]

    logger.info(f"Starting real database transaction simulation: {cycles} cycles...")

    try:
        with conn.cursor() as cur:
            for cycle in range(1, cycles + 1):
                order_id = f"ORD-{int(time.time())}-{random.randint(1000, 9999)}"
                customer = random.choice(customers)
                sku, unit_price = random.choice(products)
                qty = random.randint(5, 50)
                total = round(qty * unit_price, 2)

                # 1. INSERT new order
                logger.info(f"[Cycle {cycle}/{cycles}] INSERT order {order_id} ({qty}x {sku} = {total} EUR)")
                cur.execute(
                    """
                    INSERT INTO orders (order_id, customer_id, product_id, quantity, total_price, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, 'PENDING', NOW(), NOW());
                    """,
                    (order_id, customer, sku, qty, total)
                )

                # 2. UPDATE inventory: reserve stock
                cur.execute(
                    """
                    UPDATE inventory
                    SET reserved_stock = reserved_stock + %s,
                        available_stock = GREATEST(0, available_stock - %s)
                    WHERE sku = %s;
                    """,
                    (qty, qty, sku)
                )
                conn.commit()
                time.sleep(delay)

                # 3. UPDATE order status: PENDING -> CONFIRMED
                logger.info(f"[Cycle {cycle}/{cycles}] UPDATE order {order_id} -> CONFIRMED")
                cur.execute(
                    """
                    UPDATE orders
                    SET status = 'CONFIRMED', updated_at = NOW()
                    WHERE order_id = %s;
                    """,
                    (order_id,)
                )
                conn.commit()
                time.sleep(delay)

                # 4. UPDATE order status: CONFIRMED -> SHIPPED
                logger.info(f"[Cycle {cycle}/{cycles}] UPDATE order {order_id} -> SHIPPED")
                cur.execute(
                    """
                    UPDATE orders
                    SET status = 'SHIPPED', updated_at = NOW()
                    WHERE order_id = %s;
                    """,
                    (order_id,)
                )
                conn.commit()
                time.sleep(delay)

                # Occasional DELETE to test CDC delete propagation
                if cycle % 3 == 0:
                    logger.info(f"[Cycle {cycle}/{cycles}] DELETE order {order_id} (CDC delete test)")
                    cur.execute("DELETE FROM orders WHERE order_id = %s;", (order_id,))
                    conn.commit()
                    time.sleep(delay)

        logger.info("Successfully generated transactions in PostgreSQL.")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during transaction execution: {e}")
        return False
    finally:
        conn.close()


def run_stream_simulation(topic_prefix: str = "erp_cdc", storage_dir: str = "./lakehouse_storage"):
    """
    Executes offline mock stream replay directly against CDCPipelineConsumer.
    Validates end-to-end Medallion lakehouse routing, DLQ quarantine, and Delta merges.
    """
    from pipeline.delta_engine import DeltaLakehouseEngine
    from pipeline.consumer import CDCPipelineConsumer
    from analytics.gold_analytics import GoldLakehouseAnalytics

    logger.info(f"=== Running Offline Stream Simulation (Storage: {storage_dir}) ===")
    engine = DeltaLakehouseEngine(base_storage_dir=storage_dir)
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)
    events = generate_sample_cdc_stream(topic_prefix=topic_prefix)

    for idx, ev in enumerate(events, 1):
        raw_msg = json.dumps(ev)
        table = ev.get("payload", {}).get("source", {}).get("table", "orders")
        topic = f"{topic_prefix}.public.{table}"
        res = consumer.process_raw_message(raw_msg, topic_hint=topic)
        logger.info(f"[{idx}/{len(events)}] Status: {res['status']} | Topic: {topic} | Details: {res.get('merge_result', res.get('reason'))}")

    # Inject deliberate malformed JSON to test DLQ isolation
    poison_raw = "{'broken_json': true, unquoted_val: 123"
    res_dlq = consumer.process_raw_message(poison_raw, topic_hint=f"{topic_prefix}.public.orders")
    logger.info(f"[Poison Pill DLQ Test] Status: {res_dlq['status']} | Reason: {res_dlq.get('reason')}")

    logger.info("\n--- Consumer Metrics Summary ---")
    for k, v in consumer.metrics.items():
        logger.info(f"  {k}: {v}")

    # Run Gold OLAP verification
    logger.info("\n--- Gold Layer Analytics Verification ---")
    analytics = GoldLakehouseAnalytics(lakehouse_dir=storage_dir)
    rev_df = analytics.get_order_revenue_summary()
    logger.info(f"Revenue Summary:\n{rev_df}")
    stock_df = analytics.get_low_stock_alerts()
    logger.info(f"Low Stock Alerts:\n{stock_df}")


def main():
    parser = argparse.ArgumentParser(description="ERP Transaction & CDC Stream Simulator")
    parser.add_argument(
        "--mode",
        choices=["database", "stream", "both"],
        default="database",
        help="Operation mode: 'database' (real SQL), 'stream' (offline replay), or 'both'"
    )
    parser.add_argument("--database-mode", action="store_true", help="Shortcut for --mode database")
    parser.add_argument("--stream-mode", action="store_true", help="Shortcut for --mode stream")
    parser.add_argument("--unit-test-mode", action="store_true", help="Shortcut for --mode stream")

    # DB Connection params
    parser.add_argument("--host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--user", default=os.getenv("POSTGRES_USER", "erp_admin"), help="PostgreSQL user")
    parser.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "erp_secure_password123"), help="PostgreSQL password")
    parser.add_argument("--dbname", default=os.getenv("POSTGRES_DB", "erp_database"), help="PostgreSQL database")

    parser.add_argument("--cycles", type=int, default=5, help="Number of transaction cycles")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between operations in seconds")
    parser.add_argument("--topic-prefix", default=os.getenv("KAFKA_TOPIC_PREFIX", "erp_cdc"), help="Kafka topic prefix")
    parser.add_argument("--storage-dir", default=os.getenv("LAKEHOUSE_STORAGE_DIR", "./lakehouse_storage"), help="Lakehouse storage dir")

    args = parser.parse_args()

    mode = args.mode
    if args.stream_mode or args.unit_test_mode:
        mode = "stream"
    elif args.database_mode:
        mode = "database"

    if mode in ("database", "both"):
        success = run_database_simulation(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.dbname,
            cycles=args.cycles,
            delay=args.delay
        )
        if not success and mode == "database":
            logger.warning("Database connection failed. Falling back to offline stream mode for local validation.")
            run_stream_simulation(topic_prefix=args.topic_prefix, storage_dir=args.storage_dir)

    if mode == "stream":
        run_stream_simulation(topic_prefix=args.topic_prefix, storage_dir=args.storage_dir)


if __name__ == "__main__":
    main()
