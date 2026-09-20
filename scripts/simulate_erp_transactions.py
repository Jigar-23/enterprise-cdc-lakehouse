"""
ERP Transaction & CDC Stream Simulator:
Generates high-fidelity Debezium PostgreSQL CDC events to simulate transactional
workloads, schema changes, and poison-pill anomalies for end-to-end lakehouse validation.
"""

import os
import sys
import json
import time
from datetime import datetime
from typing import List, Dict, Any

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def generate_sample_cdc_stream() -> List[Dict[str, Any]]:
    """
    Constructs a deterministic sequence of Debezium CDC events representing
    a realistic enterprise order-to-delivery lifecycle.
    """
    now_ms = int(time.time() * 1000)

    events = [
        # 1. Snapshot Read: Initial Inventory State
        {
            "schema": {"type": "struct", "name": "erp_pg.public.inventory.Envelope"},
            "payload": {
                "op": "r",
                "ts_ms": now_ms - 60000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_pg",
                    "ts_ms": now_ms - 60000,
                    "db": "enterprise_erp",
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
            "schema": {"type": "struct", "name": "erp_pg.public.orders.Envelope"},
            "payload": {
                "op": "c",
                "ts_ms": now_ms - 50000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_pg",
                    "ts_ms": now_ms - 50000,
                    "db": "enterprise_erp",
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
            "schema": {"type": "struct", "name": "erp_pg.public.orders.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 40000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_pg",
                    "ts_ms": now_ms - 40000,
                    "db": "enterprise_erp",
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
            "schema": {"type": "struct", "name": "erp_pg.public.inventory.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 35000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_pg",
                    "ts_ms": now_ms - 35000,
                    "db": "enterprise_erp",
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
            "schema": {"type": "struct", "name": "erp_pg.public.orders.Envelope"},
            "payload": {
                "op": "u",
                "ts_ms": now_ms - 45000,  # Older timestamp than current Silver state (-40000)
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "postgresql",
                    "name": "erp_pg",
                    "ts_ms": now_ms - 45000,
                    "db": "enterprise_erp",
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
        # 6. POISON PILL 1: Negative price and invalid status (Schema contract violation -> DLQ)
        {
            "schema": {"type": "struct", "name": "erp_pg.public.orders.Envelope"},
            "payload": {
                "op": "c",
                "ts_ms": now_ms - 20000,
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
        },
        # 7. POISON PILL 2: Completely broken JSON string (Handled in test runner)
    ]

    return events


if __name__ == "__main__":
    from pipeline.consumer import CDCPipelineConsumer

    print("=== Simulating CDC Stream to Local Lakehouse ===")
    consumer = CDCPipelineConsumer()
    events = generate_sample_cdc_stream()

    for idx, ev in enumerate(events, 1):
        raw_msg = json.dumps(ev)
        table = ev.get("payload", {}).get("source", {}).get("table", "orders")
        res = consumer.process_raw_message(raw_msg, topic_hint=f"erp_pg.public.{table}")
        print(f"[{idx}/{len(events)}] Status: {res['status']} | Details: {res}")

    # Inject deliberate malformed JSON to test DLQ
    poison_raw = "{'broken_json': true, unquoted_val: 123"
    res_dlq = consumer.process_raw_message(poison_raw, topic_hint="erp_pg.public.orders")
    print(f"\n[Poison Pill DLQ Test] Status: {res_dlq['status']} | Reason: {res_dlq.get('reason')}")

    print("\n--- Processing Metrics Summary ---")
    for k, v in consumer.metrics.items():
        print(f"  {k}: {v}")
