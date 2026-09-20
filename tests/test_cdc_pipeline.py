"""
Comprehensive Test Suite for Enterprise CDC & Delta Lakehouse Platform.
Validates:
- Pydantic schema validation & contracts
- Debezium CDC envelope parsing
- Bronze append-only changelog
- Silver ACID MERGE INTO upsert & out-of-order resolution
- Soft and hard delete semantics
- Dead Letter Queue (DLQ) poison pill quarantine
- Gold layer DuckDB OLAP queries
"""

import os
import json
import shutil
import pytest
import pandas as pd
from pydantic import ValidationError

from pipeline.models import OrderPayload, InventoryPayload, CDCEnvelope
from pipeline.delta_engine import DeltaLakehouseEngine
from pipeline.consumer import CDCPipelineConsumer
from analytics.gold_analytics import GoldLakehouseAnalytics


@pytest.fixture
def temp_lakehouse(tmp_path):
    """Provides a fresh temporary lakehouse storage directory for each test."""
    storage_path = str(tmp_path / "lakehouse_test")
    engine = DeltaLakehouseEngine(base_storage_dir=storage_path)
    yield engine, storage_path
    if os.path.exists(storage_path):
        shutil.rmtree(storage_path, ignore_errors=True)


# =====================================================================
# 1. Schema Contract & Poison Pill Validation Tests
# =====================================================================

def test_order_schema_validation_success():
    payload = {
        "order_id": "ORD-100",
        "customer_id": "CUST-A",
        "product_id": "PROD-1",
        "quantity": 10,
        "total_price": 99.99,
        "status": "CONFIRMED"
    }
    order = OrderPayload(**payload)
    assert order.order_id == "ORD-100"
    assert order.total_price == 99.99
    assert order.status == "CONFIRMED"


def test_order_schema_validation_failure_negative_price():
    payload = {
        "order_id": "ORD-101",
        "customer_id": "CUST-A",
        "product_id": "PROD-1",
        "quantity": 10,
        "total_price": -50.00,  # Negative price
        "status": "CONFIRMED"
    }
    with pytest.raises(ValidationError):
        OrderPayload(**payload)


def test_order_schema_validation_failure_invalid_status():
    payload = {
        "order_id": "ORD-102",
        "customer_id": "CUST-A",
        "product_id": "PROD-1",
        "quantity": 10,
        "total_price": 50.00,
        "status": "NON_EXISTENT_STATE"
    }
    with pytest.raises(ValidationError):
        OrderPayload(**payload)


def test_cdc_envelope_validation():
    # Valid INSERT envelope
    valid_env = {
        "op": "c",
        "after": {"order_id": "ORD-1", "customer_id": "CUST-1", "product_id": "P1", "quantity": 1, "total_price": 10.0}
    }
    env = CDCEnvelope(**valid_env)
    assert env.op == "c"

    # Invalid INSERT without 'after'
    invalid_env = {"op": "c", "after": None}
    with pytest.raises(ValidationError):
        CDCEnvelope(**invalid_env)


# =====================================================================
# 2. Bronze Append & Silver ACID Upsert Tests
# =====================================================================

def test_bronze_append(temp_lakehouse):
    engine, storage_path = temp_lakehouse
    records = [
        {"order_id": "ORD-1", "quantity": 5, "total_price": 50.0},
        {"order_id": "ORD-2", "quantity": 10, "total_price": 100.0}
    ]
    count = engine.append_bronze("orders", records)
    assert count == 2

    bronze_dir = os.path.join(storage_path, "bronze", "orders")
    files = os.listdir(bronze_dir)
    assert len(files) == 1
    df = pd.read_parquet(os.path.join(bronze_dir, files[0]))
    assert len(df) == 2
    assert "_ingested_at" in df.columns


def test_silver_upsert_and_deduplication(temp_lakehouse):
    engine, _ = temp_lakehouse

    # 1. Initial Insert
    initial_records = [
        {"order_id": "ORD-1", "status": "PENDING", "ts_ms": 1000},
        {"order_id": "ORD-2", "status": "PENDING", "ts_ms": 1000}
    ]
    stats1 = engine.merge_silver("orders", initial_records, primary_key="order_id")
    assert stats1["inserted"] == 2

    df1 = engine.get_silver_table("orders")
    assert len(df1) == 2
    assert df1[df1["order_id"] == "ORD-1"]["status"].iloc[0] == "PENDING"

    # 2. Update ORD-1 to CONFIRMED with newer timestamp
    update_records = [
        {"order_id": "ORD-1", "status": "CONFIRMED", "ts_ms": 2000}
    ]
    stats2 = engine.merge_silver("orders", update_records, primary_key="order_id")
    assert stats2["updated"] == 1

    df2 = engine.get_silver_table("orders")
    assert len(df2) == 2
    assert df2[df2["order_id"] == "ORD-1"]["status"].iloc[0] == "CONFIRMED"


def test_silver_out_of_order_resolution(temp_lakehouse):
    """
    Guarantees deterministic state: an event with an older timestamp
    must not overwrite a newer state.
    """
    engine, _ = temp_lakehouse

    # Initial state at ts = 2000
    engine.merge_silver("orders", [{"order_id": "ORD-1", "status": "SHIPPED", "ts_ms": 2000}], primary_key="order_id")

    # Late-arriving stale event from network retry at ts = 1500
    stale_event = [{"order_id": "ORD-1", "status": "PENDING", "ts_ms": 1500}]
    stats = engine.merge_silver("orders", stale_event, primary_key="order_id")

    assert stats["ignored"] == 1
    assert stats["updated"] == 0

    df = engine.get_silver_table("orders")
    assert df[df["order_id"] == "ORD-1"]["status"].iloc[0] == "SHIPPED"


def test_silver_delete_handling(temp_lakehouse):
    engine, _ = temp_lakehouse

    engine.merge_silver("orders", [{"order_id": "ORD-1", "status": "ACTIVE", "ts_ms": 1000}], primary_key="order_id")
    assert len(engine.get_silver_table("orders")) == 1

    # Apply CDC DELETE operation
    delete_event = [{"order_id": "ORD-1", "status": "ACTIVE", "_cdc_op": "d", "ts_ms": 2000}]
    stats = engine.merge_silver("orders", delete_event, primary_key="order_id")
    assert stats["deleted"] == 1

    df = engine.get_silver_table("orders")
    assert len(df) == 0


# =====================================================================
# 3. Consumer End-to-End & DLQ Quarantine Tests
# =====================================================================

def test_consumer_end_to_end_and_dlq(temp_lakehouse):
    engine, storage_path = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    # 1. Valid order message
    valid_order_msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1726830000000,
            "source": {"table": "orders", "txId": 501, "lsn": 100200},
            "after": {
                "order_id": "ORD-VALID-01",
                "customer_id": "CUST-CONTINENTAL",
                "product_id": "SKU-SENSOR-1",
                "quantity": 100,
                "total_price": 5000.00,
                "status": "CONFIRMED"
            }
        }
    })
    res1 = consumer.process_raw_message(valid_order_msg, topic_hint="erp_pg.public.orders")
    assert res1["status"] == "success"

    # 2. Corrupt payload (negative price) -> DLQ
    corrupt_msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1726830005000,
            "source": {"table": "orders"},
            "after": {
                "order_id": "ORD-CORRUPT-02",
                "customer_id": "CUST-BAD",
                "product_id": "SKU-SENSOR-1",
                "quantity": 10,
                "total_price": -99.00,  # Fails contract
                "status": "CONFIRMED"
            }
        }
    })
    res2 = consumer.process_raw_message(corrupt_msg, topic_hint="erp_pg.public.orders")
    assert res2["status"] == "quarantined"
    assert res2["reason"] == "SCHEMA_CONTRACT_VIOLATION"

    # 3. Completely invalid JSON -> DLQ
    malformed_json = "NOT_A_JSON_STRING{{{"
    res3 = consumer.process_raw_message(malformed_json, topic_hint="erp_pg.public.orders")
    assert res3["status"] == "quarantined"
    assert res3["reason"] == "JSON_DECODE_ERROR"

    # Verify DLQ directory contains quarantined records
    quarantine_files = os.listdir(os.path.join(storage_path, "quarantine"))
    assert len(quarantine_files) > 0


# =====================================================================
# 4. DuckDB Gold Analytics Tests
# =====================================================================

def test_gold_analytics_queries(temp_lakehouse):
    engine, storage_path = temp_lakehouse

    # Populate Silver orders & inventory
    orders_data = [
        {"order_id": "ORD-1", "customer_id": "CUST-BMW", "product_id": "P1", "quantity": 10, "total_price": 1000.0, "status": "CONFIRMED", "ts_ms": 1000},
        {"order_id": "ORD-2", "customer_id": "CUST-BMW", "product_id": "P2", "quantity": 5, "total_price": 500.0, "status": "CONFIRMED", "ts_ms": 1000},
        {"order_id": "ORD-3", "customer_id": "CUST-VOLVO", "product_id": "P1", "quantity": 2, "total_price": 200.0, "status": "SHIPPED", "ts_ms": 1000}
    ]
    engine.merge_silver("orders", orders_data, primary_key="order_id")

    inventory_data = [
        {"sku": "SKU-TIRE-CRITICAL", "warehouse_id": "WH-1", "available_stock": 250, "reserved_stock": 100, "last_restocked_at": "2026-09-20T00:00:00Z", "ts_ms": 1000},
        {"sku": "SKU-TIRE-PLENTY", "warehouse_id": "WH-1", "available_stock": 5000, "reserved_stock": 50, "last_restocked_at": "2026-09-20T00:00:00Z", "ts_ms": 1000}
    ]
    engine.merge_silver("inventory", inventory_data, primary_key="sku")

    analytics = GoldLakehouseAnalytics(lakehouse_dir=storage_path)

    # Test revenue summary
    rev_df = analytics.get_order_revenue_summary()
    assert len(rev_df) == 2
    bmw_row = rev_df[rev_df["customer_id"] == "CUST-BMW"].iloc[0]
    assert bmw_row["total_revenue_eur"] == 1500.0
    assert bmw_row["total_orders"] == 2

    # Test low stock alerts
    stock_df = analytics.get_low_stock_alerts(threshold=500)
    assert len(stock_df) == 1
    assert stock_df.iloc[0]["sku"] == "SKU-TIRE-CRITICAL"
    assert stock_df.iloc[0]["stock_status"] == "CRITICAL_REORDER"
