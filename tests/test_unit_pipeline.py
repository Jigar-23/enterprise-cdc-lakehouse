"""
Unit test suite for Enterprise CDC & Delta Lakehouse pipeline.
Tests:
- Strict Pydantic domain contracts (extra=forbid, value limits, enums)
- Debezium envelope structure and source metadata validation
- Topic/source entity conflict rejection (no silent reinterpretation)
- Forensic Bronze raw changelog preservation (all 15 attributes)
- Monotonic LSN & timestamp ordering matrix:
    * newer ts + newer LSN -> APPLIED
    * older ts + older LSN -> REJECTED
    * same ts + higher LSN -> APPLIED
    * same ts + lower LSN -> REJECTED
    * newer ts + LOWER LSN -> REJECTED (LSN physical WAL precedence)
    * older ts + HIGHER LSN -> APPLIED (LSN physical WAL precedence)
    * duplicate event -> IDEMPOTENT
- Tombstone delete lifecycle:
    * insert -> update -> delete
    * stale update after delete -> REJECTED
    * new insert / resurrection -> APPLIED
    * stale delete after new insert -> REJECTED
    * duplicate delete -> IDEMPOTENT
- DLQ quarantine isolation with independent assertions on all metadata fields
- Discrete failure semantics:
    * Validation failure -> Quarantined
    * Storage failure -> Unhandled / No offset commit
- Gold Layer DuckDB analytical OLAP views
"""

import os
import json
import shutil
import pytest
import pandas as pd
from pydantic import ValidationError
from deltalake import DeltaTable

from pipeline.models import OrderPayload, InventoryPayload, CDCEnvelope, SourceInfo, QuarantineRecord
from pipeline.delta_engine import DeltaLakehouseEngine
from pipeline.consumer import CDCPipelineConsumer
from analytics.gold_analytics import GoldLakehouseAnalytics


@pytest.fixture
def temp_lakehouse(tmp_path):
    """Provides an isolated clean lakehouse directory for each test."""
    storage_path = str(tmp_path / "lakehouse_adversarial_test")
    engine = DeltaLakehouseEngine(base_storage_dir=storage_path)
    yield engine, storage_path
    if os.path.exists(storage_path):
        shutil.rmtree(storage_path, ignore_errors=True)


# =====================================================================
# 1. Strict Pydantic Data Contract Tests
# =====================================================================

def test_order_model_valid():
    order = OrderPayload(
        order_id="ORD-001",
        customer_id="CUST-BMW",
        product_id="SKU-TIRE-001",
        quantity=5,
        total_price=475.00,
        status="CONFIRMED"
    )
    assert order.order_id == "ORD-001"
    assert order.quantity == 5
    assert order.total_price == 475.00
    assert order.status == "CONFIRMED"


def test_order_model_forbids_extra_fields():
    """Extra fields must be strictly rejected (extra='forbid')."""
    payload = {
        "order_id": "ORD-001",
        "customer_id": "CUST-BMW",
        "product_id": "SKU-TIRE-001",
        "quantity": 5,
        "total_price": 475.00,
        "status": "CONFIRMED",
        "arbitrary_unexpected_field": "malicious_injection"
    }
    with pytest.raises(ValidationError) as exc:
        OrderPayload(**payload)
    assert "extra_forbidden" in str(exc.value)


@pytest.mark.parametrize("invalid_qty", [0, -1, -50])
def test_order_model_invalid_quantity(invalid_qty):
    with pytest.raises(ValidationError):
        OrderPayload(
            order_id="ORD-002",
            customer_id="CUST-BMW",
            product_id="SKU-TIRE-001",
            quantity=invalid_qty,
            total_price=100.0,
            status="PENDING"
        )


@pytest.mark.parametrize("invalid_price", [-0.01, -100.0])
def test_order_model_negative_price(invalid_price):
    with pytest.raises(ValidationError):
        OrderPayload(
            order_id="ORD-003",
            customer_id="CUST-BMW",
            product_id="SKU-TIRE-001",
            quantity=1,
            total_price=invalid_price,
            status="CONFIRMED"
        )


def test_order_model_invalid_status():
    with pytest.raises(ValidationError):
        OrderPayload(
            order_id="ORD-004",
            customer_id="CUST-BMW",
            product_id="SKU-TIRE-001",
            quantity=1,
            total_price=10.0,
            status="UNKNOWN_STATUS_XYZ"
        )


def test_inventory_model_forbids_extra_fields():
    with pytest.raises(ValidationError):
        InventoryPayload(
            sku="SKU-1",
            warehouse_id="WH-1",
            available_stock=100,
            reserved_stock=10,
            rogue_column="bad"
        )


def test_source_info_lsn_numeric():
    s1 = SourceInfo(lsn=24891000)
    assert s1.lsn_numeric == 24891000

    # PostgreSQL WAL hex string e.g. '0/16B2D40'
    s2 = SourceInfo(lsn="0/16B2D40")
    expected = (0 << 32) + int("16B2D40", 16)
    assert s2.lsn_numeric == expected

    # None LSN returns 0
    s3 = SourceInfo(lsn=None)
    assert s3.lsn_numeric == 0


def test_source_info_malformed_lsn():
    with pytest.raises(ValueError):
        s = SourceInfo(lsn="NOT_A_VALID_LSN_HEX")
        _ = s.lsn_numeric


def test_cdc_envelope_requires_source():
    """CDC Envelope without source block must be rejected."""
    with pytest.raises(ValidationError):
        CDCEnvelope(
            op="c",
            ts_ms=1000,
            source=None,
            after={"order_id": "O1", "quantity": 1}
        )


def test_cdc_envelope_invalid_operation():
    with pytest.raises(ValidationError):
        CDCEnvelope(
            op="INVALID_OP",  # Only 'r', 'c', 'u', 'd' allowed
            ts_ms=1000,
            source=SourceInfo(table="orders"),
            after={"order_id": "O1"}
        )


def test_cdc_envelope_invalid_source_table():
    with pytest.raises(ValidationError):
        CDCEnvelope(
            op="c",
            ts_ms=1000,
            source=SourceInfo(table="malicious_table_injection"),
            after={"order_id": "O1"}
        )


# =====================================================================
# 2. Entity Routing & Topic-Source Conflict Tests
# =====================================================================

def test_consumer_topic_source_mismatch_quarantined(temp_lakehouse):
    """An inventory payload sent to an orders topic must be quarantined, NOT reinterpreted."""
    engine, _ = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1726830000000,
            "source": {"table": "inventory", "lsn": 100},
            "after": {
                "sku": "SKU-TIRE-001",
                "warehouse_id": "WH-1",
                "available_stock": 100,
                "reserved_stock": 10
            }
        }
    })
    res = consumer.process_raw_message(msg, topic_hint="erp_cdc.public.orders", partition=0, offset=5)
    assert res["status"] == "quarantined"
    assert res["reason"] == "TOPIC_SOURCE_MISMATCH"


# =====================================================================
# 3. Forensic Bronze Context Preservation Tests
# =====================================================================

def test_bronze_preserves_full_raw_cdc_context(temp_lakehouse):
    """Bronze must retain event_id, topic, partition, offset, key, op, before, after, source, transaction, LSN."""
    engine, storage_path = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1726830000000,
            "source": {"version": "2.5.0", "table": "orders", "txId": 501, "lsn": 24891000},
            "before": None,
            "after": {
                "order_id": "ORD-FORENSIC-01",
                "customer_id": "CUST-AUDI",
                "product_id": "SKU-TIRE-001",
                "quantity": 10,
                "total_price": 950.00,
                "status": "CONFIRMED"
            },
            "transaction": {"id": "tx-123"}
        }
    })

    res = consumer.process_raw_message(msg, topic_hint="erp_cdc.public.orders", partition=2, offset=88)
    assert res["status"] == "success"

    bronze_dir = os.path.join(storage_path, "bronze", "orders")
    assert DeltaTable.is_deltatable(bronze_dir)
    b_df = engine.get_bronze_table("orders")
    assert len(b_df) == 1

    row = b_df.iloc[0]
    assert row["event_id"] == "erp_cdc.public.orders:2:88"
    assert row["source_topic"] == "erp_cdc.public.orders"
    assert row["partition"] == 2
    assert row["offset"] == 88
    assert row["key"] == "ORD-FORENSIC-01"
    assert row["op"] == "c"
    assert row["before"] is None
    assert "ORD-FORENSIC-01" in row["after"]
    assert "24891000" in row["source"]
    assert "tx-123" in row["transaction"]
    assert row["cdc_lsn"] == 24891000
    assert row["tx_id"] == 501
    assert "ingested_at" in b_df.columns
    assert "_partition_date" in b_df.columns


def test_bronze_is_duplicate_tolerant(temp_lakehouse):
    """Kafka re-delivery (at-least-once) appends duplicate records into Bronze without error."""
    engine, _ = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1000,
            "source": {"table": "orders", "lsn": 100},
            "after": {"order_id": "ORD-DUP", "customer_id": "C1", "product_id": "P1", "quantity": 1, "total_price": 10.0, "status": "PENDING"}
        }
    })
    consumer.process_raw_message(msg, topic_hint="erp_cdc.public.orders", partition=0, offset=10)
    consumer.process_raw_message(msg, topic_hint="erp_cdc.public.orders", partition=0, offset=10)

    # Bronze has both raw records
    b_df = engine.get_bronze_table("orders")
    assert len(b_df) == 2

    # Silver deduplicates to exactly 1 row
    s_df = engine.get_silver_table("orders")
    assert len(s_df) == 1


# =====================================================================
# 4. Monotonic LSN & Timestamp Ordering Matrix (All 9 Cases)
# =====================================================================

def test_ordering_case_1_newer_ts_newer_lsn(temp_lakehouse):
    """Case 1: Newer timestamp + newer LSN -> APPLIED."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "PENDING", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "c"}], primary_key="order_id")
    engine.merge_silver("orders", [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 2000, "_lsn": 200, "_cdc_op": "u"}], primary_key="order_id")
    assert engine.get_silver_table("orders").iloc[0]["status"] == "CONFIRMED"


def test_ordering_case_2_older_ts_older_lsn(temp_lakehouse):
    """Case 2: Older timestamp + older LSN -> REJECTED."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 2000, "_lsn": 200, "_cdc_op": "c"}], primary_key="order_id")
    stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "PENDING", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "u"}], primary_key="order_id")
    assert stats["ignored"] == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "CONFIRMED"


def test_ordering_case_3_same_ts_higher_lsn(temp_lakehouse):
    """Case 3: Same timestamp + higher LSN -> APPLIED."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "STEP_A", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "c"}], primary_key="order_id")
    engine.merge_silver("orders", [{"order_id": "O1", "status": "STEP_B", "ts_ms": 1000, "_lsn": 200, "_cdc_op": "u"}], primary_key="order_id")
    assert engine.get_silver_table("orders").iloc[0]["status"] == "STEP_B"


def test_ordering_case_4_same_ts_lower_lsn(temp_lakehouse):
    """Case 4: Same timestamp + lower LSN -> REJECTED."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "STEP_B", "ts_ms": 1000, "_lsn": 200, "_cdc_op": "c"}], primary_key="order_id")
    stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "STEP_A", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "u"}], primary_key="order_id")
    assert stats["ignored"] == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "STEP_B"


def test_ordering_case_5_newer_ts_lower_lsn(temp_lakehouse):
    """Case 5: Newer timestamp + lower LSN -> REJECTED (Physical WAL LSN takes precedence)."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "COMMITTED_LATER", "ts_ms": 1000, "_lsn": 500, "_cdc_op": "c"}], primary_key="order_id")
    stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "CLOCK_SKEWED_STALE", "ts_ms": 5000, "_lsn": 400, "_cdc_op": "u"}], primary_key="order_id")
    assert stats["ignored"] == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "COMMITTED_LATER"


def test_ordering_case_6_older_ts_higher_lsn(temp_lakehouse):
    """Case 6: Older timestamp + higher LSN -> APPLIED (Physical WAL LSN takes precedence)."""
    engine, _ = temp_lakehouse
    engine.merge_silver("orders", [{"order_id": "O1", "status": "EARLIER_WAL", "ts_ms": 5000, "_lsn": 100, "_cdc_op": "c"}], primary_key="order_id")
    stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "LATER_WAL", "ts_ms": 2000, "_lsn": 200, "_cdc_op": "u"}], primary_key="order_id")
    assert stats["updated"] == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "LATER_WAL"


def test_ordering_case_7_duplicate_event_idempotency(temp_lakehouse):
    """Case 7: Duplicate event -> IDEMPOTENT."""
    engine, _ = temp_lakehouse
    rec = [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "c"}]
    engine.merge_silver("orders", rec, primary_key="order_id")
    engine.merge_silver("orders", rec, primary_key="order_id")
    assert len(engine.get_silver_table("orders")) == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "CONFIRMED"


# =====================================================================
# 5. Tombstone Delete Semantics (Cases 8 & 9 and Lifecycle)
# =====================================================================

def test_delete_lifecycle_and_stale_rejection(temp_lakehouse):
    """
    Validates complete delete lifecycle:
    1. Insert at LSN 100
    2. Update at LSN 200
    3. Delete at LSN 300
    4. Stale update at LSN 250 (Case 8: Delete followed by stale update -> REJECTED)
    5. Duplicate delete at LSN 300 -> IDEMPOTENT
    6. Resurrection / New insert at LSN 400 -> APPLIED
    7. Stale delete at LSN 350 (Case 9: Update followed by stale delete -> REJECTED)
    """
    engine, _ = temp_lakehouse

    # 1. Insert
    engine.merge_silver("orders", [{"order_id": "O1", "status": "PENDING", "ts_ms": 1000, "_lsn": 100, "_cdc_op": "c"}], primary_key="order_id")
    assert len(engine.get_silver_table("orders")) == 1

    # 2. Update
    engine.merge_silver("orders", [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 2000, "_lsn": 200, "_cdc_op": "u"}], primary_key="order_id")
    assert engine.get_silver_table("orders").iloc[0]["status"] == "CONFIRMED"

    # 3. Delete at LSN 300
    del_stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 3000, "_lsn": 300, "_cdc_op": "d"}], primary_key="order_id")
    assert del_stats["deleted"] == 1
    # Active rows = 0
    assert len(engine.get_silver_table("orders", include_deleted=False)) == 0
    # Tombstone preserved
    tombstone_df = engine.get_silver_table("orders", include_deleted=True)
    assert len(tombstone_df) == 1
    assert tombstone_df.iloc[0]["_is_deleted"] == True

    # 4. Case 8: Stale update at LSN 250 (after delete at LSN 300) -> REJECTED
    stale_stats = engine.merge_silver("orders", [{"order_id": "O1", "status": "STALE_UPDATE", "ts_ms": 2500, "_lsn": 250, "_cdc_op": "u"}], primary_key="order_id")
    assert stale_stats["ignored"] == 1
    assert len(engine.get_silver_table("orders", include_deleted=False)) == 0

    # 5. Duplicate delete at LSN 300 -> IDEMPOTENT
    dup_del = engine.merge_silver("orders", [{"order_id": "O1", "status": "CONFIRMED", "ts_ms": 3000, "_lsn": 300, "_cdc_op": "d"}], primary_key="order_id")
    assert len(engine.get_silver_table("orders", include_deleted=False)) == 0

    # 6. Re-insert / Resurrection at LSN 400 -> APPLIED
    re_insert = engine.merge_silver("orders", [{"order_id": "O1", "status": "RE_ORDERED", "ts_ms": 4000, "_lsn": 400, "_cdc_op": "c"}], primary_key="order_id")
    assert re_insert["updated"] == 1 or re_insert["inserted"] == 1
    assert len(engine.get_silver_table("orders", include_deleted=False)) == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "RE_ORDERED"

    # 7. Case 9: Stale delete at LSN 350 (after re-insert at LSN 400) -> REJECTED
    stale_del = engine.merge_silver("orders", [{"order_id": "O1", "status": "RE_ORDERED", "ts_ms": 3500, "_lsn": 350, "_cdc_op": "d"}], primary_key="order_id")
    assert stale_del["ignored"] == 1
    assert len(engine.get_silver_table("orders", include_deleted=False)) == 1
    assert engine.get_silver_table("orders").iloc[0]["status"] == "RE_ORDERED"


# =====================================================================
# 6. DLQ Quarantine Metadata Verification Tests
# =====================================================================

def test_dlq_metadata_strict_assertions(temp_lakehouse):
    """Asserts each DLQ quarantine field independently (no weak 'or' assertions)."""
    engine, storage_path = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    raw_corrupt = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1000,
            "source": {"table": "orders", "lsn": 50},
            "after": {
                "order_id": "ORD-POISON-42",
                "customer_id": "CUST-BAD",
                "product_id": "SKU-1",
                "quantity": -5,  # Contract violation
                "total_price": -50.0,
                "status": "CONFIRMED"
            }
        }
    })

    res = consumer.process_raw_message(raw_corrupt, topic_hint="erp_cdc.public.orders", partition=1, offset=77)
    assert res["status"] == "quarantined"
    assert res["reason"] == "SCHEMA_CONTRACT_VIOLATION"

    dlq_dir = os.path.join(storage_path, "quarantine")
    dlq_files = [f for f in os.listdir(dlq_dir) if f.endswith(".jsonl")]
    assert len(dlq_files) == 1

    with open(os.path.join(dlq_dir, dlq_files[0])) as f:
        record = json.loads(f.readline())

    # Assert every metadata field independently
    assert record["event_id"] == "erp_cdc.public.orders:1:77"
    assert record["topic"] == "erp_cdc.public.orders"
    assert record["partition"] == 1
    assert record["offset"] == 77
    assert record["key"] == "ORD-POISON-42"
    assert record["entity"] == "orders"
    assert record["source_table"] == "orders"
    assert record["operation"] == "c"
    assert record["error_type"] == "SCHEMA_CONTRACT_VIOLATION"
    assert "greater_than" in record["error_message"]
    assert record["stack_trace"] is not None
    assert record["quarantined_at"] is not None


# =====================================================================
# 7. Discrete Failure Semantics Tests
# =====================================================================

def test_storage_failure_raises_and_blocks_commit(temp_lakehouse):
    """Storage failure during Delta write must raise and NOT commit offset."""
    engine, _ = temp_lakehouse
    consumer = CDCPipelineConsumer(lakehouse_engine=engine)

    # Monkeypatch append_bronze to simulate unhandled I/O / disk failure
    def broken_append(*args, **kwargs):
        raise IOError("Disk quota exceeded / write failure")

    engine.append_bronze = broken_append

    msg = json.dumps({
        "payload": {
            "op": "c",
            "ts_ms": 1000,
            "source": {"table": "orders", "lsn": 100},
            "after": {"order_id": "ORD-1", "customer_id": "C1", "product_id": "P1", "quantity": 10, "total_price": 100.0, "status": "PENDING"}
        }
    })

    # Storage failure must raise to trigger process restart/retry without committing offset
    with pytest.raises(IOError):
        consumer.process_raw_message(msg, topic_hint="erp_cdc.public.orders")


# =====================================================================
# 8. Gold Layer DuckDB Analytics Tests
# =====================================================================

def test_gold_analytics_olap_queries(temp_lakehouse):
    engine, storage_path = temp_lakehouse

    # Populate Silver orders
    engine.merge_silver("orders", [
        {"order_id": "O1", "customer_id": "CUST-BMW", "product_id": "P1", "quantity": 10, "total_price": 1000.0, "status": "CONFIRMED", "ts_ms": 1000, "_lsn": 10},
        {"order_id": "O2", "customer_id": "CUST-BMW", "product_id": "P2", "quantity": 5, "total_price": 500.0, "status": "CONFIRMED", "ts_ms": 1000, "_lsn": 20},
        {"order_id": "O3", "customer_id": "CUST-MERCEDES", "product_id": "P1", "quantity": 2, "total_price": 250.0, "status": "SHIPPED", "ts_ms": 1000, "_lsn": 30}
    ], primary_key="order_id")

    # Populate Silver inventory
    engine.merge_silver("inventory", [
        {"sku": "SKU-CRITICAL", "warehouse_id": "WH-1", "available_stock": 200, "reserved_stock": 100, "last_restocked_at": "2026-09-20", "ts_ms": 1000, "_lsn": 10},
        {"sku": "SKU-SAFE", "warehouse_id": "WH-1", "available_stock": 2000, "reserved_stock": 50, "last_restocked_at": "2026-09-20", "ts_ms": 1000, "_lsn": 20}
    ], primary_key="sku")

    analytics = GoldLakehouseAnalytics(lakehouse_dir=storage_path)

    # 1. Revenue
    rev_df = analytics.get_order_revenue_summary()
    assert len(rev_df) == 2
    bmw = rev_df[rev_df["customer_id"] == "CUST-BMW"].iloc[0]
    assert bmw["total_revenue_eur"] == 1500.0
    assert bmw["total_orders"] == 2

    # 2. Stock Alerts
    alert_df = analytics.get_low_stock_alerts(threshold=500)
    assert len(alert_df) == 1
    assert alert_df.iloc[0]["sku"] == "SKU-CRITICAL"
    assert alert_df.iloc[0]["stock_status"] == "CRITICAL_REORDER"
