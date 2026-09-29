"""
Delta Engine: Implements Medallion Architecture (Bronze -> Silver -> DLQ)
Powered by native Delta Lake (Rust-backed deltalake library) with ACID logs (_delta_log).
Features:
- Bronze: Append-only raw event changelog preserving complete Debezium context (forensic replay).
- Silver: Curated tables with ACID MERGE INTO semantics, monotonic (LSN, ts_ms) ordering,
  and tombstone deletion semantics.
- DLQ: Structured quarantine with comprehensive message and partition metadata.
"""

import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("delta_engine")


class DeltaLakehouseEngine:
    """
    Delta Lake storage and transaction layer for CDC streaming.
    - Bronze: Append-only raw event changelog partitioned by date, duplicate-tolerant.
    - Silver: Curated tables with ACID MERGE INTO, monotonic ordering, and tombstone deletes.
    - DLQ: Isolated storage for poison pills and contract violations.
    """

    def __init__(self, base_storage_dir: str = "./lakehouse_storage"):
        self.base_dir = os.path.abspath(base_storage_dir)
        self.bronze_dir = os.path.join(self.base_dir, "bronze")
        self.silver_dir = os.path.join(self.base_dir, "silver")
        self.dlq_dir = os.path.join(self.base_dir, "quarantine")

        for d in [self.bronze_dir, self.silver_dir, self.dlq_dir]:
            os.makedirs(d, exist_ok=True)

    # -------------------------------------------------------------
    # Bronze Layer: Raw Changelog Ingestion (Append-Only Delta Table)
    # -------------------------------------------------------------
    def append_bronze(self, entity_name: str, records: List[Dict[str, Any]]) -> int:
        """
        Appends raw CDC records into the Bronze Delta table partitioned by ingestion date.
        Preserves complete raw context (event_id, topic, partition, offset, key, op,
        before, after, source, transaction, ts_ms, cdc_lsn, tx_id, ingested_at).
        Bronze is append-only and duplicate-tolerant.
        """
        if not records:
            return 0

        target_dir = os.path.join(self.bronze_dir, entity_name)
        os.makedirs(target_dir, exist_ok=True)

        now = datetime.now(timezone.utc)
        partition_date = now.strftime("%Y-%m-%d")

        formatted_records = []
        for r in records:
            item = dict(r)
            now_iso = now.isoformat()
            item.setdefault("ingested_at", now_iso)
            item["_ingested_at"] = item["ingested_at"]
            item.setdefault("_partition_date", partition_date)
            # Ensure numeric types
            item["ts_ms"] = int(item.get("ts_ms") or 0)
            item["cdc_lsn"] = int(item.get("cdc_lsn") or 0)
            item["tx_id"] = int(item.get("tx_id") or 0)
            item["partition"] = int(item.get("partition") or 0)
            item["offset"] = int(item.get("offset") or 0)
            # Convert JSON structures to string for Arrow consistency if needed
            for col in ("before", "after", "source", "transaction"):
                val = item.get(col)
                if val is not None and not isinstance(val, str):
                    item[col] = json.dumps(val)
            formatted_records.append(item)

        df = pd.DataFrame(formatted_records)

        table = pa.Table.from_pandas(df)

        if DeltaTable.is_deltatable(target_dir):
            write_deltalake(target_dir, table, mode="append", partition_by=["_partition_date"], schema_mode="merge")
        else:
            write_deltalake(target_dir, table, mode="overwrite", partition_by=["_partition_date"])

        logger.info(f"[BRONZE] Appended {len(records)} raw records to Delta table at {target_dir}")
        return len(records)

    # -------------------------------------------------------------
    # Silver Layer: Native Delta Lake ACID MERGE INTO (Upsert)
    # -------------------------------------------------------------
    def merge_silver(
        self,
        entity_name: str,
        incoming_records: List[Dict[str, Any]],
        primary_key: str,
        timestamp_col: str = "ts_ms"
    ) -> Dict[str, int]:
        """
        Executes native Delta Lake MERGE INTO with strict monotonic ordering:
        Order priority:
        1. PostgreSQL LSN (_lsn) is the primary physical ordering key.
        2. ts_ms serves as tie-breaker when LSN is equal or zero.
        Delete semantics:
        Uses tombstones (_is_deleted=True, _cdc_op='d') to prevent stale updates
        from resurrecting deleted rows, while preserving full state for time-travel.
        """
        if not incoming_records:
            return {"inserted": 0, "updated": 0, "deleted": 0, "ignored": 0}

        target_dir = os.path.join(self.silver_dir, entity_name)
        os.makedirs(target_dir, exist_ok=True)

        incoming_df = pd.DataFrame(incoming_records)

        # Standardize timestamp and LSN columns
        if timestamp_col not in incoming_df.columns and "_cdc_ts_ms" in incoming_df.columns:
            incoming_df[timestamp_col] = incoming_df["_cdc_ts_ms"]
        incoming_df[timestamp_col] = pd.to_numeric(incoming_df.get(timestamp_col, 0), errors="coerce").fillna(0).astype("int64")

        if "_lsn" in incoming_df.columns:
            incoming_df["_lsn"] = pd.to_numeric(incoming_df["_lsn"], errors="coerce").fillna(0).astype("int64")
        elif "cdc_lsn" in incoming_df.columns:
            incoming_df["_lsn"] = pd.to_numeric(incoming_df["cdc_lsn"], errors="coerce").fillna(0).astype("int64")
        else:
            incoming_df["_lsn"] = 0

        if "_cdc_op" not in incoming_df.columns:
            incoming_df["_cdc_op"] = "u"

        incoming_df["_is_deleted"] = incoming_df["_cdc_op"] == "d"
        incoming_df["_last_synced_at"] = datetime.now(timezone.utc).isoformat()

        # Intra-batch deduplication: sort by (_lsn, timestamp_col) ascending and take last per PK
        sort_cols = ["_lsn", timestamp_col]
        incoming_sorted = incoming_df.sort_values(by=sort_cols, ascending=[True, True])
        deduped_df = incoming_sorted.groupby(primary_key).last().reset_index()

        table_exists = DeltaTable.is_deltatable(target_dir)

        if not table_exists:
            # Initial table creation: filter out records whose latest operation is 'd'
            active_df = deduped_df[deduped_df["_cdc_op"] != "d"].copy()
            if active_df.empty:
                logger.info(f"[SILVER] Initial batch for '{entity_name}' contained only deletes. Table not initialized.")
                return {
                    "inserted": 0,
                    "updated": 0,
                    "deleted": 0,
                    "ignored": len(incoming_records)
                }

            initial_table = pa.Table.from_pandas(active_df)
            write_deltalake(target_dir, initial_table, mode="overwrite")
            inserted_count = len(active_df)
            ignored_count = len(incoming_records) - inserted_count
            logger.info(f"[SILVER] Initialized Delta table '{entity_name}' with {inserted_count} records")
            return {
                "inserted": inserted_count,
                "updated": 0,
                "deleted": 0,
                "ignored": ignored_count
            }

        # Table exists: execute native Delta Lake merge
        dt = DeltaTable(target_dir)
        source_table = pa.Table.from_pandas(deduped_df)

        # Monotonic ordering condition: LSN is primary; ts_ms tie-breaks
        order_cond = f"(COALESCE(source._lsn, 0) > COALESCE(target._lsn, 0)) OR (COALESCE(source._lsn, 0) = COALESCE(target._lsn, 0) AND source.{timestamp_col} >= target.{timestamp_col})"

        merge_builder = (
            dt.merge(
                source=source_table,
                predicate=f"target.{primary_key} = source.{primary_key}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all(
                predicate=order_cond
            )
            .when_not_matched_insert_all(
                predicate="source._cdc_op != 'd'"
            )
        )

        metrics = merge_builder.execute()

        inserted_count = metrics.get("num_target_rows_inserted", 0)
        updated_count = metrics.get("num_target_rows_updated", 0)
        deleted_count = metrics.get("num_target_rows_deleted", 0)
        
        # Calculate deleted vs updated based on tombstone changes
        tombstone_deletes = len(deduped_df[deduped_df["_cdc_op"] == "d"]) if updated_count > 0 else 0
        actual_updated = updated_count - tombstone_deletes if updated_count >= tombstone_deletes else updated_count
        actual_deleted = deleted_count + tombstone_deletes

        affected_count = inserted_count + updated_count + deleted_count
        ignored_count = max(0, len(incoming_records) - affected_count)

        stats = {
            "inserted": inserted_count,
            "updated": actual_updated,
            "deleted": actual_deleted,
            "ignored": ignored_count,
        }
        logger.info(f"[SILVER] Native Delta MERGE into '{entity_name}': {stats}")
        return stats

    # -------------------------------------------------------------
    # DLQ / Quarantine Layer: Poison Pill & Anomaly Isolation
    # -------------------------------------------------------------
    def route_to_quarantine(
        self,
        raw_payload: str,
        error_type: str,
        error_message: str,
        topic: str,
        partition: int = 0,
        offset: int = 0,
        key: Optional[str] = None,
        entity: Optional[str] = None,
        operation: Optional[str] = None,
        source_table: Optional[str] = None,
        stack_trace: Optional[str] = None
    ) -> str:
        """
        Isolates corrupted messages, schema validation failures, and unparseable events.
        Enriches record with full forensic metadata: event_id, topic, partition, offset,
        key, entity, operation, source_table, error_type, error_message, stack_trace, quarantined_at.
        """
        now = datetime.now(timezone.utc)
        dlq_file = os.path.join(
            self.dlq_dir,
            f"quarantine_{now.strftime('%Y%m%d')}.jsonl"
        )
        event_id = f"{topic}:{partition}:{offset}"
        record = {
            "event_id": event_id,
            "raw_payload": raw_payload,
            "error_type": error_type,
            "error_message": error_message,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "key": key,
            "entity": entity,
            "operation": operation,
            "source_table": source_table,
            "stack_trace": stack_trace,
            "quarantined_at": now.isoformat()
        }
        with open(dlq_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        logger.warning(f"[DLQ] Quarantined invalid event {event_id} into {dlq_file}: {error_type} - {error_message}")
        return dlq_file

    def get_silver_table(self, entity_name: str, include_deleted: bool = False) -> pd.DataFrame:
        """
        Reads the current curated Silver dataset as a pandas DataFrame.
        By default (include_deleted=False), filters out tombstoned deleted rows.
        """
        target_dir = os.path.join(self.silver_dir, entity_name)
        if DeltaTable.is_deltatable(target_dir):
            df = DeltaTable(target_dir).to_pandas()
            if not include_deleted and "_is_deleted" in df.columns:
                return df[df["_is_deleted"] == False].copy().reset_index(drop=True)
            return df

        legacy_file = os.path.join(self.silver_dir, f"{entity_name}.parquet")
        if os.path.exists(legacy_file):
            df = pd.read_parquet(legacy_file)
            if not include_deleted and "_is_deleted" in df.columns:
                return df[df["_is_deleted"] == False].copy().reset_index(drop=True)
            return df

        return pd.DataFrame()

    def get_silver_delta_table(self, entity_name: str) -> Optional[DeltaTable]:
        """Returns the DeltaTable instance for the entity if it exists."""
        target_dir = os.path.join(self.silver_dir, entity_name)
        if DeltaTable.is_deltatable(target_dir):
            return DeltaTable(target_dir)
        return None

    def get_bronze_table(self, entity_name: str) -> pd.DataFrame:
        """Reads all Bronze changelog records as a pandas DataFrame."""
        target_dir = os.path.join(self.bronze_dir, entity_name)
        if DeltaTable.is_deltatable(target_dir):
            return DeltaTable(target_dir).to_pandas()
        return pd.DataFrame()

    def get_bronze_delta_table(self, entity_name: str) -> Optional[DeltaTable]:
        """Returns the Bronze DeltaTable instance for the entity if it exists."""
        target_dir = os.path.join(self.bronze_dir, entity_name)
        if DeltaTable.is_deltatable(target_dir):
            return DeltaTable(target_dir)
        return None
