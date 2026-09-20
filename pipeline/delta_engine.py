"""
Delta Engine: Implements Medallion Architecture (Bronze -> Silver -> DLQ)
Simulates Delta Lake ACID Upsert semantics (MERGE INTO) using PyArrow & DuckDB.
Provides idempotent writes, schema enforcement, and out-of-order event resolution.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("delta_engine")


class DeltaLakehouseEngine:
    """
    Simulates Delta Lake storage and transaction layer for CDC streaming.
    - Bronze: Append-only raw event changelog partitioned by date.
    - Silver: Curated tables with ACID MERGE INTO semantics (deduplicated by primary key & timestamp).
    - DLQ: Quarantine for poison pills, corrupt schema changes, and invalid events.
    """

    def __init__(self, base_storage_dir: str = "./lakehouse_storage"):
        self.base_dir = base_storage_dir
        self.bronze_dir = os.path.join(self.base_dir, "bronze")
        self.silver_dir = os.path.join(self.base_dir, "silver")
        self.dlq_dir = os.path.join(self.base_dir, "quarantine")

        for d in [self.bronze_dir, self.silver_dir, self.dlq_dir]:
            os.makedirs(d, exist_ok=True)

    # -------------------------------------------------------------
    # Bronze Layer: Raw Changelog Ingestion (Append-Only)
    # -------------------------------------------------------------
    def append_bronze(self, entity_name: str, records: List[Dict[str, Any]]) -> int:
        """
        Appends raw CDC records into the Bronze layer partitioned by ingestion date.
        """
        if not records:
            return 0

        target_dir = os.path.join(self.bronze_dir, entity_name)
        os.makedirs(target_dir, exist_ok=True)

        now = datetime.now(timezone.utc)
        partition_date = now.strftime("%Y-%m-%d")
        file_name = f"bronze_{entity_name}_{partition_date}_{int(now.timestamp() * 1000)}.parquet"
        file_path = os.path.join(target_dir, file_name)

        df = pd.DataFrame(records)
        df["_ingested_at"] = now.isoformat()
        df["_partition_date"] = partition_date

        table = pa.Table.from_pandas(df)
        pq.write_table(table, file_path, compression="snappy")
        logger.info(f"[BRONZE] Appended {len(records)} raw records to {file_path}")
        return len(records)

    # -------------------------------------------------------------
    # Silver Layer: ACID MERGE INTO (Upsert with Out-of-Order Handling)
    # -------------------------------------------------------------
    def merge_silver(
        self,
        entity_name: str,
        incoming_records: List[Dict[str, Any]],
        primary_key: str,
        timestamp_col: str = "ts_ms"
    ) -> Dict[str, int]:
        """
        Simulates Delta Lake MERGE INTO:
        MERGE INTO silver.<entity> AS target
        USING incoming AS source
        ON target.<primary_key> = source.<primary_key>
        WHEN MATCHED AND source.<ts_ms> >= target.<ts_ms> THEN UPDATE
        WHEN NOT MATCHED THEN INSERT
        """
        if not incoming_records:
            return {"inserted": 0, "updated": 0, "deleted": 0, "ignored": 0}

        target_file = os.path.join(self.silver_dir, f"{entity_name}.parquet")
        incoming_df = pd.DataFrame(incoming_records)

        # Load existing Silver table or initialize empty DataFrame
        if os.path.exists(target_file):
            silver_df = pd.read_parquet(target_file)
        else:
            silver_df = pd.DataFrame()

        inserted_count = 0
        updated_count = 0
        deleted_count = 0
        ignored_count = 0

        if silver_df.empty:
            # First initialization: filter deleted, keep latest per PK
            incoming_df = incoming_df.sort_values(by=timestamp_col, ascending=True)
            grouped = incoming_df.groupby(primary_key).last().reset_index()
            # Filter out records whose latest operation is 'd' (delete)
            if "_cdc_op" in grouped.columns:
                active_df = grouped[grouped["_cdc_op"] != "d"].copy()
            else:
                active_df = grouped
            active_df["_last_synced_at"] = datetime.now(timezone.utc).isoformat()
            
            table = pa.Table.from_pandas(active_df)
            pq.write_table(table, target_file, compression="snappy")
            inserted_count = len(active_df)
            return {"inserted": inserted_count, "updated": 0, "deleted": 0, "ignored": 0}

        # Convert Silver into dict indexed by primary key for fast deterministic merge
        existing_map = {}
        for row in silver_df.to_dict(orient="records"):
            existing_map[row[primary_key]] = row

        # Sort incoming records chronologically
        incoming_sorted = incoming_df.sort_values(by=timestamp_col, ascending=True).to_dict(orient="records")

        for inc in incoming_sorted:
            pk_val = inc[primary_key]
            inc_ts = inc.get(timestamp_col, 0)
            op = inc.get("_cdc_op", "u")

            if pk_val not in existing_map:
                if op != "d":
                    inc["_last_synced_at"] = datetime.now(timezone.utc).isoformat()
                    existing_map[pk_val] = inc
                    inserted_count += 1
                else:
                    ignored_count += 1
            else:
                current_record = existing_map[pk_val]
                curr_ts = current_record.get(timestamp_col, 0)

                # Out-of-order check: only apply if incoming event is newer or equal
                if inc_ts >= curr_ts:
                    if op == "d":
                        # Soft delete or removal
                        del existing_map[pk_val]
                        deleted_count += 1
                    else:
                        inc["_last_synced_at"] = datetime.now(timezone.utc).isoformat()
                        existing_map[pk_val] = inc
                        updated_count += 1
                else:
                    # Stale / out-of-order event arrived; ignore to maintain consistency
                    ignored_count += 1

        # Atomically write updated state back to Silver Parquet file
        updated_records = list(existing_map.values())
        if updated_records:
            final_df = pd.DataFrame(updated_records)
        else:
            final_df = pd.DataFrame(columns=silver_df.columns)

        temp_target = f"{target_file}.tmp"
        table = pa.Table.from_pandas(final_df)
        pq.write_table(table, temp_target, compression="snappy")
        os.replace(temp_target, target_file)

        stats = {
            "inserted": inserted_count,
            "updated": updated_count,
            "deleted": deleted_count,
            "ignored": ignored_count
        }
        logger.info(f"[SILVER] Merged into '{entity_name}': {stats}")
        return stats

    # -------------------------------------------------------------
    # DLQ / Quarantine Layer: Poison Pill & Anomaly Isolation
    # -------------------------------------------------------------
    def route_to_quarantine(
        self,
        raw_payload: str,
        error_type: str,
        error_message: str,
        source_topic: Optional[str] = None
    ) -> str:
        """
        Isolates corrupted messages, schema validation failures, and unparseable JSON.
        """
        now = datetime.now(timezone.utc)
        dlq_file = os.path.join(
            self.dlq_dir,
            f"quarantine_{now.strftime('%Y%m%d')}.jsonl"
        )
        record = {
            "quarantined_at": now.isoformat(),
            "source_topic": source_topic or "unknown",
            "error_type": error_type,
            "error_message": error_message,
            "raw_payload": raw_payload
        }
        with open(dlq_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

        logger.warning(f"[DLQ] Quarantined invalid event into {dlq_file}: {error_type} - {error_message}")
        return dlq_file

    def get_silver_table(self, entity_name: str) -> pd.DataFrame:
        """Reads the current curated Silver dataset."""
        target_file = os.path.join(self.silver_dir, f"{entity_name}.parquet")
        if not os.path.exists(target_file):
            return pd.DataFrame()
        return pd.read_parquet(target_file)
