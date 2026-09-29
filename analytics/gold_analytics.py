"""
Gold Layer Analytics:
Performs analytical aggregations, KPI calculations, and operational alerts
over curated Silver tables and Bronze change-streams using DuckDB.
Supports native Delta Lake tables (via zero-copy PyArrow datasets) and Parquet fallback.
Filters out tombstoned deleted rows in Silver to ensure accurate current-state OLAP.
"""

import os
import duckdb
import pandas as pd
from typing import Dict, Any
from deltalake import DeltaTable


class GoldLakehouseAnalytics:
    """
    Executes OLAP queries and metric aggregations using DuckDB directly over
    Delta Lake tables in the Lakehouse storage.
    """

    def __init__(self, lakehouse_dir: str = "./lakehouse_storage"):
        self.lakehouse_dir = os.path.abspath(lakehouse_dir)
        self.silver_dir = os.path.join(self.lakehouse_dir, "silver")
        self.bronze_dir = os.path.join(self.lakehouse_dir, "bronze")
        self.con = duckdb.connect(database=":memory:")

    def _register_table(self, entity_name: str, layer: str) -> bool:
        """Registers a Delta Lake table or parquet file as a clean DuckDB view."""
        table_alias = f"{layer}_{entity_name}"
        layer_dir = self.silver_dir if layer == "silver" else self.bronze_dir
        target_dir = os.path.join(layer_dir, entity_name)

        if DeltaTable.is_deltatable(target_dir):
            dt = DeltaTable(target_dir)
            raw_alias = f"{table_alias}_raw"
            self.con.register(raw_alias, dt.to_pyarrow_dataset())
            if layer == "silver":
                # Clean view filtering out tombstoned deleted rows
                self.con.execute(f"""
                CREATE OR REPLACE VIEW {table_alias} AS 
                SELECT * FROM {raw_alias} 
                WHERE COALESCE(_is_deleted, false) = false
                """)
            else:
                self.con.execute(f"CREATE OR REPLACE VIEW {table_alias} AS SELECT * FROM {raw_alias}")
            return True

        # Check for single parquet file (legacy fallback)
        parquet_file = os.path.join(layer_dir, f"{entity_name}.parquet")
        if os.path.exists(parquet_file):
            if layer == "silver":
                self.con.execute(f"""
                CREATE OR REPLACE VIEW {table_alias} AS 
                SELECT * FROM parquet_scan('{parquet_file}')
                WHERE COALESCE(_is_deleted, false) = false
                """)
            else:
                self.con.execute(f"CREATE OR REPLACE VIEW {table_alias} AS SELECT * FROM parquet_scan('{parquet_file}')")
            return True

        # Check for partitioned bronze parquet directory (legacy fallback)
        if os.path.isdir(target_dir):
            parquet_files = [
                os.path.join(r, f)
                for r, _, files in os.walk(target_dir)
                for f in files if f.endswith(".parquet")
            ]
            if parquet_files:
                self.con.execute(f"CREATE OR REPLACE VIEW {table_alias} AS SELECT * FROM read_parquet({parquet_files})")
                return True

        return False

    def get_low_stock_alerts(self, threshold: int = 500) -> pd.DataFrame:
        """
        Identifies active inventory items requiring immediate reorder based on available stock
        and pending reservation ratio.
        """
        if not self._register_table("inventory", "silver"):
            return pd.DataFrame()

        query = f"""
        SELECT 
            sku,
            warehouse_id,
            available_stock,
            reserved_stock,
            (available_stock - reserved_stock) AS net_free_stock,
            CASE 
                WHEN available_stock < {threshold} THEN 'CRITICAL_REORDER'
                WHEN (available_stock - reserved_stock) < 100 THEN 'WARNING_HIGH_ALLOCATION'
                ELSE 'HEALTHY'
            END AS stock_status,
            last_restocked_at
        FROM silver_inventory
        WHERE available_stock < {threshold} OR (available_stock - reserved_stock) < 100
        ORDER BY net_free_stock ASC
        """
        try:
            return self.con.execute(query).df()
        except Exception:
            return pd.DataFrame()

    def get_order_revenue_summary(self) -> pd.DataFrame:
        """
        Aggregates active order volumes and gross merchandise value (GMV) by status and customer.
        """
        if not self._register_table("orders", "silver"):
            return pd.DataFrame()

        query = """
        SELECT 
            customer_id,
            status,
            COUNT(order_id) AS total_orders,
            SUM(quantity) AS total_units_ordered,
            ROUND(SUM(total_price), 2) AS total_revenue_eur,
            ROUND(AVG(total_price), 2) AS avg_order_value_eur
        FROM silver_orders
        GROUP BY customer_id, status
        ORDER BY total_revenue_eur DESC
        """
        try:
            return self.con.execute(query).df()
        except Exception:
            return pd.DataFrame()

    def get_cdc_velocity_audit(self) -> pd.DataFrame:
        """
        Analyzes the ingestion velocity and operation breakdown (inserts, updates, deletes)
        from raw Bronze changelog partitions.
        """
        if not self._register_table("orders", "bronze"):
            return pd.DataFrame()

        try:
            cols = [c[0] for c in self.con.execute("DESCRIBE bronze_orders").fetchall()]
            op_col = "op" if "op" in cols else ("_cdc_op" if "_cdc_op" in cols else None)
            ts_col = "ingested_at" if "ingested_at" in cols else ("_ingested_at" if "_ingested_at" in cols else None)
            if not op_col:
                return pd.DataFrame()

            ts_min = f"MIN({ts_col})" if ts_col else "NULL"
            ts_max = f"MAX({ts_col})" if ts_col else "NULL"

            query = f"""
            SELECT 
                {op_col} AS operation_type,
                CASE {op_col}
                    WHEN 'c' THEN 'INSERT'
                    WHEN 'u' THEN 'UPDATE'
                    WHEN 'd' THEN 'DELETE'
                    WHEN 'r' THEN 'SNAPSHOT_READ'
                    ELSE 'UNKNOWN'
                END AS operation_name,
                COUNT(*) AS event_count,
                {ts_min} AS first_seen_at,
                {ts_max} AS latest_seen_at
            FROM bronze_orders
            GROUP BY {op_col}
            ORDER BY event_count DESC
            """
            return self.con.execute(query).df()
        except Exception:
            return pd.DataFrame()

    def generate_gold_executive_summary(self) -> Dict[str, Any]:
        """Runs all Gold metrics and returns a consolidated summary dictionary."""
        return {
            "low_stock_alerts": self.get_low_stock_alerts().to_dict(orient="records"),
            "revenue_summary": self.get_order_revenue_summary().to_dict(orient="records"),
            "cdc_audit": self.get_cdc_velocity_audit().to_dict(orient="records"),
        }


if __name__ == "__main__":
    analytics = GoldLakehouseAnalytics()
    print("--- LOW STOCK ALERTS ---")
    print(analytics.get_low_stock_alerts())
    print("\n--- REVENUE BY CUSTOMER ---")
    print(analytics.get_order_revenue_summary())
    print("\n--- CDC INGESTION VELOCITY ---")
    print(analytics.get_cdc_velocity_audit())
