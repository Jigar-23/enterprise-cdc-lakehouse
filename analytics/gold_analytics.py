"""
Gold Layer Analytics:
Performs analytical aggregations, KPI calculations, and operational alerts
over curated Silver tables and Bronze change-streams using DuckDB.
"""

import os
import duckdb
import pandas as pd
from typing import Dict, Any


class GoldLakehouseAnalytics:
    """
    Executes OLAP queries and metric aggregations using DuckDB directly over
    Parquet files in the Lakehouse storage.
    """

    def __init__(self, lakehouse_dir: str = "./lakehouse_storage"):
        self.lakehouse_dir = lakehouse_dir
        self.silver_dir = os.path.join(self.lakehouse_dir, "silver")
        self.bronze_dir = os.path.join(self.lakehouse_dir, "bronze")
        self.con = duckdb.connect(database=":memory:")

    def get_low_stock_alerts(self, threshold: int = 500) -> pd.DataFrame:
        """
        Identifies inventory items requiring immediate reorder based on available stock
        and pending reservation ratio.
        """
        inventory_file = os.path.join(self.silver_dir, "inventory.parquet")
        if not os.path.exists(inventory_file):
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
        FROM parquet_scan('{inventory_file}')
        WHERE available_stock < {threshold} OR (available_stock - reserved_stock) < 100
        ORDER BY net_free_stock ASC
        """
        return self.con.execute(query).df()

    def get_order_revenue_summary(self) -> pd.DataFrame:
        """
        Aggregates order volumes and gross merchandise value (GMV) by status and customer.
        """
        orders_file = os.path.join(self.silver_dir, "orders.parquet")
        if not os.path.exists(orders_file):
            return pd.DataFrame()

        query = f"""
        SELECT 
            customer_id,
            status,
            COUNT(order_id) AS total_orders,
            SUM(quantity) AS total_units_ordered,
            ROUND(SUM(total_price), 2) AS total_revenue_eur,
            ROUND(AVG(total_price), 2) AS avg_order_value_eur
        FROM parquet_scan('{orders_file}')
        GROUP BY customer_id, status
        ORDER BY total_revenue_eur DESC
        """
        return self.con.execute(query).df()

    def get_cdc_velocity_audit(self) -> pd.DataFrame:
        """
        Analyzes the ingestion velocity and operation breakdown (inserts, updates, deletes)
        from raw Bronze changelog partitions.
        """
        bronze_orders = os.path.join(self.bronze_dir, "orders", "*.parquet")
        try:
            query = f"""
            SELECT 
                _cdc_op AS operation_type,
                CASE _cdc_op
                    WHEN 'c' THEN 'INSERT'
                    WHEN 'u' THEN 'UPDATE'
                    WHEN 'd' THEN 'DELETE'
                    WHEN 'r' THEN 'SNAPSHOT_READ'
                    ELSE 'UNKNOWN'
                END AS operation_name,
                COUNT(*) AS event_count,
                MIN(_ingested_at) AS first_seen_at,
                MAX(_ingested_at) AS latest_seen_at
            FROM parquet_scan('{bronze_orders}')
            GROUP BY _cdc_op
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
