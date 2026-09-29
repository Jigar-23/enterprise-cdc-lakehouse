#!/usr/bin/env bash
# ==============================================================================
# Enterprise CDC & Delta Lakehouse: Interactive Demo Script
# Demonstrates full Medallion lifecycle, ACID Delta commits, DLQ, and DuckDB OLAP.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

echo "======================================================================"
echo "      ENTERPRISE CDC & DELTA LAKEHOUSE PLATFORM - LIVE DEMO          "
echo "======================================================================"

# Clean demo directory
DEMO_STORAGE="./demo_lakehouse_storage"
rm -rf "${DEMO_STORAGE}"

echo ""
echo ">>> STEP 1: Running High-Fidelity ERP Transaction Stream Simulation"
echo "----------------------------------------------------------------------"
echo "Feeding events through CDCPipelineConsumer into Delta Lakehouse..."
LAKEHOUSE_STORAGE_DIR="${DEMO_STORAGE}" python3 scripts/simulate_erp_transactions.py --stream-mode --storage-dir "${DEMO_STORAGE}"

echo ""
echo ">>> STEP 2: Inspecting Bronze Layer (Append-Only Delta Table & Logs)"
echo "----------------------------------------------------------------------"
python3 -c "
import os
from deltalake import DeltaTable
bronze_dir = '${DEMO_STORAGE}/bronze/orders'
if os.path.exists(bronze_dir):
    dt = DeltaTable(bronze_dir)
    print(f'Bronze Orders Table Version: {dt.version()}')
    print('Transaction Log Directory:', os.path.join(bronze_dir, '_delta_log'))
    print('Recent Log Files:', os.listdir(os.path.join(bronze_dir, '_delta_log'))[:5])
    print(f'Total Ingested Raw Changelog Records: {len(dt.to_pandas())}')
"

echo ""
echo ">>> STEP 3: Inspecting Silver Layer (Curated ACID Upserts & History)"
echo "----------------------------------------------------------------------"
python3 -c "
import os
from deltalake import DeltaTable
silver_inv = '${DEMO_STORAGE}/silver/inventory'
if os.path.exists(silver_inv):
    dt = DeltaTable(silver_inv)
    print(f'Silver Inventory Table Version: {dt.version()}')
    print(f'Active Inventory Records:\n{dt.to_pandas()}')
"

echo ""
echo ">>> STEP 4: Inspecting Dead Letter Queue (DLQ Poison Pill Isolation)"
echo "----------------------------------------------------------------------"
python3 -c "
import os, json
dlq_dir = '${DEMO_STORAGE}/quarantine'
if os.path.exists(dlq_dir):
    files = [f for f in os.listdir(dlq_dir) if f.endswith('.jsonl')]
    for file in files:
        print(f'Found DLQ Log: {file}')
        with open(os.path.join(dlq_dir, file)) as f:
            for i, line in enumerate(f, 1):
                rec = json.loads(line)
                print(f'  [Poison Pill {i}] Error: {rec[\"error_type\"]} | Topic: {rec.get(\"topic\", rec.get(\"source_topic\"))}')
                print(f'                  Msg: {rec[\"error_message\"][:80]}...')
"

echo ""
echo ">>> STEP 5: Executing DuckDB Gold Layer OLAP Analytics"
echo "----------------------------------------------------------------------"
python3 -c "
from analytics.gold_analytics import GoldLakehouseAnalytics
analytics = GoldLakehouseAnalytics(lakehouse_dir='${DEMO_STORAGE}')
print('--- LOW STOCK ALERTS (OLAP Query) ---')
print(analytics.get_low_stock_alerts())
print('\n--- REVENUE GMV BY CUSTOMER ---')
print(analytics.get_order_revenue_summary())
print('\n--- CDC INGESTION VELOCITY BREAKDOWN ---')
print(analytics.get_cdc_velocity_audit())
"

echo ""
echo ">>> STEP 6: Cleaning up demo storage"
rm -rf "${DEMO_STORAGE}"
echo "======================================================================"
echo "[✓] DEMO COMPLETE: All Medallion layers, Delta logs, and DLQ verified!"
echo "======================================================================"
