#!/usr/bin/env bash
# ==============================================================================
# Enterprise CDC & Delta Lakehouse: Bootstrap Script
# Sets up environment, validates dependencies, starts services, and registers CDC.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

echo "======================================================================"
echo "   Enterprise CDC & Delta Lakehouse Platform - System Bootstrap      "
echo "======================================================================"

# 1. Environment file setup
if [ ! -f .env ]; then
    echo "[+] Creating .env from .env.example..."
    cp .env.example .env
else
    echo "[✓] .env file detected."
fi

# Load environment variables
set -a
# shellcheck disable=SC1091
source .env
set +a

# 2. Check Python environment
echo "[+] Checking Python dependencies..."
python3 -c "import deltalake, pyarrow, pandas, duckdb, pydantic, psycopg2" 2>/dev/null && {
    echo "[✓] Core Python libraries verified (deltalake, pyarrow, pandas, duckdb, pydantic, psycopg2)."
} || {
    echo "[!] Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
}

# 3. Mode selection
MODE="${1:---full}"

if [ "${MODE}" = "--local" ] || [ "${MODE}" = "--offline" ]; then
    echo "======================================================================"
    echo "Running in OFFLINE/LOCAL mode (No Docker required)..."
    echo "======================================================================"
    echo "[+] Running unit & pipeline tests..."
    pytest tests/ -v
    echo "[+] Running stream simulation into Delta Lakehouse..."
    python3 scripts/simulate_erp_transactions.py --stream-mode
    echo "[✓] Local bootstrap and validation complete!"
    exit 0
fi

# 4. Check Docker daemon
echo "[+] Checking Docker availability..."
if ! docker info >/dev/null 2>&1; then
    echo "[WARNING] Docker daemon is not currently running on this machine."
    echo "          To run the full live stack (PostgreSQL, Redpanda, Debezium), start Docker Desktop/daemon."
    echo "          Running local Lakehouse engine validation instead..."
    pytest tests/ -v
    python3 scripts/simulate_erp_transactions.py --stream-mode
    exit 0
fi

# 5. Start Docker Infrastructure
echo "[+] Starting Docker services (postgres-source, kafka, connect)..."
docker compose up -d postgres-source kafka connect

echo "[+] Waiting for services to become healthy..."
MAX_WAIT=60
WAIT_COUNT=0

until curl -s "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
    echo "    Waiting for Debezium Kafka Connect at ${CONNECT_URL}... (${WAIT_COUNT}s)"
    sleep 3
    WAIT_COUNT=$((WAIT_COUNT + 3))
    if [ ${WAIT_COUNT} -ge ${MAX_WAIT} ]; then
        echo "[ERROR] Debezium Connect failed to start within ${MAX_WAIT}s."
        docker compose logs connect
        exit 1
    fi
done
echo "[✓] Debezium Kafka Connect is online and responsive."

# 6. Register Debezium Connector
echo "[+] Registering PostgreSQL CDC Connector..."
bash connectors/register-postgres.sh

# 7. Start CDC Consumer
echo "[+] Starting containerized CDC consumer..."
docker compose up -d cdc-consumer

echo "======================================================================"
echo "[✓] Enterprise CDC Lakehouse Platform is fully bootstrapped and live!"
echo "    - PostgreSQL: ${POSTGRES_HOST}:${POSTGRES_PORT} (DB: ${POSTGRES_DB})"
echo "    - Redpanda/Kafka: ${KAFKA_BROKER}"
echo "    - Debezium Connect: ${CONNECT_URL}"
echo "    - CDC Consumer: Streaming into lakehouse_storage/"
echo "======================================================================"
echo "To generate transactions, run:"
echo "    python3 scripts/simulate_erp_transactions.py --database-mode"
echo "To view live analytics, run:"
echo "    python3 analytics/gold_analytics.py"
echo "======================================================================"
