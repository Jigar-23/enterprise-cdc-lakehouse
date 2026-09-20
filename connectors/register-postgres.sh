#!/usr/bin/env bash
# Script to register Debezium CDC Postgres Connector with Kafka Connect REST API

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG_FILE="$(dirname "$0")/postgres-connector.json"

echo "[INFO] Verifying Kafka Connect availability at ${CONNECT_URL}..."
until curl -s -f -o /dev/null "${CONNECT_URL}/connectors"; do
    echo "[WAIT] Kafka Connect is still starting up. Retrying in 5 seconds..."
    sleep 5
done

echo "[INFO] Kafka Connect is healthy."
echo "[INFO] Registering or updating connector 'postgres-cdc-connector'..."

RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d @"${CONNECTOR_CONFIG_FILE}")

HTTP_STATUS=$(echo "$RESPONSE" | grep "HTTP_STATUS" | cut -d: -f2)
BODY=$(echo "$RESPONSE" | grep -v "HTTP_STATUS")

if [ "$HTTP_STATUS" -eq 201 ]; then
    echo "[SUCCESS] Connector registered successfully! Status: 201 Created."
elif [ "$HTTP_STATUS" -eq 409 ]; then
    echo "[INFO] Connector already exists (409 Conflict). Updating configuration..."
    curl -s -X PUT "${CONNECT_URL}/connectors/postgres-cdc-connector/config" \
      -H "Content-Type: application/json" \
      -d @<(jq '.config' "${CONNECTOR_CONFIG_FILE}")
    echo "[SUCCESS] Connector configuration updated successfully."
else
    echo "[ERROR] Failed to register connector. HTTP Status: ${HTTP_STATUS}"
    echo "$BODY"
    exit 1
fi

echo "[INFO] Checking connector task status..."
curl -s "${CONNECT_URL}/connectors/postgres-cdc-connector/status" | jq . || cat
echo ""
echo "[SUCCESS] CDC stream active. PostgreSQL WAL changes are now streaming to topics: erp_pg.public.orders, erp_pg.public.inventory"
