#!/usr/bin/env bash
# Register Debezium CDC PostgreSQL Connector with Kafka Connect REST API
# Dynamically substitutes environment variables for credentials and endpoints.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG_FILE="$(dirname "$0")/postgres-connector.json"

echo "[INFO] Verifying Kafka Connect availability at ${CONNECT_URL}..."
MAX_RETRIES=30
RETRY_COUNT=0

until curl -s -f -o /dev/null "${CONNECT_URL}/connectors"; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
        echo "[ERROR] Kafka Connect failed to become healthy within $((MAX_RETRIES * 2)) seconds."
        exit 1
    fi
    echo "[WAIT] Kafka Connect is initializing (${RETRY_COUNT}/${MAX_RETRIES}). Retrying in 2s..."
    sleep 2
done

echo "[INFO] Kafka Connect is healthy."
echo "[INFO] Resolving configuration template from ${CONNECTOR_CONFIG_FILE}..."

RESOLVED_PAYLOAD=$(python3 -c '
import os, json, re

config_file = "connectors/postgres-connector.json"
with open(config_file) as f:
    raw = f.read()

def replacer(m):
    var_name = m.group(1)
    default_val = m.group(2) if m.group(2) is not None else ""
    return os.getenv(var_name, default_val)

pattern = re.compile(r"\$\{([A-Za-z0-9_]+)(?::-([^}]*))?\}")
resolved = pattern.sub(replacer, raw)
json.loads(resolved)
print(resolved)
')

echo "[INFO] Registering or updating connector 'postgres-cdc-connector'..."

RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}" -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d "${RESOLVED_PAYLOAD}")

HTTP_STATUS=$(echo "$RESPONSE" | grep "HTTP_STATUS" | cut -d: -f2)
BODY=$(echo "$RESPONSE" | grep -v "HTTP_STATUS")

if [ "$HTTP_STATUS" -eq 201 ]; then
    echo "[SUCCESS] Connector registered successfully! Status: 201 Created."
elif [ "$HTTP_STATUS" -eq 409 ]; then
    echo "[INFO] Connector already exists (409 Conflict). Updating configuration..."
    CONFIG_ONLY=$(echo "${RESOLVED_PAYLOAD}" | python3 -c "import sys, json; print(json.dumps(json.load(sys.stdin)['config']))")
    curl -s -f -X PUT "${CONNECT_URL}/connectors/postgres-cdc-connector/config" \
      -H "Content-Type: application/json" \
      -d "${CONFIG_ONLY}" > /dev/null
    echo "[SUCCESS] Connector configuration updated successfully."
else
    echo "[ERROR] Failed to register connector. HTTP Status: ${HTTP_STATUS}"
    echo "$BODY"
    exit 1
fi

echo "[INFO] Waiting for connector tasks to reach RUNNING state..."
TASK_RETRIES=15
TASK_COUNT=0
CONNECTOR_RUNNING=false

while [ "$TASK_COUNT" -lt "$TASK_RETRIES" ]; do
    TASK_STATUS=$(curl -s "${CONNECT_URL}/connectors/postgres-cdc-connector/status" 2>/dev/null || echo "{}")
    STATE=$(echo "$TASK_STATUS" | python3 -c "import sys, json; data=json.load(sys.stdin); print(data.get('connector', {}).get('state', ''))" 2>/dev/null || echo "")
    
    if [ "$STATE" = "RUNNING" ]; then
        echo "[SUCCESS] Connector 'postgres-cdc-connector' is in RUNNING state."
        CONNECTOR_RUNNING=true
        break
    fi
    TASK_COUNT=$((TASK_COUNT + 1))
    sleep 2
done

if [ "$CONNECTOR_RUNNING" = false ]; then
    echo "[WARNING] Connector has not reached RUNNING state yet. Status:"
    curl -s "${CONNECT_URL}/connectors/postgres-cdc-connector/status" | python3 -m json.tool || true
fi

echo "[SUCCESS] PostgreSQL CDC setup complete."
