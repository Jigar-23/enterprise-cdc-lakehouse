"""
PostgreSQL CDC Replication & WAL Progress Verifier:
Monitors PostgreSQL logical replication slots, publications, WAL positions,
and Debezium connector status to verify that CDC events are actively flowing.
"""

import os
import sys
import time
import json
import logging
import argparse
import urllib.request
import urllib.error

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cdc_verifier")


def check_debezium_connect_status(connect_url: str = "http://localhost:8083", connector_name: str = "postgres-connector"):
    """Queries Debezium Kafka Connect REST API for connector and task status."""
    url = f"{connect_url.rstrip('/')}/connectors/{connector_name}/status"
    logger.info(f"Checking Debezium connector status at: {url}")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                connector_state = data.get("connector", {}).get("state", "UNKNOWN")
                tasks = data.get("tasks", [])
                task_states = [t.get("state", "UNKNOWN") for t in tasks]
                logger.info(f"[Debezium] Connector State: {connector_state} | Tasks: {task_states}")
                return {
                    "online": True,
                    "connector_state": connector_state,
                    "tasks": tasks
                }
    except urllib.error.URLError as e:
        logger.warning(f"[Debezium] Kafka Connect not reachable at {connect_url}: {e.reason}")
    except Exception as e:
        logger.warning(f"[Debezium] Failed to query Kafka Connect: {e}")
    return {"online": False, "connector_state": "OFFLINE", "tasks": []}


def inspect_postgres_cdc(
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
    slot_name: str = "debezium_slot",
    publication_name: str = "cdc_publication"
):
    """
    Connects to PostgreSQL and verifies:
    1. Replication slot presence, status, and LSN progression
    2. Publication configuration and member tables
    3. Current WAL position and replication lag
    """
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 is required for PostgreSQL inspection. Run 'pip install psycopg2-binary'.")
        return None

    logger.info(f"Connecting to PostgreSQL at {host}:{port}/{dbname} as {user}...")
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            connect_timeout=3
        )
    except Exception as e:
        logger.warning(f"Unable to connect to PostgreSQL: {e}")
        return None

    try:
        with conn.cursor() as cur:
            # 1. Check Publication
            cur.execute(
                """
                SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
                FROM pg_publication
                WHERE pubname = %s;
                """,
                (publication_name,)
            )
            pub_row = cur.fetchone()

            cur.execute(
                """
                SELECT tablename
                FROM pg_publication_tables
                WHERE pubname = %s;
                """,
                (publication_name,)
            )
            tables = [r[0] for r in cur.fetchall()]

            # 2. Check Replication Slot
            cur.execute(
                """
                SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
                FROM pg_replication_slots
                WHERE slot_name = %s;
                """,
                (slot_name,)
            )
            slot_row = cur.fetchone()

            # 3. Check Current WAL Position & Lag
            cur.execute("SELECT pg_current_wal_lsn();")
            current_wal = cur.fetchone()[0]

            lag_bytes = None
            if slot_row and slot_row[5]:
                cur.execute(
                    "SELECT pg_wal_lsn_diff(%s, %s);",
                    (current_wal, slot_row[5])
                )
                lag_bytes = cur.fetchone()[0]

            result = {
                "publication": {
                    "name": publication_name,
                    "exists": pub_row is not None,
                    "tables": tables
                },
                "slot": {
                    "name": slot_name,
                    "exists": slot_row is not None,
                    "plugin": slot_row[1] if slot_row else None,
                    "active": slot_row[3] if slot_row else False,
                    "restart_lsn": str(slot_row[4]) if slot_row and slot_row[4] else None,
                    "confirmed_flush_lsn": str(slot_row[5]) if slot_row and slot_row[5] else None,
                },
                "current_wal_lsn": str(current_wal),
                "lag_bytes": lag_bytes
            }
            return result
    finally:
        conn.close()


def monitor_wal_progress(
    host: str,
    port: int,
    user: str,
    password: str,
    dbname: str,
    slot_name: str = "debezium_slot",
    duration_sec: int = 10,
    interval_sec: float = 2.0
):
    """Watches PostgreSQL WAL position and confirmed flush LSN over time."""
    logger.info(f"Monitoring WAL progression on slot '{slot_name}' for {duration_sec}s...")
    start_time = time.time()
    last_lsn = None

    while (time.time() - start_time) < duration_sec:
        status = inspect_postgres_cdc(host, port, user, password, dbname, slot_name=slot_name)
        if status and status.get("slot", {}).get("exists"):
            slot_info = status["slot"]
            curr_flush = slot_info.get("confirmed_flush_lsn")
            curr_wal = status.get("current_wal_lsn")
            lag = status.get("lag_bytes")
            logger.info(f"[WAL Monitor] Current WAL: {curr_wal} | Flush LSN: {curr_flush} | Lag: {lag} bytes | Active: {slot_info.get('active')}")
            if last_lsn and curr_flush != last_lsn:
                logger.info(f"[WAL Progress] Verified LSN advance: {last_lsn} -> {curr_flush}")
            last_lsn = curr_flush
        else:
            logger.warning("[WAL Monitor] PostgreSQL offline or replication slot not yet created.")
        time.sleep(interval_sec)


def main():
    parser = argparse.ArgumentParser(description="PostgreSQL CDC Replication & WAL Progress Verifier")
    parser.add_argument("--host", default=os.getenv("POSTGRES_HOST", "localhost"), help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=int(os.getenv("POSTGRES_PORT", "5432")), help="PostgreSQL port")
    parser.add_argument("--user", default=os.getenv("POSTGRES_USER", "erp_admin"), help="PostgreSQL user")
    parser.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "erp_secure_password123"), help="PostgreSQL password")
    parser.add_argument("--dbname", default=os.getenv("POSTGRES_DB", "erp_database"), help="PostgreSQL database")
    parser.add_argument("--slot", default=os.getenv("DEBEZIUM_SLOT", "debezium_slot"), help="Replication slot name")
    parser.add_argument("--publication", default="cdc_publication", help="Publication name")
    parser.add_argument("--connect-url", default=os.getenv("CONNECT_URL", "http://localhost:8083"), help="Debezium Connect URL")
    parser.add_argument("--monitor", action="store_true", help="Monitor WAL progression over time")
    parser.add_argument("--monitor-duration", type=int, default=10, help="Duration in seconds to monitor")

    args = parser.parse_args()

    print("======================================================================")
    print("        POSTGRESQL CDC & DEBEZIUM WAL REPLICATION VERIFIER            ")
    print("======================================================================")

    # 1. Inspect Debezium Connect status
    deb_status = check_debezium_connect_status(connect_url=args.connect_url)
    print(f"Debezium Status: {deb_status['connector_state']} (Online: {deb_status['online']})")

    # 2. Inspect PostgreSQL CDC configuration
    pg_status = inspect_postgres_cdc(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        slot_name=args.slot,
        publication_name=args.publication
    )

    if pg_status:
        print("\n--- PostgreSQL Replication Configuration ---")
        print(f"Publication '{args.publication}': Exists={pg_status['publication']['exists']} | Tables={pg_status['publication']['tables']}")
        print(f"Replication Slot '{args.slot}': Exists={pg_status['slot']['exists']} | Plugin={pg_status['slot']['plugin']} | Active={pg_status['slot']['active']}")
        print(f"Current WAL LSN: {pg_status['current_wal_lsn']}")
        print(f"Confirmed Flush LSN: {pg_status['slot']['confirmed_flush_lsn']}")
        print(f"Replication Lag: {pg_status['lag_bytes']} bytes")
    else:
        print("\n[!] PostgreSQL is currently unreachable on localhost:5432.")
        print("    If running inside Docker, start with: ./scripts/bootstrap.sh")
        print("    For offline local testing without Docker, run: ./scripts/demo.sh")

    if args.monitor and pg_status:
        print("\n--- Starting Live WAL Progression Monitoring ---")
        monitor_wal_progress(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.dbname,
            slot_name=args.slot,
            duration_sec=args.monitor_duration
        )


if __name__ == "__main__":
    main()
