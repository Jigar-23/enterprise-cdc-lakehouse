"""
CDC Streaming Consumer:
Consumes Debezium changelog events from Kafka/Redpanda or simulated streams.
Features:
- Strict Pydantic contract validation (extra=forbid)
- Consistent entity routing with topic-source conflict rejection
- Bronze append-only storage preserving complete raw Debezium context (forensic replay)
- Silver ACID MERGE INTO with monotonic (LSN, ts_ms) ordering and tombstone deletion
- Discrete failure semantics:
    * Validation failures -> DLQ + manual Kafka offset commit
    * Storage / unexpected failures -> NO Kafka commit + retry / crash
"""

import os
import sys
import json
import uuid
import signal
import logging
import traceback
from typing import Dict, Any, Optional
from pydantic import ValidationError

from pipeline.models import CDCEnvelope, OrderPayload, InventoryPayload
from pipeline.delta_engine import DeltaLakehouseEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cdc_consumer")


class CDCPipelineConsumer:
    """
    Kafka / Event Stream consumer handling Debezium CDC records with Medallion Lakehouse routing.
    """

    def __init__(self, lakehouse_engine: Optional[DeltaLakehouseEngine] = None):
        self.engine = lakehouse_engine or DeltaLakehouseEngine()
        self.metrics = {
            "processed_events": 0,
            "bronze_appended": 0,
            "silver_upserts": 0,
            "quarantined_errors": 0,
        }
        self.running = True

    def process_raw_message(
        self,
        raw_data: str,
        topic_hint: str = "erp_cdc.public.orders",
        partition: Optional[int] = None,
        offset: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process a single raw message string (from Kafka or replay script).
        Implements distinct failure semantics:
        - Validation errors -> Quarantined to DLQ (non-blocking)
        - Storage errors -> Raised / unhandled (blocks offset commit to guarantee no data loss)
        """
        self.metrics["processed_events"] += 1
        part_num = partition if partition is not None else 0
        off_num = offset if offset is not None else 0
        event_id = f"{topic_hint}:{part_num}:{off_num}"

        # 1. Parse JSON payload
        try:
            parsed_json = json.loads(raw_data)
        except Exception as e:
            self.metrics["quarantined_errors"] += 1
            stack = traceback.format_exc()
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="JSON_DECODE_ERROR",
                error_message=str(e),
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                stack_trace=stack
            )
            return {"status": "quarantined", "reason": "JSON_DECODE_ERROR", "event_id": event_id}

        # Unwrap Debezium payload wrapper if present (Debezium schema-enabled JSON)
        payload_body = parsed_json.get("payload", parsed_json) if isinstance(parsed_json, dict) else parsed_json
        if not isinstance(payload_body, dict):
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="INVALID_PAYLOAD_STRUCTURE",
                error_message="Payload body must be a JSON object",
                topic=topic_hint,
                partition=part_num,
                offset=off_num
            )
            return {"status": "quarantined", "reason": "INVALID_PAYLOAD_STRUCTURE", "event_id": event_id}

        # 2. Validate CDC Envelope
        try:
            envelope = CDCEnvelope(**payload_body)
        except ValidationError as ve:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="ENVELOPE_VALIDATION_ERROR",
                error_message=str(ve),
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                stack_trace=traceback.format_exc()
            )
            return {"status": "quarantined", "reason": "ENVELOPE_VALIDATION_ERROR", "event_id": event_id}

        # 3. Entity derivation and topic-source conflict validation
        topic_lower = topic_hint.lower()
        topic_entity = "unknown"
        if "orders" in topic_lower:
            topic_entity = "orders"
        elif "inventory" in topic_lower:
            topic_entity = "inventory"

        source_entity = envelope.source.table.lower() if (envelope.source and envelope.source.table) else "unknown"

        # If both are known but conflict -> QUARANTINE (Never silently reinterpret!)
        if topic_entity != "unknown" and source_entity != "unknown" and topic_entity != source_entity:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="TOPIC_SOURCE_MISMATCH",
                error_message=f"Topic entity '{topic_entity}' conflicts with source table '{source_entity}'",
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                entity=source_entity,
                operation=envelope.op,
                source_table=source_entity
            )
            return {"status": "quarantined", "reason": "TOPIC_SOURCE_MISMATCH", "event_id": event_id}

        entity_type = source_entity if source_entity != "unknown" else topic_entity
        if entity_type not in ("orders", "inventory"):
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="UNKNOWN_ENTITY_TYPE",
                error_message=f"Cannot determine target entity for topic '{topic_hint}' and source '{source_entity}'",
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                operation=envelope.op
            )
            return {"status": "quarantined", "reason": "UNKNOWN_ENTITY_TYPE", "event_id": event_id}

        # 4. Validate domain schema contract (strict Pydantic)
        data_to_validate = envelope.after if envelope.op != "d" else (envelope.before or envelope.after)
        if not data_to_validate:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="EMPTY_RECORD_DATA",
                error_message=f"No data present for operation {envelope.op}",
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                entity=entity_type,
                operation=envelope.op,
                source_table=source_entity
            )
            return {"status": "quarantined", "reason": "EMPTY_RECORD_DATA", "event_id": event_id}

        try:
            if entity_type == "orders":
                if envelope.op != "d":
                    OrderPayload(**data_to_validate)
                elif "order_id" not in data_to_validate:
                    raise ValueError("Delete operation requires 'order_id' primary key")
                pk_val = str(data_to_validate.get("order_id"))
                pk = "order_id"
            elif entity_type == "inventory":
                if envelope.op != "d":
                    InventoryPayload(**data_to_validate)
                elif "sku" not in data_to_validate:
                    raise ValueError("Delete operation requires 'sku' primary key")
                pk_val = str(data_to_validate.get("sku"))
                pk = "sku"
            else:
                raise ValueError(f"Unsupported entity type: {entity_type}")
        except (ValidationError, ValueError) as ve:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="SCHEMA_CONTRACT_VIOLATION",
                error_message=str(ve),
                topic=topic_hint,
                partition=part_num,
                offset=off_num,
                key=str(data_to_validate.get("order_id") or data_to_validate.get("sku") or ""),
                entity=entity_type,
                operation=envelope.op,
                source_table=source_entity,
                stack_trace=traceback.format_exc()
            )
            return {"status": "quarantined", "reason": "SCHEMA_CONTRACT_VIOLATION", "event_id": event_id}

        # 5. Append to Bronze (Raw immutable audit log in Delta Lake preserving full context)
        raw_before_json = json.dumps(envelope.before) if envelope.before else None
        raw_after_json = json.dumps(envelope.after) if envelope.after else None
        raw_source_json = json.dumps(envelope.source.model_dump()) if envelope.source else None
        raw_tx_json = json.dumps(envelope.transaction) if envelope.transaction else None

        bronze_record = {
            "event_id": event_id,
            "source_topic": topic_hint,
            "partition": part_num,
            "offset": off_num,
            "key": pk_val,
            "op": envelope.op,
            "before": raw_before_json,
            "after": raw_after_json,
            "source": raw_source_json,
            "transaction": raw_tx_json,
            "ts_ms": envelope.ts_ms,
            "cdc_lsn": envelope.source.lsn_numeric if envelope.source else 0,
            "tx_id": envelope.source.txId if (envelope.source and envelope.source.txId) else 0,
        }

        # Durable write to Bronze - if this fails, exception propagates and offset is NOT committed
        self.engine.append_bronze(entity_name=entity_type, records=[bronze_record])
        self.metrics["bronze_appended"] += 1

        # 6. Merge into Silver (Native Delta Lake ACID Upsert with monotonic LSN ordering)
        silver_record = dict(data_to_validate)
        silver_record["_cdc_op"] = envelope.op
        silver_record["ts_ms"] = envelope.ts_ms
        silver_record["_lsn"] = envelope.source.lsn_numeric if envelope.source else 0

        # Durable write to Silver - if this fails, exception propagates and offset is NOT committed
        merge_result = self.engine.merge_silver(
            entity_name=entity_type,
            incoming_records=[silver_record],
            primary_key=pk,
            timestamp_col="ts_ms"
        )
        self.metrics["silver_upserts"] += 1

        return {
            "status": "success",
            "event_id": event_id,
            "entity": entity_type,
            "op": envelope.op,
            "merge_result": merge_result
        }

    def start_kafka_consumer(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "cdc_lakehouse_group",
        topic_prefix: Optional[str] = None
    ):
        """
        Starts live Kafka consumer loop consuming Debezium topics.
        Uses explicit manual commit (enable.auto.commit=False) to enforce at-least-once delivery.
        Only commits offsets after Bronze append and Silver merge succeed durably.
        """
        try:
            from confluent_kafka import Consumer, KafkaError
        except ImportError:
            logger.error("confluent-kafka not installed. Run 'pip install confluent-kafka'.")
            return

        broker = bootstrap_servers or os.getenv("KAFKA_BROKER", "localhost:9092")
        prefix = topic_prefix or os.getenv("KAFKA_TOPIC_PREFIX", "erp_cdc")

        conf = {
            "bootstrap.servers": broker,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # Strict manual offset commit
        }

        logger.info(f"Connecting consumer to Kafka broker {broker} with group {group_id}...")
        consumer = Consumer(conf)

        topics = [f"{prefix}.public.orders", f"{prefix}.public.inventory"]
        consumer.subscribe(topics)
        logger.info(f"Subscribed to Kafka topics: {topics}. Consuming CDC events with manual offset commit...")

        def shutdown_handler(signum, frame):
            logger.info("Shutdown signal received. Stopping consumer gracefully...")
            self.running = False

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            while self.running:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                raw_val = msg.value().decode("utf-8")
                res = self.process_raw_message(
                    raw_data=raw_val,
                    topic_hint=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset()
                )

                # Commit offset manually ONLY after durable write or DLQ quarantine
                if res["status"] in ("success", "quarantined"):
                    consumer.commit(message=msg, asynchronous=False)

        except Exception as e:
            # Fatal error (e.g. disk full, storage failure) -> Process exits WITHOUT committing offset
            logger.error(f"Fatal exception in consumer loop: {e}. Offset NOT committed.", exc_info=True)
            raise
        finally:
            logger.info("Closing Kafka consumer...")
            consumer.close()
            logger.info(f"Final Consumer Metrics: {self.metrics}")


if __name__ == "__main__":
    storage_dir = os.getenv("LAKEHOUSE_STORAGE_DIR", "./lakehouse_storage")
    engine = DeltaLakehouseEngine(base_storage_dir=storage_dir)
    pipeline_consumer = CDCPipelineConsumer(lakehouse_engine=engine)
    pipeline_consumer.start_kafka_consumer()
