"""
CDC Streaming Consumer:
Consumes Debezium changelog events from Kafka or simulated streams,
validates schemas via Pydantic, appends to Bronze storage,
executes ACID upsert into Silver, and isolates bad records to DLQ.
"""

import json
import logging
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

    def process_raw_message(self, raw_data: str, topic_hint: str = "erp_pg.public.orders") -> Dict[str, Any]:
        """
        Process a single raw message string (from Kafka or replay script).
        Guarantees isolation: malformed records are routed to DLQ without raising unhandled exceptions.
        """
        self.metrics["processed_events"] += 1

        # 1. Parse JSON payload
        try:
            parsed_json = json.loads(raw_data)
        except Exception as e:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="JSON_DECODE_ERROR",
                error_message=str(e),
                source_topic=topic_hint
            )
            return {"status": "quarantined", "reason": "JSON_DECODE_ERROR"}

        # Unwrap Debezium payload wrapper if present (Debezium schema-enabled JSON)
        payload_body = parsed_json.get("payload", parsed_json)

        # 2. Validate CDC Envelope
        try:
            envelope = CDCEnvelope(**payload_body)
        except ValidationError as ve:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="ENVELOPE_VALIDATION_ERROR",
                error_message=str(ve),
                source_topic=topic_hint
            )
            return {"status": "quarantined", "reason": "ENVELOPE_VALIDATION_ERROR"}

        # 3. Identify target entity (orders vs inventory)
        entity_type = "unknown"
        if "orders" in topic_hint or (envelope.source and envelope.source.table == "orders"):
            entity_type = "orders"
        elif "inventory" in topic_hint or (envelope.source and envelope.source.table == "inventory"):
            entity_type = "inventory"
        else:
            # Infer from payload keys
            row = envelope.after or envelope.before or {}
            if "order_id" in row:
                entity_type = "orders"
            elif "sku" in row:
                entity_type = "inventory"

        # 4. Validate domain schema
        data_to_validate = envelope.after if envelope.op != "d" else envelope.before
        if not data_to_validate:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="EMPTY_RECORD_DATA",
                error_message=f"No data present for operation {envelope.op}",
                source_topic=topic_hint
            )
            return {"status": "quarantined", "reason": "EMPTY_RECORD_DATA"}

        try:
            if entity_type == "orders":
                OrderPayload(**data_to_validate)
                pk = "order_id"
            elif entity_type == "inventory":
                InventoryPayload(**data_to_validate)
                pk = "sku"
            else:
                raise ValueError(f"Unknown entity type for topic: {topic_hint}")
        except (ValidationError, ValueError) as ve:
            self.metrics["quarantined_errors"] += 1
            self.engine.route_to_quarantine(
                raw_payload=raw_data,
                error_type="SCHEMA_CONTRACT_VIOLATION",
                error_message=str(ve),
                source_topic=topic_hint
            )
            return {"status": "quarantined", "reason": "SCHEMA_CONTRACT_VIOLATION"}

        # 5. Append to Bronze (Raw immutable audit log)
        bronze_record = dict(data_to_validate)
        bronze_record["_cdc_op"] = envelope.op
        bronze_record["_cdc_ts_ms"] = envelope.ts_ms
        if envelope.source:
            bronze_record["_tx_id"] = envelope.source.txId
            bronze_record["_lsn"] = envelope.source.lsn

        self.engine.append_bronze(entity_name=entity_type, records=[bronze_record])
        self.metrics["bronze_appended"] += 1

        # 6. Merge into Silver (ACID Upsert)
        silver_record = dict(data_to_validate)
        silver_record["_cdc_op"] = envelope.op
        silver_record["ts_ms"] = envelope.ts_ms

        merge_result = self.engine.merge_silver(
            entity_name=entity_type,
            incoming_records=[silver_record],
            primary_key=pk,
            timestamp_col="ts_ms"
        )
        self.metrics["silver_upserts"] += 1

        return {
            "status": "success",
            "entity": entity_type,
            "op": envelope.op,
            "merge_result": merge_result
        }

    def start_kafka_consumer(self, bootstrap_servers: str, group_id: str = "cdc_lakehouse_group"):
        """
        Starts live Kafka consumer loop consuming Debezium topics.
        """
        try:
            from confluent_kafka import Consumer, KafkaException
        except ImportError:
            logger.error("confluent-kafka not installed. Run 'pip install confluent-kafka'.")
            return

        conf = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        }

        consumer = Consumer(conf)
        topics = ["erp_pg.public.orders", "erp_pg.public.inventory"]
        consumer.subscribe(topics)
        logger.info(f"Subscribed to Kafka topics: {topics}. Consuming CDC events...")

        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                raw_val = msg.value().decode("utf-8")
                self.process_raw_message(raw_val, topic_hint=msg.topic())
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user.")
        finally:
            consumer.close()
