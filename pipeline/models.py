"""
Strict data validation models for PostgreSQL Debezium CDC events using Pydantic V2.
Enforces explicit extra-forbid domain contracts, strong type checking,
hex WAL LSN parsing, and structured DLQ quarantine records.
"""

import re
from typing import Optional, Dict, Any, Literal, Union
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict


class SourceInfo(BaseModel):
    """Debezium WAL source metadata from PostgreSQL connector."""
    model_config = ConfigDict(extra="ignore")

    version: Optional[str] = None
    connector: Optional[str] = "postgresql"
    name: Optional[str] = None
    ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    snapshot: Optional[Union[str, bool]] = "false"
    db: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    table: Optional[str] = None
    txId: Optional[int] = None
    lsn: Optional[Union[int, str]] = None
    sequence: Optional[str] = None

    @field_validator("ts_ms")
    @classmethod
    def validate_source_ts(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Source ts_ms must be a positive unix timestamp in milliseconds")
        return v

    @property
    def lsn_numeric(self) -> int:
        """Returns 64-bit numeric LSN for monotonic ordering, parsing WAL hex format (e.g. '0/16B2D40')."""
        if self.lsn is None:
            return 0
        if isinstance(self.lsn, int):
            return self.lsn
        try:
            val_str = str(self.lsn).strip()
            if "/" in val_str:
                parts = val_str.split("/")
                return (int(parts[0], 16) << 32) + int(parts[1], 16)
            return int(val_str)
        except Exception:
            raise ValueError(f"Malformed LSN string cannot be parsed: '{self.lsn}'")


class OrderPayload(BaseModel):
    """Strict schema contract for Enterprise Order entity. Extra fields are forbidden."""
    model_config = ConfigDict(extra="forbid", strict=False)

    order_id: str = Field(..., min_length=1, description="Primary business key")
    customer_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0, description="Order quantity must be positive")
    total_price: float = Field(..., ge=0.0, description="Order total price cannot be negative")
    status: str = Field(default="PENDING")
    created_at: Optional[Union[str, datetime]] = None
    updated_at: Optional[Union[str, datetime]] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        valid_statuses = {"PENDING", "PROCESSING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"}
        v_upper = str(v).upper()
        if v_upper not in valid_statuses:
            raise ValueError(f"Invalid order status '{v}'. Allowed: {sorted(list(valid_statuses))}")
        return v_upper


class InventoryPayload(BaseModel):
    """Strict schema contract for Warehouse Inventory entity. Extra fields are forbidden."""
    model_config = ConfigDict(extra="forbid", strict=False)

    sku: str = Field(..., min_length=1, description="Stock Keeping Unit primary key")
    warehouse_id: str = Field(..., min_length=1)
    available_stock: int = Field(..., ge=0, description="Available inventory cannot be negative")
    reserved_stock: int = Field(default=0, ge=0, description="Reserved inventory cannot be negative")
    last_restocked_at: Optional[Union[str, datetime]] = None


class CDCEnvelope(BaseModel):
    """
    Standard Debezium Change Data Capture (CDC) envelope representation.
    Supported operations:
      - 'r': Read (Initial snapshot)
      - 'c': Create (INSERT)
      - 'u': Update (UPDATE)
      - 'd': Delete (DELETE)
    """
    model_config = ConfigDict(extra="forbid")

    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    source: Optional[SourceInfo] = None
    op: Literal["r", "c", "u", "d"] = Field(..., description="CDC operation type")
    ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    transaction: Optional[Dict[str, Any]] = None

    @field_validator("ts_ms")
    @classmethod
    def validate_envelope_ts(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("Envelope ts_ms must be a positive unix timestamp in milliseconds")
        return v

    @model_validator(mode="after")
    def validate_envelope_structure(self) -> "CDCEnvelope":
        if self.source is None:
            raise ValueError("CDC Envelope requires 'source' metadata block.")
        if self.source.table and self.source.table.lower() not in ("orders", "inventory"):
            raise ValueError(f"Invalid source table '{self.source.table}'. Supported: ['orders', 'inventory']")

        if self.op in ("c", "r") and not self.after:
            raise ValueError(f"Operation '{self.op}' requires a non-null 'after' payload.")
        if self.op == "u" and not self.after:
            raise ValueError("Update operation 'u' requires 'after' payload.")
        if self.op == "d" and not self.before and not self.after:
            raise ValueError("Delete operation 'd' requires 'before' or 'after' payload.")
        return self


class QuarantineRecord(BaseModel):
    """Canonical model for rejected payloads routed to Dead Letter Queue (DLQ)."""
    model_config = ConfigDict(extra="forbid")

    event_id: str
    raw_payload: str
    error_type: str
    error_message: str
    topic: str
    partition: int
    offset: int
    key: Optional[str] = None
    entity: Optional[str] = None
    operation: Optional[str] = None
    source_table: Optional[str] = None
    stack_trace: Optional[str] = None
    quarantined_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
