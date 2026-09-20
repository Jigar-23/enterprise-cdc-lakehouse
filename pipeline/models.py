"""
Data validation models for PostgreSQL Debezium CDC events using Pydantic V2.
Encapsulates strong typing, field coercion, and poison-pill detection.
"""

from typing import Optional, Dict, Any, Literal
from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator, model_validator


class SourceInfo(BaseModel):
    """Debezium WAL source metadata."""
    version: Optional[str] = None
    connector: Optional[str] = "postgresql"
    name: Optional[str] = None
    ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    snapshot: Optional[str] = "false"
    db: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    table: Optional[str] = None
    txId: Optional[int] = None
    lsn: Optional[int] = None


class OrderPayload(BaseModel):
    """Schema contract for Enterprise Order entity."""
    order_id: str = Field(..., min_length=1, description="Primary business key")
    customer_id: str = Field(..., min_length=1)
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0, description="Order quantity must be positive")
    total_price: float = Field(..., ge=0.0, description="Order total price cannot be negative")
    status: str = Field(default="PENDING")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        valid_statuses = {"PENDING", "PROCESSING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"}
        v_upper = v.upper()
        if v_upper not in valid_statuses:
            raise ValueError(f"Invalid order status '{v}'. Allowed: {valid_statuses}")
        return v_upper


class InventoryPayload(BaseModel):
    """Schema contract for Warehouse Inventory entity."""
    sku: str = Field(..., min_length=1, description="Stock Keeping Unit primary key")
    warehouse_id: str = Field(..., min_length=1)
    available_stock: int = Field(..., ge=0, description="Available inventory cannot be negative")
    reserved_stock: int = Field(default=0, ge=0, description="Reserved inventory cannot be negative")
    last_restocked_at: Optional[datetime] = None


class CDCEnvelope(BaseModel):
    """
    Standard Debezium Change Data Capture (CDC) envelope representation.
    Supported operations:
      - 'r': Read (Initial snapshot)
      - 'c': Create (INSERT)
      - 'u': Update (UPDATE)
      - 'd': Delete (DELETE)
    """
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    source: Optional[SourceInfo] = None
    op: Literal["r", "c", "u", "d"] = Field(..., description="CDC operation type")
    ts_ms: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
    transaction: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_payload_presence(self) -> "CDCEnvelope":
        if self.op in ("c", "r") and not self.after:
            raise ValueError(f"Operation '{self.op}' requires a non-null 'after' payload.")
        if self.op == "d" and not self.before and not self.after:
            raise ValueError("Delete operation 'd' requires 'before' or 'after' payload with primary key.")
        if self.op == "u" and not self.after:
            raise ValueError("Update operation 'u' requires 'after' payload.")
        return self


class QuarantineRecord(BaseModel):
    """Model for payloads rejected by schema validation or parsing (DLQ)."""
    raw_payload: str
    error_type: str
    error_message: str
    source_topic: Optional[str] = None
    quarantined_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
