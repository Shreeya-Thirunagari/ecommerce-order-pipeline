"""Pydantic models and schema definitions for order line items."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# Controlled vocabularies (mirrored in config.yaml for easy change)
ALLOWED_CURRENCIES = {"USD", "EUR", "GBP", "INR"}
ALLOWED_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled"}


class OrderLineItem(BaseModel):
    """
    Canonical schema for a single order line item.

    Primary key (business): (order_id, product_id)
    Grain: one row per product within an order.
    """

    order_id: str = Field(..., min_length=1, description="Unique order identifier")
    product_id: str = Field(..., min_length=1, description="Product SKU")
    customer_id: str = Field(..., min_length=1, description="Customer identifier")
    order_ts: datetime = Field(..., description="Order placement timestamp (UTC preferred)")
    quantity: int = Field(..., ge=1, description="Units ordered (>= 1)")
    unit_price: Decimal = Field(..., ge=0, description="Price per unit (>= 0)")
    currency: str = Field(..., description="ISO currency code")
    status: str = Field(..., description="Order status")

    # Derived / metadata (populated later)
    event_date: Optional[date] = Field(None, description="Derived from order_ts")
    source_file: Optional[str] = Field(None, description="Original source filename")
    ingestion_ts: Optional[datetime] = Field(None, description="When file was ingested")
    run_id: Optional[str] = Field(None, description="Pipeline run identifier")

    model_config = {
        "str_strip_whitespace": True,
        "extra": "ignore",  # ignore unexpected columns gracefully
    }

    @field_validator("currency")
    @classmethod
    def currency_must_be_allowed(cls, v: str) -> str:
        v = v.upper().strip()
        if v not in ALLOWED_CURRENCIES:
            raise ValueError(f"currency must be one of {sorted(ALLOWED_CURRENCIES)}, got '{v}'")
        return v

    @field_validator("status")
    @classmethod
    def status_must_be_allowed(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in ALLOWED_STATUSES:
            raise ValueError(f"status must be one of {sorted(ALLOWED_STATUSES)}, got '{v}'")
        return v

    @field_validator("order_id", "product_id", "customer_id")
    @classmethod
    def non_empty_string(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("must be a non-empty string")
        return v.strip()

    @model_validator(mode="after")
    def derive_event_date(self) -> "OrderLineItem":
        if self.order_ts is not None and self.event_date is None:
            # Use date portion of order_ts
            self.event_date = self.order_ts.date()
        return self


class RejectedRecord(BaseModel):
    """Schema for records that failed validation (quarantine)."""

    # Original raw values kept as strings / Any for maximum fidelity
    order_id: Optional[str] = None
    product_id: Optional[str] = None
    customer_id: Optional[str] = None
    order_ts: Optional[str] = None
    quantity: Optional[str] = None
    unit_price: Optional[str] = None
    currency: Optional[str] = None
    status: Optional[str] = None

    # Traceability
    source_file: str
    ingestion_ts: datetime
    run_id: str
    rejection_reason: str
    raw_row: Optional[dict] = None  # full original dict if available


# Expected columns in incoming CSV files (order does not matter)
REQUIRED_COLUMNS: List[str] = [
    "order_id",
    "product_id",
    "customer_id",
    "order_ts",
    "quantity",
    "unit_price",
    "currency",
    "status",
]
