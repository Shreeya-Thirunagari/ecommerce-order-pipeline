#!/usr/bin/env python3
"""
Generate a small, reproducible sample dataset with both clean and dirty records.

Produces files under data/sample/:
  - orders_clean.csv          (happy-path records)
  - orders_with_issues.csv    (nulls, bad types, invalid status, future dates, dups)
  - orders_late.csv           (intentionally late event_dates)

Fixed random seed = 42 for full reproducibility.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Project root = parent of scripts/
ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DIR = ROOT / "data" / "sample"
SEED = 42


def _ts(base: datetime, offset_hours: int = 0) -> str:
    return (base + timedelta(hours=offset_hours)).isoformat()


def generate_clean(n: int = 20) -> pd.DataFrame:
    rng = random.Random(SEED)
    base = datetime(2025, 10, 15, 10, 0, 0, tzinfo=timezone.utc)
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    currencies = ["USD", "EUR", "GBP", "INR"]

    rows = []
    for i in range(n):
        order_id = f"ORD-{10000 + i}"
        # 1-3 line items per order
        for j in range(rng.randint(1, 3)):
            rows.append(
                {
                    "order_id": order_id,
                    "product_id": f"PRD-{rng.randint(1, 50)}",
                    "customer_id": f"CUST-{rng.randint(1, 200)}",
                    "order_ts": _ts(base, offset_hours=rng.randint(0, 72)),
                    "quantity": rng.randint(1, 5),
                    "unit_price": round(rng.uniform(5.0, 200.0), 2),
                    "currency": rng.choice(currencies),
                    "status": rng.choice(statuses),
                }
            )
    return pd.DataFrame(rows)


def generate_with_issues() -> pd.DataFrame:
    """Mix of good rows + intentional problems."""
    rng = random.Random(SEED + 1)
    base = datetime(2025, 10, 16, 12, 0, 0, tzinfo=timezone.utc)

    rows = [
        # Good row
        {
            "order_id": "ORD-20001",
            "product_id": "PRD-10",
            "customer_id": "CUST-50",
            "order_ts": _ts(base),
            "quantity": 2,
            "unit_price": 29.99,
            "currency": "USD",
            "status": "confirmed",
        },
        # Null quantity
        {
            "order_id": "ORD-20002",
            "product_id": "PRD-11",
            "customer_id": "CUST-51",
            "order_ts": _ts(base, 1),
            "quantity": None,
            "unit_price": 15.00,
            "currency": "EUR",
            "status": "pending",
        },
        # Negative quantity
        {
            "order_id": "ORD-20003",
            "product_id": "PRD-12",
            "customer_id": "CUST-52",
            "order_ts": _ts(base, 2),
            "quantity": -1,
            "unit_price": 40.00,
            "currency": "USD",
            "status": "shipped",
        },
        # Invalid status
        {
            "order_id": "ORD-20004",
            "product_id": "PRD-13",
            "customer_id": "CUST-53",
            "order_ts": _ts(base, 3),
            "quantity": 1,
            "unit_price": 99.99,
            "currency": "GBP",
            "status": "refunded",  # not in allowed list
        },
        # Invalid currency
        {
            "order_id": "ORD-20005",
            "product_id": "PRD-14",
            "customer_id": "CUST-54",
            "order_ts": _ts(base, 4),
            "quantity": 3,
            "unit_price": 12.50,
            "currency": "XYZ",
            "status": "delivered",
        },
        # Future order_ts (beyond tolerance)
        {
            "order_id": "ORD-20006",
            "product_id": "PRD-15",
            "customer_id": "CUST-55",
            "order_ts": (
                datetime.now(timezone.utc) + timedelta(days=10)
            ).isoformat(),
            "quantity": 1,
            "unit_price": 5.00,
            "currency": "INR",
            "status": "pending",
        },
        # Duplicate key of first good row (will be deduped later)
        {
            "order_id": "ORD-20001",
            "product_id": "PRD-10",
            "customer_id": "CUST-50",
            "order_ts": _ts(base, 5),  # later timestamp
            "quantity": 4,  # different quantity – keep_latest should keep this
            "unit_price": 29.99,
            "currency": "USD",
            "status": "shipped",
        },
        # Empty order_id
        {
            "order_id": "",
            "product_id": "PRD-16",
            "customer_id": "CUST-56",
            "order_ts": _ts(base, 6),
            "quantity": 1,
            "unit_price": 8.00,
            "currency": "USD",
            "status": "confirmed",
        },
        # Bad type for unit_price
        {
            "order_id": "ORD-20007",
            "product_id": "PRD-17",
            "customer_id": "CUST-57",
            "order_ts": _ts(base, 7),
            "quantity": 2,
            "unit_price": "free",
            "currency": "EUR",
            "status": "pending",
        },
    ]
    return pd.DataFrame(rows)


def generate_late() -> pd.DataFrame:
    """Records whose event_date is intentionally old (late-arriving)."""
    base = datetime(2024, 6, 1, 8, 0, 0, tzinfo=timezone.utc)  # ~1.5 years ago
    rows = []
    for i in range(5):
        rows.append(
            {
                "order_id": f"ORD-LATE-{i+1}",
                "product_id": f"PRD-{20 + i}",
                "customer_id": f"CUST-{100 + i}",
                "order_ts": _ts(base, offset_hours=i * 24),
                "quantity": 1 + i,
                "unit_price": round(10.0 + i * 3.5, 2),
                "currency": "USD",
                "status": "delivered",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    clean = generate_clean(20)
    issues = generate_with_issues()
    late = generate_late()

    clean.to_csv(SAMPLE_DIR / "orders_clean.csv", index=False)
    issues.to_csv(SAMPLE_DIR / "orders_with_issues.csv", index=False)
    late.to_csv(SAMPLE_DIR / "orders_late.csv", index=False)

    print(f"Wrote sample files to {SAMPLE_DIR}")
    print(f"  orders_clean.csv          : {len(clean)} rows")
    print(f"  orders_with_issues.csv    : {len(issues)} rows")
    print(f"  orders_late.csv           : {len(late)} rows")


if __name__ == "__main__":
    main()
