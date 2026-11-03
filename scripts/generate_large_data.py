#!/usr/bin/env python3
"""
Generate a larger synthetic dataset (default 100k+ rows) for performance checks.

Usage:
    python scripts/generate_large_data.py
    python scripts/generate_large_data.py --rows 250000 --out data/sample/orders_large.csv

Fixed seed for reproducibility.
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SEED = 42


def generate(n_rows: int, seed: int = SEED) -> pd.DataFrame:
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    currencies = ["USD", "EUR", "GBP", "INR"]

    # Pre-allocate lists for speed
    order_ids = []
    product_ids = []
    customer_ids = []
    order_ts = []
    quantities = []
    unit_prices = []
    currs = []
    stats = []

    # ~2.2 line items per order on average
    order_counter = 100000
    rows_generated = 0
    while rows_generated < n_rows:
        order_id = f"ORD-{order_counter}"
        n_items = rng.randint(1, 4)
        for _ in range(n_items):
            if rows_generated >= n_rows:
                break
            order_ids.append(order_id)
            product_ids.append(f"PRD-{rng.randint(1, 500)}")
            customer_ids.append(f"CUST-{rng.randint(1, 5000)}")
            # Spread over ~300 days
            ts = base + timedelta(
                days=rng.randint(0, 300),
                hours=rng.randint(0, 23),
                minutes=rng.randint(0, 59),
            )
            order_ts.append(ts.isoformat())
            quantities.append(rng.randint(1, 10))
            unit_prices.append(round(rng.uniform(1.0, 500.0), 2))
            currs.append(rng.choice(currencies))
            stats.append(rng.choice(statuses))
            rows_generated += 1
        order_counter += 1

    return pd.DataFrame(
        {
            "order_id": order_ids,
            "product_id": product_ids,
            "customer_id": customer_ids,
            "order_ts": order_ts,
            "quantity": quantities,
            "unit_price": unit_prices,
            "currency": currs,
            "status": stats,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate large synthetic order data")
    parser.add_argument("--rows", type=int, default=120_000, help="Target number of rows")
    parser.add_argument(
        "--out",
        type=str,
        default=str(ROOT / "data" / "sample" / "orders_large.csv"),
        help="Output CSV path",
    )
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.rows:,} rows (seed={args.seed}) ...")
    df = generate(args.rows, seed=args.seed)
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(df):,} rows to {out_path}")
    print(f"File size: {out_path.stat().st_size / 1_048_576:.2f} MB")


if __name__ == "__main__":
    main()
