"""Clean, deduplicate and write accepted records into the curated zone."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def prepare_for_curated(
    accepted_df: pd.DataFrame,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, int]:
    """
    Apply final cleaning and intra-batch deduplication.

    Returns:
        (cleaned_df, num_duplicates_removed)
    """
    if accepted_df.empty:
        return accepted_df, 0

    df = accepted_df.copy()

    # Ensure proper types
    df["order_ts"] = pd.to_datetime(df["order_ts"], utc=True)
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"])
    df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], utc=True)

    # Deduplication on business key (order_id, product_id)
    # Strategy: keep_latest by order_ts then ingestion_ts
    before = len(df)
    strategy = config.get("curation", {}).get("dedup_strategy", "keep_latest")

    if strategy == "keep_latest":
        df = df.sort_values(
            by=["order_id", "product_id", "order_ts", "ingestion_ts"],
            ascending=[True, True, False, False],
        )
        df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")
    else:
        # fallback: keep first occurrence
        df = df.drop_duplicates(subset=["order_id", "product_id"], keep="first")

    dups_removed = before - len(df)
    if dups_removed:
        logger.info("Removed %d intra-batch duplicates", dups_removed)

    # Drop helper columns that should not live in curated
    drop_cols = [c for c in ["_content_hash", "is_late"] if c in df.columns]
    df = df.drop(columns=drop_cols, errors="ignore")

    # Stable column order
    preferred = [
        "order_id",
        "product_id",
        "customer_id",
        "order_ts",
        "event_date",
        "quantity",
        "unit_price",
        "currency",
        "status",
        "source_file",
        "ingestion_ts",
        "run_id",
    ]
    cols = [c for c in preferred if c in df.columns] + [
        c for c in df.columns if c not in preferred
    ]
    df = df[cols]

    return df, dups_removed


def write_curated_partitioned(
    df: pd.DataFrame,
    curated_dir: Path,
    partition_col: str = "event_date",
) -> List[Path]:
    """
    Write curated data partitioned by event_date (Hive-style folders).

    Uses CSV for maximum portability (no pyarrow required).
    Returns list of written partition paths.
    """
    if df.empty:
        logger.info("No records to write to curated zone")
        return []

    curated_dir.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    # Group by partition value
    for part_value, group in df.groupby(partition_col, sort=True):
        # Hive-style: event_date=YYYY-MM-DD/
        part_str = str(part_value)
        part_dir = curated_dir / f"{partition_col}={part_str}"
        part_dir.mkdir(parents=True, exist_ok=True)

        # Deterministic name per partition so re-runs merge cleanly
        out_file = part_dir / f"orders_{part_str}.csv"

        # If file already exists we append & re-dedup (safe replay)
        if out_file.exists():
            existing = pd.read_csv(out_file)
            # Normalise types for concat
            for col in ("order_ts", "ingestion_ts"):
                if col in existing.columns:
                    existing[col] = pd.to_datetime(existing[col], utc=True)
                if col in group.columns:
                    group[col] = pd.to_datetime(group[col], utc=True)
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.sort_values(
                by=["order_id", "product_id", "order_ts", "ingestion_ts"],
                ascending=[True, True, False, False],
            )
            combined = combined.drop_duplicates(
                subset=["order_id", "product_id"], keep="first"
            )
            combined.to_csv(out_file, index=False)
            logger.info(
                "Updated partition %s (now %d rows)", part_str, len(combined)
            )
        else:
            group.to_csv(out_file, index=False)
            logger.info("Wrote new partition %s (%d rows)", part_str, len(group))

        written.append(out_file)

    return written


def write_validated(
    accepted_df: pd.DataFrame,
    validated_dir: Path,
    source_stem: str,
    run_id: str,
) -> Optional[Path]:
    """Write the accepted (validated) records as a single CSV for auditability."""
    if accepted_df.empty:
        return None
    validated_dir.mkdir(parents=True, exist_ok=True)
    out = validated_dir / f"{source_stem}__{run_id}_validated.csv"
    accepted_df.to_csv(out, index=False)
    logger.info("Wrote validated file %s (%d rows)", out.name, len(accepted_df))
    return out


def write_quarantine(
    rejected_df: pd.DataFrame,
    quarantine_dir: Path,
    source_stem: str,
    run_id: str,
) -> Optional[Path]:
    """Write rejected records with reasons to the quarantine zone."""
    if rejected_df.empty:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    out = quarantine_dir / f"{source_stem}__{run_id}_rejected.csv"
    rejected_df.to_csv(out, index=False)
    logger.info("Wrote quarantine file %s (%d rows)", out.name, len(rejected_df))
    return out
