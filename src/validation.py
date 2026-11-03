"""Schema + business-rule validation. Good rows continue; bad rows go to quarantine."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pydantic import ValidationError

from src.schema import ALLOWED_CURRENCIES, ALLOWED_STATUSES, OrderLineItem, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)


def _row_to_dict(row: pd.Series) -> Dict[str, Any]:
    """Convert a pandas Series to a plain dict suitable for Pydantic."""
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}


def validate_dataframe(
    df: pd.DataFrame,
    source_file: str,
    run_id: str,
    ingestion_ts: datetime,
    config: Dict[str, Any],
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """
    Validate every row against the OrderLineItem schema and business rules.

    Returns:
        (accepted_df, rejected_df, rejection_reason_counts)
    """
    val_cfg = config.get("validation", {})
    max_future_days = val_cfg.get("max_future_days", 1)
    late_days = val_cfg.get("late_data_days", 30)

    now = datetime.now(timezone.utc)
    future_cutoff = now + timedelta(days=max_future_days)
    late_cutoff = now - timedelta(days=late_days)

    accepted_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    reason_counts: Dict[str, int] = {}

    # Quick structural check
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_cols:
        # Entire file is rejected if required columns are missing
        reason = f"missing_required_columns:{','.join(missing_cols)}"
        reason_counts[reason] = len(df)
        for _, row in df.iterrows():
            rej = _build_reject_row(row, source_file, run_id, ingestion_ts, reason)
            rejected_rows.append(rej)
        rejected_df = pd.DataFrame(rejected_rows)
        return pd.DataFrame(), rejected_df, reason_counts

    for idx, row in df.iterrows():
        raw = _row_to_dict(row)
        # Drop technical / metadata columns before validation
        payload = {k: raw.get(k) for k in REQUIRED_COLUMNS}

        try:
            # Coerce common types before Pydantic
            if payload.get("quantity") is not None:
                payload["quantity"] = int(float(str(payload["quantity"]).strip()))
            if payload.get("unit_price") is not None:
                payload["unit_price"] = str(payload["unit_price"]).strip()

            item = OrderLineItem(**payload)

            # Additional temporal business rules
            order_ts = item.order_ts
            if order_ts.tzinfo is None:
                # Assume UTC if naive
                order_ts = order_ts.replace(tzinfo=timezone.utc)
                item.order_ts = order_ts

            if order_ts > future_cutoff:
                raise ValueError(
                    f"order_ts is too far in the future (>{max_future_days} days)"
                )

            is_late = order_ts < late_cutoff

            # Enrich with metadata
            record = item.model_dump()
            record["source_file"] = source_file
            record["ingestion_ts"] = ingestion_ts
            record["run_id"] = run_id
            record["is_late"] = is_late
            accepted_rows.append(record)

        except (ValidationError, ValueError, TypeError, KeyError) as exc:
            reason = _extract_reason(exc)
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            rej = _build_reject_row(row, source_file, run_id, ingestion_ts, reason)
            rejected_rows.append(rej)

    accepted_df = pd.DataFrame(accepted_rows) if accepted_rows else pd.DataFrame()
    rejected_df = pd.DataFrame(rejected_rows) if rejected_rows else pd.DataFrame()

    logger.info(
        "Validation for %s: accepted=%d, rejected=%d",
        source_file,
        len(accepted_df),
        len(rejected_df),
    )
    return accepted_df, rejected_df, reason_counts


def _extract_reason(exc: Exception) -> str:
    """Produce a short, stable rejection reason string."""
    if isinstance(exc, ValidationError):
        # Take the first error message and clean it
        errs = exc.errors()
        if errs:
            loc = ".".join(str(x) for x in errs[0].get("loc", []))
            msg = errs[0].get("msg", str(exc))
            return f"{loc}:{msg}"[:200]
        return "pydantic_validation_error"
    return str(exc)[:200]


def _build_reject_row(
    row: pd.Series,
    source_file: str,
    run_id: str,
    ingestion_ts: datetime,
    reason: str,
) -> Dict[str, Any]:
    """Build a quarantine record preserving original values as strings."""
    out: Dict[str, Any] = {
        "source_file": source_file,
        "run_id": run_id,
        "ingestion_ts": ingestion_ts.isoformat(),
        "rejection_reason": reason,
    }
    for col in REQUIRED_COLUMNS:
        val = row.get(col)
        out[col] = None if pd.isna(val) else str(val)
    # Keep any extra columns for debugging
    for col in row.index:
        if col not in out and not col.startswith("_"):
            val = row.get(col)
            out[f"raw_{col}"] = None if pd.isna(val) else str(val)
    return out
