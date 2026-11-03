"""
Main pipeline orchestration.

Usage (from project root):
    python -m src.pipeline
    python -m src.pipeline --incoming data/sample
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.ingestion import (
    is_already_processed,
    land_file_to_raw,
    list_incoming_files,
    mark_processed,
)
from src.metrics import RunMetrics
from src.transformation import (
    prepare_for_curated,
    write_curated_partitioned,
    write_quarantine,
    write_validated,
)
from src.utils import (
    ensure_dirs,
    generate_run_id,
    get_project_root,
    load_config,
    setup_logging,
    utc_now,
)
from src.validation import validate_dataframe

logger = logging.getLogger(__name__)


def run_pipeline(
    incoming_dir: Optional[Path] = None,
    config_path: str = "config.yaml",
    force: bool = False,
) -> RunMetrics:
    """
    Execute a full pipeline run.

    Parameters
    ----------
    incoming_dir:
        Directory containing source CSV files. Defaults to data/sample.
    config_path:
        Path to config.yaml.
    force:
        If True, ignore the processed-file manifest (useful for testing).
    """
    config = load_config(config_path)
    root = get_project_root()
    ensure_dirs(config)

    paths = config["paths"]
    raw_dir = root / paths["raw"]
    validated_dir = root / paths["validated"]
    curated_dir = root / paths["curated"]
    quarantine_dir = root / paths["quarantine"]
    state_dir = root / paths.get("state", "data/state")
    logs_dir = root / paths.get("logs", "data/logs")

    setup_logging(logs_dir)

    run_id = generate_run_id()
    metrics = RunMetrics(run_id=run_id)

    if incoming_dir is None:
        incoming_dir = root / paths.get("sample", "data/sample")
    incoming_dir = Path(incoming_dir)

    logger.info("=== Pipeline run started | run_id=%s ===", run_id)
    logger.info("Incoming directory: %s", incoming_dir)

    try:
        files = list_incoming_files(
            incoming_dir,
            supported_extensions=config.get("ingestion", {}).get(
                "supported_extensions", [".csv"]
            ),
        )
        metrics.input_files = len(files)

        if not files:
            logger.warning("No input files found in %s", incoming_dir)
            metrics.finish(status="success")
            metrics.log_summary()
            if config.get("pipeline", {}).get("write_metrics", True):
                metrics.persist(logs_dir)
            return metrics

        all_accepted: List[pd.DataFrame] = []
        total_rejected = 0
        total_late = 0
        total_dups = 0

        for src_path in files:
            content_hash = None
            try:
                # --- Idempotency check ---
                from src.utils import file_content_hash

                content_hash = file_content_hash(src_path)
                if (
                    not force
                    and config.get("pipeline", {}).get("enforce_idempotency", True)
                    and is_already_processed(content_hash, state_dir)
                ):
                    logger.info(
                        "Skipping already-processed file %s (hash=%s...)",
                        src_path.name,
                        content_hash[:12],
                    )
                    metrics.skipped_files.append(src_path.name)
                    continue

                # --- Land to raw ---
                raw_path, raw_df, content_hash = land_file_to_raw(
                    src_path,
                    raw_dir,
                    run_id=run_id,
                    encoding=config.get("ingestion", {}).get("encoding", "utf-8"),
                )
                metrics.input_records += len(raw_df)
                metrics.processed_files.append(src_path.name)

                # --- Validate ---
                ingestion_ts = utc_now()  # approximate; real value already in df
                # Prefer the value written into the dataframe
                if "ingestion_ts" in raw_df.columns and len(raw_df) > 0:
                    try:
                        ingestion_ts = pd.to_datetime(
                            raw_df["ingestion_ts"].iloc[0], utc=True
                        ).to_pydatetime()
                    except Exception:
                        pass

                accepted_df, rejected_df, reason_counts = validate_dataframe(
                    raw_df,
                    source_file=src_path.name,
                    run_id=run_id,
                    ingestion_ts=ingestion_ts,
                    config=config,
                )

                for reason, cnt in reason_counts.items():
                    metrics.rejection_reasons[reason] = (
                        metrics.rejection_reasons.get(reason, 0) + cnt
                    )

                total_rejected += len(rejected_df)
                if "is_late" in accepted_df.columns:
                    total_late += int(accepted_df["is_late"].sum())

                # --- Write validated & quarantine ---
                write_validated(
                    accepted_df, validated_dir, src_path.stem, run_id
                )
                write_quarantine(
                    rejected_df, quarantine_dir, src_path.stem, run_id
                )

                if not accepted_df.empty:
                    all_accepted.append(accepted_df)

                # Mark as processed only after successful validation write
                if config.get("pipeline", {}).get("enforce_idempotency", True):
                    mark_processed(
                        content_hash,
                        src_path.name,
                        run_id,
                        state_dir,
                        record_count=len(raw_df),
                    )

            except Exception as exc:
                logger.exception("Failed processing file %s: %s", src_path.name, exc)
                metrics.failure_reason = str(exc)
                # Continue with remaining files (partial success)

        # --- Curate (union of all accepted in this run) ---
        if all_accepted:
            combined = pd.concat(all_accepted, ignore_index=True)
            cleaned, dups = prepare_for_curated(combined, config)
            total_dups += dups
            metrics.accepted_records = len(cleaned)
            metrics.duplicate_records = total_dups
            metrics.late_records = total_late
            metrics.rejected_records = total_rejected

            write_curated_partitioned(
                cleaned,
                curated_dir,
                partition_col=config.get("curation", {}).get(
                    "partition_col", "event_date"
                ),
            )
        else:
            metrics.accepted_records = 0
            metrics.rejected_records = total_rejected

        metrics.finish(status="success" if not metrics.failure_reason else "partial")

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        metrics.finish(status="failed", failure_reason=str(exc))

    metrics.log_summary()
    if config.get("pipeline", {}).get("write_metrics", True):
        metrics.persist(logs_dir)
    return metrics


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Yottaflex local-first order data pipeline"
    )
    parser.add_argument(
        "--incoming",
        type=str,
        default=None,
        help="Directory containing source CSV files (default: data/sample)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore processed-file manifest and reprocess everything",
    )
    args = parser.parse_args(argv)

    metrics = run_pipeline(
        incoming_dir=Path(args.incoming) if args.incoming else None,
        config_path=args.config,
        force=args.force,
    )
    return 0 if metrics.status in ("success", "partial") else 1


if __name__ == "__main__":
    sys.exit(main())
