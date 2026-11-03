"""Happy-path and basic integration tests."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.ingestion import land_file_to_raw, list_incoming_files
from src.pipeline import run_pipeline
from src.schema import OrderLineItem
from src.transformation import prepare_for_curated, write_curated_partitioned
from src.utils import generate_run_id, load_config, utc_now
from src.validation import validate_dataframe


def test_schema_accepts_valid_record():
    item = OrderLineItem(
        order_id="ORD-1",
        product_id="PRD-1",
        customer_id="CUST-1",
        order_ts="2025-10-15T10:00:00Z",
        quantity=2,
        unit_price="19.99",
        currency="usd",  # lower-case should be normalised
        status="Confirmed",  # mixed case
    )
    assert item.currency == "USD"
    assert item.status == "confirmed"
    assert item.event_date is not None
    assert item.quantity == 2


def test_land_and_validate_clean(sample_clean_csv, tmp_path):
    raw_dir = tmp_path / "raw"
    run_id = generate_run_id()
    raw_path, df, content_hash = land_file_to_raw(sample_clean_csv, raw_dir, run_id)

    assert raw_path.exists()
    assert len(df) == 3
    assert "source_file" in df.columns
    assert "run_id" in df.columns
    assert content_hash

    config = load_config()
    accepted, rejected, reasons = validate_dataframe(
        df,
        source_file=sample_clean_csv.name,
        run_id=run_id,
        ingestion_ts=utc_now(),
        config=config,
    )
    assert len(accepted) == 3
    assert len(rejected) == 0
    assert reasons == {}


def test_end_to_end_pipeline(sample_clean_csv, tmp_path, monkeypatch):
    """Run the full pipeline against a temporary sample directory."""
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    # Copy the clean file into the sample dir
    target = sample_dir / "orders_clean.csv"
    target.write_text(sample_clean_csv.read_text())

    # Point config paths at the temp tree
    config = load_config()
    for zone in ["raw", "validated", "curated", "quarantine", "state", "logs"]:
        (tmp_path / zone).mkdir(exist_ok=True)

    # Monkeypatch get_project_root so pipeline writes into tmp_path
    from src import utils, pipeline as pipe_mod

    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pipe_mod, "get_project_root", lambda: tmp_path)

    # Also make load_config return a config whose paths are relative to tmp
    original_load = utils.load_config

    def _patched_load(path="config.yaml"):
        cfg = original_load(path)
        cfg["paths"] = {
            "raw": "raw",
            "validated": "validated",
            "curated": "curated",
            "quarantine": "quarantine",
            "state": "state",
            "logs": "logs",
            "sample": "sample",
        }
        return cfg

    monkeypatch.setattr(utils, "load_config", _patched_load)
    monkeypatch.setattr(pipe_mod, "load_config", _patched_load)

    metrics = run_pipeline(incoming_dir=sample_dir, force=True)

    assert metrics.status in ("success", "partial")
    assert metrics.input_files == 1
    assert metrics.input_records == 3
    assert metrics.accepted_records == 3
    assert metrics.rejected_records == 0

    # Curated partition should exist
    curated = tmp_path / "curated"
    partitions = list(curated.glob("event_date=*"))
    assert len(partitions) >= 1
    # Read back and check counts
    total = 0
    for p in partitions:
        for f in p.glob("*.csv"):
            total += len(pd.read_csv(f))
    assert total == 3
