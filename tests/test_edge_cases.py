"""Edge-case coverage: empty, malformed, duplicates, nulls, late data, re-runs."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion import is_already_processed, land_file_to_raw, mark_processed
from src.pipeline import run_pipeline
from src.schema import OrderLineItem
from src.transformation import prepare_for_curated
from src.utils import generate_run_id, load_config, utc_now
from src.validation import validate_dataframe


def test_empty_input(tmp_path, monkeypatch):
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    from src import utils, pipeline as pipe_mod

    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pipe_mod, "get_project_root", lambda: tmp_path)

    def _cfg(path="config.yaml"):
        c = load_config()
        c["paths"] = {
            k: k for k in ["raw", "validated", "curated", "quarantine", "state", "logs", "sample"]
        }
        return c

    monkeypatch.setattr(utils, "load_config", _cfg)
    monkeypatch.setattr(pipe_mod, "load_config", _cfg)

    for z in ["raw", "validated", "curated", "quarantine", "state", "logs"]:
        (tmp_path / z).mkdir(exist_ok=True)

    metrics = run_pipeline(incoming_dir=empty_dir, force=True)
    assert metrics.status == "success"
    assert metrics.input_files == 0
    assert metrics.accepted_records == 0


def test_malformed_and_nulls(sample_dirty_csv):
    config = load_config()
    run_id = generate_run_id()
    df = pd.read_csv(sample_dirty_csv, dtype=str)

    accepted, rejected, reasons = validate_dataframe(
        df,
        source_file=sample_dirty_csv.name,
        run_id=run_id,
        ingestion_ts=utc_now(),
        config=config,
    )
    # 1 good, 3 bad
    assert len(accepted) == 1
    assert len(rejected) == 3
    assert len(reasons) >= 1
    assert all("rejection_reason" in r for r in rejected.to_dict(orient="records"))


def test_duplicate_keys_deduplicated():
    """Two rows with same (order_id, product_id) – keep_latest wins."""
    rows = [
        {
            "order_id": "ORD-X",
            "product_id": "PRD-Y",
            "customer_id": "C1",
            "order_ts": "2025-10-15T10:00:00+00:00",
            "quantity": 1,
            "unit_price": 10.0,
            "currency": "USD",
            "status": "pending",
            "event_date": "2025-10-15",
            "source_file": "f.csv",
            "ingestion_ts": "2025-10-20T00:00:00+00:00",
            "run_id": "r1",
        },
        {
            "order_id": "ORD-X",
            "product_id": "PRD-Y",
            "customer_id": "C1",
            "order_ts": "2025-10-15T12:00:00+00:00",  # later
            "quantity": 5,
            "unit_price": 10.0,
            "currency": "USD",
            "status": "confirmed",
            "event_date": "2025-10-15",
            "source_file": "f.csv",
            "ingestion_ts": "2025-10-20T01:00:00+00:00",
            "run_id": "r1",
        },
    ]
    df = pd.DataFrame(rows)
    config = load_config()
    cleaned, dups = prepare_for_curated(df, config)
    assert dups == 1
    assert len(cleaned) == 1
    assert cleaned.iloc[0]["quantity"] == 5
    assert cleaned.iloc[0]["status"] == "confirmed"


def test_invalid_primary_key_rejected():
    with pytest.raises(Exception):
        OrderLineItem(
            order_id="",  # empty
            product_id="PRD-1",
            customer_id="C1",
            order_ts="2025-10-15T10:00:00Z",
            quantity=1,
            unit_price=1.0,
            currency="USD",
            status="pending",
        )


def test_late_data_accepted_but_flagged():
    """Records older than late_data_days are still accepted (is_late=True)."""
    config = load_config()
    old_ts = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    df = pd.DataFrame(
        [
            {
                "order_id": "ORD-LATE",
                "product_id": "PRD-L",
                "customer_id": "C1",
                "order_ts": old_ts,
                "quantity": 1,
                "unit_price": 9.99,
                "currency": "USD",
                "status": "delivered",
            }
        ]
    )
    accepted, rejected, _ = validate_dataframe(
        df,
        source_file="late.csv",
        run_id="r1",
        ingestion_ts=utc_now(),
        config=config,
    )
    assert len(accepted) == 1
    assert len(rejected) == 0
    assert bool(accepted.iloc[0]["is_late"]) is True


def test_idempotent_rerun(tmp_path, monkeypatch, sample_clean_csv):
    """Re-running the same file content must not increase curated row count."""
    from src import utils, pipeline as pipe_mod

    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    (sample_dir / "clean.csv").write_text(sample_clean_csv.read_text())

    for z in ["raw", "validated", "curated", "quarantine", "state", "logs"]:
        (tmp_path / z).mkdir(exist_ok=True)

    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pipe_mod, "get_project_root", lambda: tmp_path)

    def _cfg(path="config.yaml"):
        c = load_config()
        c["paths"] = {
            k: k for k in ["raw", "validated", "curated", "quarantine", "state", "logs", "sample"]
        }
        c["pipeline"]["enforce_idempotency"] = True
        return c

    monkeypatch.setattr(utils, "load_config", _cfg)
    monkeypatch.setattr(pipe_mod, "load_config", _cfg)

    # First run
    m1 = run_pipeline(incoming_dir=sample_dir, force=False)
    assert m1.accepted_records == 3

    # Second run – should skip the file
    m2 = run_pipeline(incoming_dir=sample_dir, force=False)
    assert m2.skipped_files == ["clean.csv"]
    assert m2.accepted_records == 0  # nothing new written

    # Curated still has exactly 3 rows
    curated = tmp_path / "curated"
    total = sum(
        len(pd.read_csv(f))
        for p in curated.glob("event_date=*")
        for f in p.glob("*.csv")
    )
    assert total == 3


def test_force_rerun_still_idempotent_in_curated(tmp_path, monkeypatch, sample_clean_csv):
    """Even with --force, curated deduplication prevents row explosion."""
    from src import utils, pipeline as pipe_mod

    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    (sample_dir / "clean.csv").write_text(sample_clean_csv.read_text())

    for z in ["raw", "validated", "curated", "quarantine", "state", "logs"]:
        (tmp_path / z).mkdir(exist_ok=True)

    monkeypatch.setattr(utils, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(pipe_mod, "get_project_root", lambda: tmp_path)

    def _cfg(path="config.yaml"):
        c = load_config()
        c["paths"] = {
            k: k for k in ["raw", "validated", "curated", "quarantine", "state", "logs", "sample"]
        }
        return c

    monkeypatch.setattr(utils, "load_config", _cfg)
    monkeypatch.setattr(pipe_mod, "load_config", _cfg)

    run_pipeline(incoming_dir=sample_dir, force=True)
    run_pipeline(incoming_dir=sample_dir, force=True)  # force again

    curated = tmp_path / "curated"
    total = sum(
        len(pd.read_csv(f))
        for p in curated.glob("event_date=*")
        for f in p.glob("*.csv")
    )
    assert total == 3  # still unique
