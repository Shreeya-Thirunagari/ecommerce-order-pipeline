"""Shared pytest fixtures."""

from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest

from src.utils import get_project_root, load_config


@pytest.fixture
def project_root() -> Path:
    return get_project_root()


@pytest.fixture
def config():
    return load_config("config.yaml")


@pytest.fixture
def tmp_data_dirs(tmp_path: Path, config):
    """
    Create an isolated set of zone directories under a temporary path
    and return a patched config that points to them.
    """
    zones = {
        "raw": tmp_path / "raw",
        "validated": tmp_path / "validated",
        "curated": tmp_path / "curated",
        "quarantine": tmp_path / "quarantine",
        "state": tmp_path / "state",
        "logs": tmp_path / "logs",
        "sample": tmp_path / "sample",
    }
    for p in zones.values():
        p.mkdir(parents=True, exist_ok=True)

    # Shallow copy of config with overridden paths
    cfg = dict(config)
    cfg["paths"] = {k: str(v.relative_to(tmp_path)) for k, v in zones.items()}
    # We will chdir or pass absolute paths; for simplicity return absolute map
    return {
        "tmp_path": tmp_path,
        "zones": zones,
        "config": cfg,
    }


@pytest.fixture
def sample_clean_csv(tmp_path: Path) -> Path:
    """Write a tiny clean CSV and return its path."""
    df = pd.DataFrame(
        [
            {
                "order_id": "ORD-1",
                "product_id": "PRD-1",
                "customer_id": "CUST-1",
                "order_ts": "2025-10-15T10:00:00+00:00",
                "quantity": 2,
                "unit_price": 19.99,
                "currency": "USD",
                "status": "confirmed",
            },
            {
                "order_id": "ORD-1",
                "product_id": "PRD-2",
                "customer_id": "CUST-1",
                "order_ts": "2025-10-15T10:00:00+00:00",
                "quantity": 1,
                "unit_price": 5.50,
                "currency": "USD",
                "status": "confirmed",
            },
            {
                "order_id": "ORD-2",
                "product_id": "PRD-3",
                "customer_id": "CUST-2",
                "order_ts": "2025-10-16T14:30:00+00:00",
                "quantity": 3,
                "unit_price": 100.00,
                "currency": "EUR",
                "status": "shipped",
            },
        ]
    )
    path = tmp_path / "clean.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_dirty_csv(tmp_path: Path) -> Path:
    """CSV containing several known bad records."""
    df = pd.DataFrame(
        [
            {
                "order_id": "ORD-10",
                "product_id": "PRD-10",
                "customer_id": "CUST-10",
                "order_ts": "2025-10-15T10:00:00+00:00",
                "quantity": 1,
                "unit_price": 10.0,
                "currency": "USD",
                "status": "pending",
            },
            {
                "order_id": "ORD-11",
                "product_id": "PRD-11",
                "customer_id": "CUST-11",
                "order_ts": "2025-10-15T11:00:00+00:00",
                "quantity": -5,  # invalid
                "unit_price": 10.0,
                "currency": "USD",
                "status": "pending",
            },
            {
                "order_id": "ORD-12",
                "product_id": "PRD-12",
                "customer_id": "CUST-12",
                "order_ts": "2025-10-15T12:00:00+00:00",
                "quantity": 1,
                "unit_price": 10.0,
                "currency": "XXX",  # invalid
                "status": "pending",
            },
            {
                "order_id": "",  # empty key
                "product_id": "PRD-13",
                "customer_id": "CUST-13",
                "order_ts": "2025-10-15T13:00:00+00:00",
                "quantity": 1,
                "unit_price": 10.0,
                "currency": "USD",
                "status": "pending",
            },
        ]
    )
    path = tmp_path / "dirty.csv"
    df.to_csv(path, index=False)
    return path
