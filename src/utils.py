"""Shared utilities for the data pipeline."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str | Path = "config.json") -> Dict[str, Any]:
    """Load JSON configuration from project root or given path."""
    path = Path(config_path)
    if not path.is_absolute():
        project_root = Path(__file__).resolve().parent.parent
        path = project_root / path

    # Always prefer config.json if the caller asked for the old yaml name
    if path.suffix.lower() in (".yaml", ".yml"):
        json_path = path.with_suffix(".json")
        if json_path.exists():
            path = json_path

    with open(path, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def get_project_root() -> Path:
    """Return the project root directory."""
    return Path(__file__).resolve().parent.parent


def ensure_dirs(config: Dict[str, Any]) -> None:
    """Create all configured data directories if they do not exist."""
    root = get_project_root()
    for key, rel in config.get("paths", {}).items():
        (root / rel).mkdir(parents=True, exist_ok=True)


def generate_run_id() -> str:
    """Generate a unique run identifier (UUID4 hex)."""
    return uuid.uuid4().hex


def utc_now() -> datetime:
    """Current UTC timestamp with timezone info."""
    return datetime.now(timezone.utc)


def file_content_hash(file_path: Path, chunk_size: int = 65536) -> str:
    """Compute SHA-256 hash of file content for idempotency checks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def setup_logging(log_dir: Optional[Path] = None, level: int = logging.INFO) -> None:
    """Configure root logger with console + optional file handler."""
    root = logging.getLogger()
    root.setLevel(level)
    # Avoid duplicate handlers on re-runs
    if root.handlers:
        return

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
        fh.setFormatter(formatter)
        root.addHandler(fh)


def write_json(path: Path, data: Any) -> None:
    """Write a JSON file atomically (write to temp then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    tmp.replace(path)


def read_json(path: Path) -> Any:
    """Read a JSON file; return empty dict if missing."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
