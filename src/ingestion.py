"""Land incoming files into the raw zone and attach metadata."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.utils import file_content_hash, get_project_root, utc_now

logger = logging.getLogger(__name__)


def list_incoming_files(
    incoming_dir: Path,
    supported_extensions: List[str] | None = None,
) -> List[Path]:
    """Return sorted list of supported files in the incoming directory."""
    if supported_extensions is None:
        supported_extensions = [".csv"]
    if not incoming_dir.exists():
        return []
    files = [
        p
        for p in incoming_dir.iterdir()
        if p.is_file() and p.suffix.lower() in supported_extensions
    ]
    return sorted(files)


def land_file_to_raw(
    source_path: Path,
    raw_dir: Path,
    run_id: str,
    encoding: str = "utf-8",
) -> Tuple[Path, pd.DataFrame, str]:
    """
    Copy a source file into the raw zone and enrich it with metadata columns.

    Returns:
        (raw_file_path, enriched_dataframe, content_hash)
    """
    raw_dir.mkdir(parents=True, exist_ok=True)
    content_hash = file_content_hash(source_path)

    # Destination name keeps original stem + run_id for uniqueness of landing
    dest_name = f"{source_path.stem}__{run_id}{source_path.suffix}"
    dest_path = raw_dir / dest_name

    # Read original (CSV only for this implementation)
    try:
        df = pd.read_csv(source_path, encoding=encoding, dtype=str)  # keep as strings initially
    except Exception as exc:
        logger.error("Failed to read %s: %s", source_path, exc)
        raise

    ingestion_ts = utc_now()
    df["source_file"] = source_path.name
    df["ingestion_ts"] = ingestion_ts.isoformat()
    df["run_id"] = run_id
    df["_content_hash"] = content_hash  # technical helper; can be dropped later

    # Write enriched raw file
    df.to_csv(dest_path, index=False, encoding=encoding)
    logger.info(
        "Landed %s -> %s (%d rows, hash=%s...)",
        source_path.name,
        dest_path.name,
        len(df),
        content_hash[:12],
    )
    return dest_path, df, content_hash


def is_already_processed(
    content_hash: str,
    state_dir: Path,
) -> bool:
    """Check the processed-files manifest for this content hash."""
    manifest_path = state_dir / "processed_manifest.json"
    if not manifest_path.exists():
        return False
    import json

    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)
    return content_hash in manifest.get("hashes", {})


def mark_processed(
    content_hash: str,
    source_name: str,
    run_id: str,
    state_dir: Path,
    record_count: int,
) -> None:
    """Append a content hash to the processed manifest (idempotency)."""
    import json

    state_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = state_dir / "processed_manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = {"hashes": {}}

    manifest["hashes"][content_hash] = {
        "source_file": source_name,
        "run_id": run_id,
        "record_count": record_count,
        "processed_at": utc_now().isoformat(),
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    logger.debug("Marked hash %s... as processed", content_hash[:12])
