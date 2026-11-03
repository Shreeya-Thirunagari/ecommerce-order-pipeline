"""Run-level metrics and observability helpers."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils import utc_now, write_json

logger = logging.getLogger(__name__)


@dataclass
class RunMetrics:
    """Collects observability data for a single pipeline run."""

    run_id: str
    status: str = "running"  # running | success | failed | partial
    start_ts: datetime = field(default_factory=utc_now)
    end_ts: Optional[datetime] = None

    # Counts
    input_files: int = 0
    input_records: int = 0
    accepted_records: int = 0
    rejected_records: int = 0
    late_records: int = 0
    duplicate_records: int = 0  # skipped due to idempotency or intra-file dupes

    # Details
    processed_files: List[str] = field(default_factory=list)
    skipped_files: List[str] = field(default_factory=list)  # already processed
    failure_reason: Optional[str] = None
    rejection_reasons: Dict[str, int] = field(default_factory=dict)

    def finish(self, status: str = "success", failure_reason: Optional[str] = None) -> None:
        self.end_ts = utc_now()
        self.status = status
        if failure_reason:
            self.failure_reason = failure_reason

    def duration_seconds(self) -> float:
        if self.end_ts is None:
            return (utc_now() - self.start_ts).total_seconds()
        return (self.end_ts - self.start_ts).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["duration_seconds"] = self.duration_seconds()
        return d

    def log_summary(self) -> None:
        logger.info(
            "Run %s finished | status=%s | files=%d | input=%d | accepted=%d | "
            "rejected=%d | late=%d | dups=%d | duration=%.2fs",
            self.run_id,
            self.status,
            self.input_files,
            self.input_records,
            self.accepted_records,
            self.rejected_records,
            self.late_records,
            self.duplicate_records,
            self.duration_seconds(),
        )
        if self.failure_reason:
            logger.error("Failure reason: %s", self.failure_reason)
        if self.rejection_reasons:
            logger.info("Rejection breakdown: %s", self.rejection_reasons)

    def persist(self, metrics_dir: Path) -> Path:
        """Write metrics JSON to disk. Returns the path written."""
        metrics_dir.mkdir(parents=True, exist_ok=True)
        out = metrics_dir / f"run_{self.run_id}.json"
        write_json(out, self.to_dict())
        return out
