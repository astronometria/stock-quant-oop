from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from stock_quant.shared.enums import PipelineStatus


@dataclass(slots=True)
class PipelineResult:
    pipeline_name: str
    status: PipelineStatus
    started_at: datetime
    finished_at: datetime
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    warnings: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.finished_at - self.started_at).total_seconds())

    def summary_dict(self) -> dict[str, Any]:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "duration_seconds": self.duration_seconds,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_skipped": self.rows_skipped,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "error_message": self.error_message,
        }
