"""
symbol_reference_history_pipeline.py

Pipeline OOP pour construire symbol_reference_history à partir des listings
historiques actifs.

But
---

Offrir une couche de référence symbole historisée, PIT-safe, dérivable ensuite
en vue courante.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from stock_quant.app.services.symbol_reference_history_service import (
    SymbolReferenceHistoryService,
)


class PipelineStatus:
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class SymbolReferenceHistoryPipelineResult:
    pipeline_name: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    rows_read: int
    rows_written: int
    rows_skipped: int
    error_message: str | None
    metrics: dict

    def summary_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_skipped": self.rows_skipped,
            "error_message": self.error_message,
            "metrics": self.metrics,
        }


class BuildSymbolReferenceHistoryPipeline:
    """
    Pipeline de construction de symbol_reference_history.
    """

    def __init__(
        self,
        service: SymbolReferenceHistoryService,
    ) -> None:
        self.service = service

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    def run(self) -> SymbolReferenceHistoryPipelineResult:
        """
        Exécute le pipeline.
        """
        started = self._utcnow()

        try:
            result = self.service.rebuild_from_active_listings()

            finished = self._utcnow()
            duration_seconds = (finished - started).total_seconds()

            return SymbolReferenceHistoryPipelineResult(
                pipeline_name="build_symbol_reference_history",
                status=PipelineStatus.SUCCESS,
                started_at=started.replace(tzinfo=None).isoformat(),
                finished_at=finished.replace(tzinfo=None).isoformat(),
                duration_seconds=duration_seconds,
                rows_read=result.listings_read,
                rows_written=result.versions_inserted,
                rows_skipped=0,
                error_message=None,
                metrics={
                    "listings_read": result.listings_read,
                    "versions_inserted": result.versions_inserted,
                    "versions_closed": result.versions_closed,
                    "versions_unchanged": result.versions_unchanged,
                },
            )
        except Exception as exc:
            finished = self._utcnow()
            duration_seconds = (finished - started).total_seconds()

            return SymbolReferenceHistoryPipelineResult(
                pipeline_name="build_symbol_reference_history",
                status=PipelineStatus.FAILED,
                started_at=started.replace(tzinfo=None).isoformat(),
                finished_at=finished.replace(tzinfo=None).isoformat(),
                duration_seconds=duration_seconds,
                rows_read=0,
                rows_written=0,
                rows_skipped=0,
                error_message=str(exc),
                metrics={},
            )
