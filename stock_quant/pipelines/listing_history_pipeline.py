"""
listing_history_pipeline.py

Pipeline OOP pour construire l'historique des listings.

But
---

Encapsuler l'orchestration du service ListingHistoryService dans un pipeline
cohérent avec le reste du projet.

Ce pipeline :
- lit toutes les dates disponibles dans la staging source
- construit / met à jour listing_status_history
- retourne un résultat standard de pipeline

Note
----

On reste volontairement simple :
- pas d'argument as_of_date dans cette première version
- pas de progress bar ici, la progression pourra être gérée au niveau CLI
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from stock_quant.app.services.listing_history_service import ListingHistoryService
from stock_quant.shared.exceptions import PipelineError


class PipelineStatus:
    """
    Mini enum simple compatible avec l'esprit du projet.
    """

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class ListingHistoryPipelineResult:
    """
    Résultat standardisé du pipeline.
    """

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


class BuildListingHistoryPipeline:
    """
    Pipeline de construction de listing_status_history.
    """

    def __init__(
        self,
        service: ListingHistoryService,
    ) -> None:
        self.service = service

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    def run(self) -> ListingHistoryPipelineResult:
        """
        Exécute le pipeline.
        """
        started = self._utcnow()

        try:
            result = self.service.build_all_available_dates()

            finished = self._utcnow()
            duration_seconds = (finished - started).total_seconds()

            return ListingHistoryPipelineResult(
                pipeline_name="build_listing_history",
                status=PipelineStatus.SUCCESS,
                started_at=started.replace(tzinfo=None).isoformat(),
                finished_at=finished.replace(tzinfo=None).isoformat(),
                duration_seconds=duration_seconds,
                rows_read=result.observations_read,
                rows_written=result.versions_inserted,
                rows_skipped=result.skipped_observations,
                error_message=None,
                metrics={
                    "as_of_date_count": result.as_of_date_count,
                    "observations_read": result.observations_read,
                    "versions_inserted": result.versions_inserted,
                    "versions_closed": result.versions_closed,
                    "versions_touched": result.versions_touched,
                    "skipped_observations": result.skipped_observations,
                },
            )
        except Exception as exc:
            finished = self._utcnow()
            duration_seconds = (finished - started).total_seconds()

            return ListingHistoryPipelineResult(
                pipeline_name="build_listing_history",
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
