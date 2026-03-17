from __future__ import annotations

"""
Canonical incremental FINRA short-interest pipeline.

Responsabilités :
- détecter les nouveaux source_file dans finra_short_interest_source_raw
- charger seulement les fichiers non encore normalisés
- transformer via le service métier
- upsert dans finra_short_interest_history
- upsert dans finra_short_interest_sources
- rebuild finra_short_interest_latest
- finir en noop s'il n'y a rien de nouveau
"""

from dataclasses import dataclass, field
from datetime import datetime

from stock_quant.pipelines.base_pipeline import BasePipeline

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable


@dataclass
class BuildShortInterestPipelineResult:
    pipeline_name: str
    status: str
    started_at: str
    finished_at: str
    rows_read: int
    rows_written: int
    error_message: str | None = None
    metrics: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "error_message": self.error_message,
            "metrics": self.metrics,
        }


class BuildShortInterestPipeline(BasePipeline):
    pipeline_name = "build_short_interest"

    def __init__(self, repository, service) -> None:
        self.repository = repository
        self.service = service
        self._progress_total_steps = 5

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def run(self) -> BuildShortInterestPipelineResult:
        started_at = datetime.utcnow()
        metrics: dict[str, int] = {}

        try:
            # -------------------------------------------------
            # 1) detect pending source files
            # -------------------------------------------------
            self._log_step(1, "detect pending FINRA short-interest source files")
            pending_source_files = self.repository.list_pending_source_files()

            metrics["pending_source_file_count"] = len(pending_source_files)

            if not pending_source_files:
                finished_at = datetime.utcnow()
                return BuildShortInterestPipelineResult(
                    pipeline_name=self.pipeline_name,
                    status="noop",
                    started_at=started_at.isoformat(),
                    finished_at=finished_at.isoformat(),
                    rows_read=0,
                    rows_written=0,
                    error_message=None,
                    metrics=metrics,
                )

            # -------------------------------------------------
            # 2) load raw rows only for pending files
            # -------------------------------------------------
            self._log_step(2, "load raw rows for pending source files")
            raw_records = self.repository.load_raw_short_interest_records_for_source_files(
                pending_source_files
            )
            metrics["raw_record_count"] = len(raw_records)

            # -------------------------------------------------
            # 3) transform raw -> normalized domain entries
            # -------------------------------------------------
            self._log_step(3, "transform raw short-interest rows")
            history_entries, source_entries, service_metrics = (
                self.service.build_history_entries_from_raw(
                    raw_records,
                    source_market="both",
                )
            )
            metrics.update(service_metrics)
            metrics["history_entry_count"] = len(history_entries)
            metrics["source_entry_count"] = len(source_entries)

            # -------------------------------------------------
            # 4) upsert history + source metadata
            # -------------------------------------------------
            self._log_step(4, "upsert history and source metadata")
            inserted_history = self.repository.upsert_short_interest_history(history_entries)
            inserted_sources = self.repository.upsert_short_interest_sources(source_entries)

            metrics["inserted_history_rows"] = int(inserted_history)
            metrics["inserted_source_rows"] = int(inserted_sources)

            # -------------------------------------------------
            # 5) rebuild latest
            # -------------------------------------------------
            self._log_step(5, "rebuild latest short-interest snapshot")
            latest_row_count = self.repository.rebuild_short_interest_latest()
            metrics["latest_row_count"] = int(latest_row_count)

            finished_at = datetime.utcnow()

            return BuildShortInterestPipelineResult(
                pipeline_name=self.pipeline_name,
                status="success",
                started_at=started_at.isoformat(),
                finished_at=finished_at.isoformat(),
                rows_read=len(raw_records),
                rows_written=int(inserted_history) + int(inserted_sources),
                error_message=None,
                metrics=metrics,
            )

        except Exception as exc:
            finished_at = datetime.utcnow()
            return BuildShortInterestPipelineResult(
                pipeline_name=self.pipeline_name,
                status="failed",
                started_at=started_at.isoformat(),
                finished_at=finished_at.isoformat(),
                rows_read=metrics.get("raw_record_count", 0),
                rows_written=metrics.get("inserted_history_rows", 0) + metrics.get("inserted_source_rows", 0),
                error_message=str(exc),
                metrics=metrics,
            )


# ---------------------------------------------------------------------
# Backward-compatible alias during migration
# ---------------------------------------------------------------------
FinraShortInterestPipeline = BuildShortInterestPipeline
