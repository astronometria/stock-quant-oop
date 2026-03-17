from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.pipelines.base_pipeline import BasePipeline


try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable


class BuildShortInterestPipeline(BasePipeline):
    """
    Canonical FINRA short-interest pipeline.

    Backward-compatible constructor:
    - service=ShortInterestService(...)
    - repository=DuckDbShortInterestRepository(...)
    - uow=... (reserved compatibility hook)

    Build decision is based on table state, not only on "pending file" logic.
    """

    pipeline_name = "build_short_interest"

    def __init__(self, service=None, repository=None, uow=None) -> None:
        # Compatibilité rétroactive avec les anciens call-sites qui injectent
        # directement `repository=...`.
        if service is None:
            if repository is not None:
                service = ShortInterestService(repository=repository)
            else:
                raise ValueError(
                    "BuildShortInterestPipeline requires either service=... or repository=..."
                )

        self.service = service
        self.repository = getattr(service, "repository", None)
        self.uow = uow
        if self.repository is None:
            raise ValueError("BuildShortInterestPipeline requires a service exposing repository")

        self._progress_total_steps = 5

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _safe_date_str(self, value):
        if value is None:
            return None
        return str(value)

    def _collect_build_state(self) -> dict:
        """
        Décide si la matérialisation canonique doit être exécutée.

        Règles:
        - raw > 0 et history == 0 -> build
        - raw > 0 et latest == 0 -> build
        - max(raw.source_date) > max(history.settlement_date) -> build
        - sinon noop
        """
        repo = self.repository

        raw_row_count = int(repo.get_raw_row_count())
        source_row_count = int(repo.get_source_row_count())
        history_row_count = int(repo.get_history_row_count())
        latest_row_count = int(repo.get_latest_row_count())

        max_raw_source_date = repo.get_max_raw_source_date()
        max_history_settlement_date = repo.get_max_history_settlement_date()
        max_latest_settlement_date = repo.get_max_latest_settlement_date()

        should_build_because_history_empty = raw_row_count > 0 and history_row_count == 0
        should_build_because_latest_empty = raw_row_count > 0 and latest_row_count == 0
        should_build_because_raw_newer_than_history = (
            max_raw_source_date is not None
            and (
                max_history_settlement_date is None
                or max_raw_source_date > max_history_settlement_date
            )
        )

        should_build = (
            should_build_because_history_empty
            or should_build_because_latest_empty
            or should_build_because_raw_newer_than_history
        )

        if raw_row_count == 0:
            build_reason = "no_raw_rows"
        elif should_build_because_history_empty:
            build_reason = "history_empty_with_raw_present"
        elif should_build_because_latest_empty:
            build_reason = "latest_empty_with_raw_present"
        elif should_build_because_raw_newer_than_history:
            build_reason = "raw_newer_than_history"
        else:
            build_reason = "already_materialized"

        return {
            "raw_row_count": raw_row_count,
            "source_row_count": source_row_count,
            "history_row_count": history_row_count,
            "latest_row_count": latest_row_count,
            "max_raw_source_date": self._safe_date_str(max_raw_source_date),
            "max_history_settlement_date": self._safe_date_str(max_history_settlement_date),
            "max_latest_settlement_date": self._safe_date_str(max_latest_settlement_date),
            "should_build_because_history_empty": should_build_because_history_empty,
            "should_build_because_latest_empty": should_build_because_latest_empty,
            "should_build_because_raw_newer_than_history": should_build_because_raw_newer_than_history,
            "should_build": should_build,
            "build_reason": build_reason,
        }

    def run(self) -> PipelineResult:
        self._log_step(1, "detect canonical short-interest build need from table state")

        state = self._collect_build_state()

        if state["raw_row_count"] == 0:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status="noop",
                rows_read=0,
                rows_written=0,
                metrics=state,
            )

        if not state["should_build"]:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status="noop",
                rows_read=state["raw_row_count"],
                rows_written=0,
                metrics=state,
            )

        self._log_step(2, "load raw short-interest rows from staging")
        rows_read = int(self.service.load_raw())

        self._log_step(3, "build canonical short-interest history")
        history_written = int(self.service.build_history())

        self._log_step(4, "rebuild canonical short-interest latest snapshot")
        latest_written = int(self.service.refresh_latest())

        self._log_step(5, "finalize metrics")
        metrics = dict(state)
        metrics.update(
            {
                "rows_loaded_from_raw": rows_read,
                "history_rows_written": history_written,
                "latest_rows_written": latest_written,
            }
        )

        return PipelineResult(
            pipeline_name=self.pipeline_name,
            status="success",
            rows_read=rows_read,
            rows_written=history_written,
            metrics=metrics,
        )
