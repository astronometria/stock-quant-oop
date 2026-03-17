from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable


class BuildShortInterestPipeline(BasePipeline):
    """
    Canonical SQL-first FINRA short-interest pipeline.

    Design:
    - Step 1: inspect current table state
    - Step 2: lightweight raw load / contract probe
    - Step 3: build canonical history in SQL
    - Step 4: rebuild latest in SQL
    - Step 5: validate outputs and return compact metrics

    Important:
    - no Python row-by-row transforms in the canonical path
    - no current-universe filter here
    - raw -> history/latest happens in DuckDB
    """

    pipeline_name = "build_short_interest"

    def __init__(self, service) -> None:
        if service is None:
            raise ValueError("BuildShortInterestPipeline requires service")
        self.service = service
        self._progress_total_steps = 5

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _collect_build_state(self) -> dict[str, object]:
        state = self.service.get_build_state()
        return {
            "raw_row_count": int(state.raw_row_count),
            "source_row_count": int(state.source_row_count),
            "history_row_count": int(state.history_row_count),
            "latest_row_count": int(state.latest_row_count),
            "max_raw_source_date": state.max_raw_source_date,
            "max_history_settlement_date": state.max_history_settlement_date,
            "max_latest_settlement_date": state.max_latest_settlement_date,
        }

    def _should_build(self, state: dict[str, object]) -> bool:
        """
        Conservative SQL-first decision logic.

        Rebuild if:
        - history empty
        - latest empty
        - raw has a newer source date than history
        """
        raw_count = int(state["raw_row_count"])
        history_count = int(state["history_row_count"])
        latest_count = int(state["latest_row_count"])

        if raw_count == 0:
            return False

        if history_count == 0:
            return True

        if latest_count == 0:
            return True

        raw_date = state["max_raw_source_date"]
        history_date = state["max_history_settlement_date"]

        if raw_date is None:
            return False

        if history_date is None:
            return True

        return str(raw_date) > str(history_date)

    def _validate_post_build(self) -> dict[str, int]:
        """
        Final sanity checks after SQL writes.
        """
        state = self._collect_build_state()

        history_count = int(state["history_row_count"])
        latest_count = int(state["latest_row_count"])

        if history_count == 0:
            raise PipelineError("finra_short_interest_history is empty after SQL-first build")

        if latest_count == 0:
            raise PipelineError("finra_short_interest_latest is empty after SQL-first refresh")

        return {
            "history_row_count": history_count,
            "latest_row_count": latest_count,
        }

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        return None

    def run(self) -> PipelineResult:
        result = PipelineResult.started(self.pipeline_name)

        try:
            # -----------------------------------------------------------------
            # Step 1 - inspect state
            # -----------------------------------------------------------------
            self._log_step(1, "detect canonical short-interest build need from table state")
            state = self._collect_build_state()
            should_build = self._should_build(state)

            result.metrics.update(
                {
                    "raw_row_count": int(state["raw_row_count"]),
                    "source_row_count": int(state["source_row_count"]),
                    "history_row_count_before": int(state["history_row_count"]),
                    "latest_row_count_before": int(state["latest_row_count"]),
                    "max_raw_source_date": state["max_raw_source_date"],
                    "max_history_settlement_date": state["max_history_settlement_date"],
                    "max_latest_settlement_date": state["max_latest_settlement_date"],
                }
            )

            if int(state["raw_row_count"]) == 0:
                result.rows_read = 0
                result.rows_written = 0
                result.metrics["build_reason"] = "raw_empty"
                result.finish_noop()
                return result

            if not should_build:
                result.rows_read = 0
                result.rows_written = 0
                result.metrics["build_reason"] = "already_up_to_date"
                result.finish_noop()
                return result

            if int(state["history_row_count"]) == 0:
                result.metrics["build_reason"] = "history_empty"
            elif int(state["latest_row_count"]) == 0:
                result.metrics["build_reason"] = "latest_empty"
            else:
                result.metrics["build_reason"] = "raw_newer_than_history"

            # -----------------------------------------------------------------
            # Step 2 - lightweight raw contract / count probe
            # -----------------------------------------------------------------
            self._log_step(2, "load raw short-interest rows from staging")
            rows_read = int(self.service.load_raw())
            result.rows_read = rows_read
            result.metrics["rows_loaded_from_raw_probe"] = rows_read

            # -----------------------------------------------------------------
            # Step 3 - SQL-first raw -> history
            # -----------------------------------------------------------------
            self._log_step(3, "build canonical short-interest history")
            history_result = self.service.build_history()
            result.metrics.update(history_result)

            # -----------------------------------------------------------------
            # Step 4 - SQL-first history -> latest
            # -----------------------------------------------------------------
            self._log_step(4, "refresh canonical latest short-interest snapshot")
            latest_result = self.service.refresh_latest()
            result.metrics.update(latest_result)

            # -----------------------------------------------------------------
            # Step 5 - validate and finalize
            # -----------------------------------------------------------------
            self._log_step(5, "validate canonical short-interest outputs")
            post = self._validate_post_build()
            result.metrics.update(post)

            result.rows_written = int(history_result.get("history_rows_inserted", 0))
            result.finish_success()
            return result

        except Exception as exc:
            result.finish_failed(str(exc))
            return result
