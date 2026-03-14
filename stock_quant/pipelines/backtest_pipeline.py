from __future__ import annotations

import json
from datetime import datetime

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildBacktestPipeline(BasePipeline):
    pipeline_name = "build_backtest"

    def __init__(
        self,
        repository=None,
        uow: DuckDbUnitOfWork | None = None,
        dataset_name: str = "research_dataset_v1",
        dataset_version: str = "v1",
        experiment_name: str = "momentum_experiment_v1",
        backtest_name: str = "momentum_backtest_v1",
    ) -> None:
        if repository is not None:
            self.uow = repository.uow
            self.repository = repository
        else:
            self.uow = uow
            self.repository = None

        if self.uow is None:
            raise ValueError("BuildBacktestPipeline requires repository or uow")

        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.experiment_name = experiment_name
        self.backtest_name = backtest_name

        self._experiment_rows = 0
        self._written_experiment_run = 0
        self._written_backtest_run = 0
        self._metrics: dict[str, int | float | None] = {}

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        count = self.con.execute(
            """
            SELECT COUNT(*)
            FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        ).fetchone()[0]
        if int(count) == 0:
            raise PipelineError("no rows available in research_dataset_daily for requested dataset")

    def load(self, data) -> None:
        con = self.con

        con.execute("DELETE FROM experiment_results_daily")

        con.execute(
            """
            INSERT INTO experiment_results_daily (
                experiment_name,
                dataset_name,
                dataset_version,
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                signal_value,
                selected_flag,
                realized_return_1d,
                created_at
            )
            SELECT
                ? AS experiment_name,
                dataset_name,
                dataset_version,
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20 AS signal_value,
                CASE
                    WHEN close_to_sma_20 IS NOT NULL AND close_to_sma_20 > 0 THEN TRUE
                    ELSE FALSE
                END AS selected_flag,
                fwd_return_1d AS realized_return_1d,
                CURRENT_TIMESTAMP AS created_at
            FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.experiment_name, self.dataset_name, self.dataset_version],
        )

        self._experiment_rows = int(
            con.execute("SELECT COUNT(*) FROM experiment_results_daily").fetchone()[0]
        )

        metric_row = con.execute(
            """
            WITH selected AS (
                SELECT realized_return_1d
                FROM experiment_results_daily
                WHERE selected_flag = TRUE
                  AND realized_return_1d IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM experiment_results_daily) AS observations,
                (SELECT COUNT(*) FROM selected) AS selected_observations,
                AVG(realized_return_1d) AS mean_return_1d,
                AVG(CASE WHEN realized_return_1d > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_1d,
                CASE
                    WHEN COUNT(*) = 0 THEN NULL
                    WHEN MIN(1.0 + realized_return_1d) <= 0 THEN NULL
                    ELSE EXP(SUM(LN(1.0 + realized_return_1d))) - 1.0
                END AS cumulative_return_proxy
            FROM selected
            """
        ).fetchone()

        observations = int(metric_row[0] or 0)
        selected_observations = int(metric_row[1] or 0)
        mean_return_1d = float(metric_row[2]) if metric_row[2] is not None else None
        hit_rate_1d = float(metric_row[3]) if metric_row[3] is not None else None
        cumulative_return_proxy = float(metric_row[4]) if metric_row[4] is not None else None

        self._metrics = {
            "experiment_rows": self._experiment_rows,
            "selected_rows": selected_observations,
            "observations": observations,
            "selected_observations": selected_observations,
            "mean_return_1d": mean_return_1d,
            "hit_rate_1d": hit_rate_1d,
            "cumulative_return_proxy": cumulative_return_proxy,
        }

        dataset_version_id = None
        if self.repository is not None:
            dataset_version_id = self.repository.lookup_dataset_version_id(
                self.dataset_name,
                self.dataset_version,
            )

        experiment_metrics_json = json.dumps(
            {
                "experiment_rows": self._experiment_rows,
                "selected_rows": selected_observations,
            },
            sort_keys=True,
        )
        backtest_metrics_json = json.dumps(
            {
                "cumulative_return_proxy": cumulative_return_proxy,
                "hit_rate_1d": hit_rate_1d,
                "mean_return_1d": mean_return_1d,
                "observations": observations,
                "selected_observations": selected_observations,
            },
            sort_keys=True,
        )

        con.execute(
            """
            INSERT INTO experiment_runs (
                experiment_name,
                status,
                dataset_version_id,
                started_at,
                finished_at,
                metrics_json,
                config_json,
                error_message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.experiment_name,
                "SUCCESS",
                dataset_version_id,
                datetime.utcnow(),
                datetime.utcnow(),
                experiment_metrics_json,
                "{}",
                None,
                datetime.utcnow(),
            ],
        )
        self._written_experiment_run = 1

        con.execute(
            """
            INSERT INTO backtest_runs (
                backtest_name,
                status,
                dataset_version_id,
                started_at,
                finished_at,
                metrics_json,
                config_json,
                error_message,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                self.backtest_name,
                "SUCCESS",
                dataset_version_id,
                datetime.utcnow(),
                datetime.utcnow(),
                backtest_metrics_json,
                "{}",
                None,
                datetime.utcnow(),
            ],
        )
        self._written_backtest_run = 1

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._experiment_rows
        result.rows_written = (
            self._experiment_rows
            + self._written_experiment_run
            + self._written_backtest_run
        )
        result.metrics.update(self._metrics)
        result.metrics["written_experiment_rows"] = self._experiment_rows
        result.metrics["written_experiment_run"] = self._written_experiment_run
        result.metrics["written_backtest_run"] = self._written_backtest_run
        return result
