from __future__ import annotations

import json
from datetime import datetime

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.backtests.signal_backtester import SignalBacktester
from stock_quant.shared.exceptions import PipelineError


class BuildBacktestPipeline(BasePipeline):
    """
    Pipeline de backtest cross-sectional v1.

    Cette version remplace le backtest momentum binaire simpliste
    par un ranking quotidien sur une colonne signal configurable.
    """

    pipeline_name = "build_backtest"

    def __init__(
        self,
        repository=None,
        uow: DuckDbUnitOfWork | None = None,
        dataset_name: str = "research_dataset_v1",
        dataset_version: str = "v1",
        experiment_name: str = "cross_sectional_experiment_v1",
        backtest_name: str = "cross_sectional_backtest_v1",
        signal_column: str = "close_to_sma_20",
        label_column: str = "fwd_return_1d",
        top_n: int = 20,
        holding_days: int = 1,
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
        self.signal_column = signal_column
        self.label_column = label_column
        self.top_n = int(top_n)
        self.holding_days = int(holding_days)

        self._backtester = SignalBacktester()
        self._metrics: dict[str, int | float | None | str] = {}
        self._rows_read = 0
        self._rows_written = 0

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        rows = self.con.execute(
            """
            SELECT *
            FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            ORDER BY as_of_date, symbol
            """,
            [self.dataset_name, self.dataset_version],
        ).fetchdf()
        return rows.to_dict(orient="records")

    def transform(self, data):
        return self._backtester.run_cross_sectional_backtest(
            dataset_rows=data,
            signal_column=self.signal_column,
            label_column=self.label_column,
            top_n=self.top_n,
            experiment_name=self.experiment_name,
            backtest_name=self.backtest_name,
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
        )

    def validate(self, data) -> None:
        if not data:
            raise PipelineError("no rows available in research_dataset_daily for requested dataset")

    def load(self, data) -> None:
        output = self.transform(data)

        con = self.con

        con.execute("DELETE FROM experiment_results_daily")
        con.execute("DELETE FROM backtest_runs")

        if output.positions:
            payload = [
                (
                    row["experiment_name"],
                    row["dataset_name"],
                    row["dataset_version"],
                    row["instrument_id"],
                    row["company_id"],
                    row["symbol"],
                    row["as_of_date"],
                    row["signal_value"],
                    True,
                    row["realized_return"],
                    row["created_at"],
                )
                for row in output.positions
            ]
            con.executemany(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )

        metrics_json = json.dumps(
            {
                "signal_column": self.signal_column,
                "label_column": self.label_column,
                "top_n": self.top_n,
                "holding_days": self.holding_days,
                **output.summary,
            },
            sort_keys=True,
            default=str,
        )

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
                None,
                datetime.utcnow(),
                datetime.utcnow(),
                metrics_json,
                json.dumps(
                    {
                        "signal_column": self.signal_column,
                        "label_column": self.label_column,
                        "top_n": self.top_n,
                        "holding_days": self.holding_days,
                    },
                    sort_keys=True,
                ),
                None,
                datetime.utcnow(),
            ],
        )

        self._rows_read = len(data)
        self._rows_written = len(output.positions) + 1
        self._metrics = {
            "signal_column": self.signal_column,
            "label_column": self.label_column,
            "top_n": self.top_n,
            "holding_days": self.holding_days,
            **output.summary,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
