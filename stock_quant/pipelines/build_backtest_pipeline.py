from __future__ import annotations

import json

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.backtests.signal_backtester import SignalBacktester
from stock_quant.shared.exceptions import PipelineError


class BuildBacktestPipeline(BasePipeline):
    """
    Pipeline de backtest cross-sectional v1.
    """

    pipeline_name = "build_backtest"

    def __init__(
        self,
        repository,
        dataset_name: str = "research_dataset_v1",
        dataset_version: str = "v1",
        experiment_name: str = "cross_sectional_experiment_v1",
        backtest_name: str = "cross_sectional_backtest_v1",
        signal_column: str = "close_to_sma_20",
        label_column: str = "fwd_return_1d",
        top_n: int = 20,
        holding_days: int = 1,
    ) -> None:
        self.repository = repository
        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.experiment_name = experiment_name
        self.backtest_name = backtest_name
        self.signal_column = signal_column
        self.label_column = label_column
        self.top_n = int(top_n)
        self.holding_days = int(holding_days)

        self._backtester = SignalBacktester()
        self._rows_read = 0
        self._rows_written = 0
        self._metrics: dict[str, int | float | str | None] = {}

    def extract(self):
        rows = self.repository.load_dataset_rows(
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
        )
        self._rows_read = len(rows)
        return rows

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

    def load(self, output) -> None:
        self.repository.clear_outputs()

        experiment_rows = [
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

        position_rows = [
            (
                row["backtest_name"],
                row["dataset_name"],
                row["dataset_version"],
                row["instrument_id"],
                row["company_id"],
                row["symbol"],
                row["as_of_date"],
                row["signal_column"],
                row["label_column"],
                row["signal_value"],
                row["realized_return"],
                row["rank_position"],
                row["weight"],
                row["created_at"],
            )
            for row in output.positions
        ]

        daily_rows = [
            (
                row["backtest_name"],
                row["dataset_name"],
                row["dataset_version"],
                row["as_of_date"],
                row["signal_column"],
                row["label_column"],
                row["selected_count"],
                row["gross_return"],
                row["hit_rate"],
                row["created_at"],
            )
            for row in output.daily
        ]

        written_experiment_rows = self.repository.insert_experiment_results_daily(experiment_rows)
        written_position_rows = self.repository.insert_backtest_positions(position_rows)
        written_daily_rows = self.repository.insert_backtest_daily(daily_rows)

        config_json = json.dumps(
            {
                "signal_column": self.signal_column,
                "label_column": self.label_column,
                "top_n": self.top_n,
                "holding_days": self.holding_days,
                "dataset_name": self.dataset_name,
                "dataset_version": self.dataset_version,
                "experiment_name": self.experiment_name,
            },
            sort_keys=True,
        )
        metrics_json = json.dumps(output.summary, sort_keys=True, default=str)
        written_run_rows = self.repository.insert_backtest_run(
            backtest_name=self.backtest_name,
            status="SUCCESS",
            metrics_json=metrics_json,
            config_json=config_json,
        )

        self._rows_written = (
            written_experiment_rows
            + written_position_rows
            + written_daily_rows
            + written_run_rows
        )

        self._metrics = {
            "signal_column": self.signal_column,
            "label_column": self.label_column,
            "top_n": self.top_n,
            "holding_days": self.holding_days,
            "written_experiment_rows": written_experiment_rows,
            "written_position_rows": written_position_rows,
            "written_daily_rows": written_daily_rows,
            "written_run_rows": written_run_rows,
            **output.summary,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
