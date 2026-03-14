from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.backtest_tracking_service import BacktestTrackingService
from stock_quant.infrastructure.repositories.duckdb_backtest_repository import DuckDbBacktestRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.backtests.signal_backtester import SignalBacktester
from stock_quant.shared.exceptions import PipelineError


class BuildBacktestPipeline(BasePipeline):
    pipeline_name = "build_backtest"

    def __init__(
        self,
        repository: DuckDbBacktestRepository,
        dataset_name: str,
        dataset_version: str,
        experiment_name: str,
        backtest_name: str,
    ) -> None:
        self.repository = repository
        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.experiment_name = experiment_name
        self.backtest_name = backtest_name
        self.backtester = SignalBacktester()
        self.tracker = BacktestTrackingService()
        self._metrics: dict[str, int | float] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "dataset_rows": self.repository.load_dataset_rows(self.dataset_name, self.dataset_version),
            "dataset_version_id": self.repository.lookup_dataset_version_id(self.dataset_name, self.dataset_version),
        }

    def transform(self, data):
        experiment_rows, summary, metrics = self.backtester.run_simple_momentum_backtest(
            dataset_rows=data["dataset_rows"],
            experiment_name=self.experiment_name,
            backtest_name=self.backtest_name,
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
            min_signal=0.0,
        )
        return {
            "experiment_rows": experiment_rows,
            "summary": summary,
            "metrics": metrics,
            "dataset_version_id": data["dataset_version_id"],
        }

    def validate(self, data) -> None:
        if not data["experiment_rows"]:
            raise PipelineError("no experiment rows built")

    def load(self, data) -> None:
        written_experiment_rows = self.repository.replace_experiment_results_daily(data["experiment_rows"])

        experiment_payload = self.tracker.build_experiment_run_payload(
            experiment_name=self.experiment_name,
            dataset_version_id=data["dataset_version_id"],
            metrics=data["metrics"],
        )
        written_experiment_run = self.repository.insert_experiment_run(experiment_payload)

        summary_metrics = self.repository.summary_as_metrics(data["summary"])
        backtest_payload = self.tracker.build_backtest_run_payload(
            backtest_name=self.backtest_name,
            dataset_version_id=data["dataset_version_id"],
            summary_metrics=summary_metrics,
        )
        written_backtest_run = self.repository.insert_backtest_run(backtest_payload)

        self._rows_written = written_experiment_rows + written_experiment_run + written_backtest_run
        self._metrics = dict(data["metrics"])
        self._metrics.update(summary_metrics)
        self._metrics["written_experiment_rows"] = written_experiment_rows
        self._metrics["written_experiment_run"] = written_experiment_run
        self._metrics["written_backtest_run"] = written_backtest_run

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("experiment_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
