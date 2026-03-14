from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.short_data_service import ShortDataService
from stock_quant.infrastructure.repositories.duckdb_short_data_repository import DuckDbShortDataRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildShortDataPipeline(BasePipeline):
    pipeline_name = "build_short_data"

    def __init__(self, repository: DuckDbShortDataRepository) -> None:
        self.repository = repository
        self.service = ShortDataService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "symbol_map": self.repository.load_symbol_map(),
            "short_interest_raw": self.repository.load_finra_short_interest_raw_rows(),
            "daily_short_volume_raw": self.repository.load_finra_daily_short_volume_raw_rows(),
        }

    def transform(self, data):
        short_interest_rows, short_interest_metrics = self.service.build_short_interest_history(
            raw_rows=data["short_interest_raw"],
            symbol_map=data["symbol_map"],
        )
        daily_short_volume_rows, daily_short_volume_metrics = self.service.build_daily_short_volume_history(
            raw_rows=data["daily_short_volume_raw"],
            symbol_map=data["symbol_map"],
        )
        features, feature_metrics = self.service.build_short_features_daily(
            short_interest_rows=short_interest_rows,
            daily_short_volume_rows=daily_short_volume_rows,
        )

        metrics = {}
        metrics.update(short_interest_metrics)
        metrics.update(daily_short_volume_metrics)
        metrics.update(feature_metrics)

        return {
            "short_interest_rows": short_interest_rows,
            "daily_short_volume_rows": daily_short_volume_rows,
            "features": features,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("raw_short_interest_rows", 0) == 0 and metrics.get("raw_daily_short_volume_rows", 0) == 0:
            raise PipelineError("no short raw rows available")

    def load(self, data) -> None:
        written_short_interest = self.repository.replace_short_interest_history(data["short_interest_rows"])
        written_daily_short_volume = self.repository.replace_daily_short_volume_history(data["daily_short_volume_rows"])
        written_features = self.repository.replace_short_features_daily(data["features"])

        self._rows_written = written_short_interest + written_daily_short_volume + written_features
        self._metrics = dict(data["metrics"])
        self._metrics["written_short_interest"] = written_short_interest
        self._metrics["written_daily_short_volume"] = written_daily_short_volume
        self._metrics["written_short_features"] = written_features

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(
            self._metrics.get("raw_short_interest_rows", 0)
            + self._metrics.get("raw_daily_short_volume_rows", 0)
        )
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
