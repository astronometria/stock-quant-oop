from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.prices_research_service import PricesResearchService
from stock_quant.infrastructure.repositories.duckdb_prices_research_repository import DuckDbPricesResearchRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildPricesResearchPipeline(BasePipeline):
    pipeline_name = "build_prices_research"

    def __init__(self, repository: DuckDbPricesResearchRepository) -> None:
        self.repository = repository
        self.service = PricesResearchService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "price_history_rows": self.repository.load_price_history_rows(),
            "symbol_instrument_map": self.repository.load_symbol_instrument_map(),
        }

    def transform(self, data):
        unadjusted_frame, unadjusted_metrics = self.service.build_unadjusted_bars(
            price_history_rows=data["price_history_rows"],
            instrument_map=data["symbol_instrument_map"],
        )
        adjusted_frame, adjusted_metrics = self.service.build_adjusted_bars(unadjusted_frame)
        quality_flags, quality_metrics = self.service.build_quality_flags(unadjusted_frame)

        metrics = {}
        metrics.update(unadjusted_metrics)
        metrics.update(adjusted_metrics)
        metrics.update(quality_metrics)

        return {
            "unadjusted_frame": unadjusted_frame,
            "adjusted_frame": adjusted_frame,
            "quality_flags": quality_flags,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("input_price_rows", 0) == 0:
            raise PipelineError("no price_history rows available")

    def load(self, data) -> None:
        written_unadjusted = self.repository.replace_price_bars_unadjusted(data["unadjusted_frame"])
        written_adjusted = self.repository.replace_price_bars_adjusted(data["adjusted_frame"])
        written_quality_flags = self.repository.replace_price_quality_flags(data["quality_flags"])

        self._rows_written = written_unadjusted + written_adjusted + written_quality_flags
        self._metrics = dict(data["metrics"])
        self._metrics["written_unadjusted"] = written_unadjusted
        self._metrics["written_adjusted"] = written_adjusted
        self._metrics["written_quality_flags"] = written_quality_flags

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("input_price_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
