from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.feature_engine_service import FeatureEngineService
from stock_quant.infrastructure.repositories.duckdb_feature_engine_repository import DuckDbFeatureEngineRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.feature_engine.technical_features_engine import TechnicalFeaturesEngine
from stock_quant.shared.exceptions import PipelineError


class BuildFeatureEnginePipeline(BasePipeline):
    pipeline_name = "build_feature_engine"

    def __init__(self, repository: DuckDbFeatureEngineRepository) -> None:
        self.repository = repository
        self.technical_engine = TechnicalFeaturesEngine()
        self.merge_service = FeatureEngineService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "price_rows": self.repository.load_price_bars_adjusted_rows(),
            "fundamental_rows": self.repository.load_fundamental_features_rows(),
            "short_rows": self.repository.load_short_features_rows(),
            "news_rows": self.repository.load_news_features_rows(),
            "instrument_company_map": self.repository.load_instrument_company_map(),
        }

    def transform(self, data):
        technical_rows, technical_metrics = self.technical_engine.build(data["price_rows"])
        research_rows, research_metrics = self.merge_service.merge_features(
            technical_rows=[row.as_dict() for row in technical_rows],
            fundamental_rows=data["fundamental_rows"],
            short_rows=data["short_rows"],
            news_rows=data["news_rows"],
        )

        metrics = {}
        metrics.update(technical_metrics)
        metrics.update(research_metrics)

        return {
            "technical_rows": technical_rows,
            "research_rows": research_rows,
            "instrument_company_map": data["instrument_company_map"],
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("technical_feature_rows", 0) == 0:
            raise PipelineError("no technical feature rows built")

    def load(self, data) -> None:
        written_technical = self.repository.replace_technical_features_daily(
            data["technical_rows"],
            data["instrument_company_map"],
        )
        written_research = self.repository.replace_research_features_daily(data["research_rows"])

        self._rows_written = written_technical + written_research
        self._metrics = dict(data["metrics"])
        self._metrics["written_technical"] = written_technical
        self._metrics["written_research"] = written_research

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("technical_feature_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
