from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.news_intelligence_service import NewsIntelligenceService
from stock_quant.infrastructure.repositories.duckdb_news_intelligence_repository import DuckDbNewsIntelligenceRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildNewsIntelligencePipeline(BasePipeline):
    pipeline_name = "build_news_intelligence"

    def __init__(self, repository: DuckDbNewsIntelligenceRepository) -> None:
        self.repository = repository
        self.service = NewsIntelligenceService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "raw_articles": self.repository.load_news_articles_raw_rows(),
            "symbol_candidates": self.repository.load_news_symbol_candidate_rows(),
            "symbol_map": self.repository.load_symbol_map(),
        }

    def transform(self, data):
        normalized_articles, links, event_windows, llm_rows, features, metrics = self.service.build(
            raw_articles=data["raw_articles"],
            symbol_candidates=data["symbol_candidates"],
            symbol_map=data["symbol_map"],
        )
        return {
            "normalized_articles": normalized_articles,
            "links": links,
            "event_windows": event_windows,
            "llm_rows": llm_rows,
            "features": features,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("raw_articles", 0) == 0:
            raise PipelineError("no news_articles_raw rows available")

    def load(self, data) -> None:
        written_articles = self.repository.replace_news_articles_normalized(data["normalized_articles"])
        written_links = self.repository.replace_news_symbol_links(data["links"])
        written_windows = self.repository.replace_news_event_windows(data["event_windows"])
        written_llm = self.repository.replace_news_llm_enrichment(data["llm_rows"])
        written_features = self.repository.replace_news_features_daily(data["features"])

        self._rows_written = written_articles + written_links + written_windows + written_llm + written_features
        self._metrics = dict(data["metrics"])
        self._metrics["written_articles"] = written_articles
        self._metrics["written_links"] = written_links
        self._metrics["written_event_windows"] = written_windows
        self._metrics["written_llm"] = written_llm
        self._metrics["written_features"] = written_features

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_articles", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
