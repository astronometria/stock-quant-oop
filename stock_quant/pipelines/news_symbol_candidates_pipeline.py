from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.news_symbol_candidate_service import NewsSymbolCandidateService
from stock_quant.domain.policies.news_symbol_candidate_policy import NewsSymbolCandidatePolicy
from stock_quant.infrastructure.repositories.duckdb_news_repository import DuckDbNewsRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildNewsSymbolCandidatesPipeline(BasePipeline):
    pipeline_name = "build_news_symbol_candidates"

    def __init__(self, repository: DuckDbNewsRepository) -> None:
        self.repository = repository
        self.service = NewsSymbolCandidateService(policy=NewsSymbolCandidatePolicy())
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        news_articles = self.repository.load_news_articles_raw()
        symbol_references = self.repository.load_symbol_reference_entries()
        return news_articles, symbol_references

    def transform(self, data):
        news_articles, symbol_references = data
        return self.service.build(news_articles, symbol_references)

    def validate(self, data) -> None:
        entries, metrics = data
        if not isinstance(entries, list):
            raise PipelineError("news symbol candidate entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("news symbol candidate metrics must be a dict")
        if metrics.get("news_articles", 0) == 0:
            raise PipelineError("no news_articles_raw rows available")
        if metrics.get("symbol_references", 0) == 0:
            raise PipelineError("no symbol_reference rows available")

    def load(self, data) -> None:
        entries, metrics = data
        self._rows_written = self.repository.replace_news_symbol_candidates(entries)
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("news_articles", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
