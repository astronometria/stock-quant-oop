from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.news_raw_ingestion_service import NewsRawIngestionService
from stock_quant.infrastructure.repositories.duckdb_news_repository import DuckDbNewsRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildNewsRawPipeline(BasePipeline):
    pipeline_name = "build_news_raw"

    def __init__(self, repository: DuckDbNewsRepository) -> None:
        self.repository = repository
        self.service = NewsRawIngestionService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return self.repository.load_raw_news_source_records()

    def transform(self, data):
        return self.service.build(data)

    def validate(self, data) -> None:
        entries, metrics = data
        if not isinstance(entries, list):
            raise PipelineError("news raw entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("news raw metrics must be a dict")
        if metrics.get("raw_records", 0) == 0:
            raise PipelineError("no raw news records available in news_source_raw")

    def load(self, data) -> None:
        entries, metrics = data
        self._rows_written = self.repository.replace_news_articles_raw(entries)
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_records", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
