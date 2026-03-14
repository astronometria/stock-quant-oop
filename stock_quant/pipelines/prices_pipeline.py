from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildPricesPipeline(BasePipeline):
    pipeline_name = "build_prices"

    def __init__(self, repository: DuckDbPriceRepository) -> None:
        self.repository = repository
        self.service = PriceIngestionService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0
        self._latest_rows = 0

    def extract(self):
        raw_bars = self.repository.load_raw_price_bars()
        allowed_symbols = self.repository.load_included_symbols()
        return raw_bars, allowed_symbols

    def transform(self, data):
        raw_bars, allowed_symbols = data
        return self.service.build(raw_bars, allowed_symbols=allowed_symbols)

    def validate(self, data) -> None:
        entries, metrics = data
        if not isinstance(entries, list):
            raise PipelineError("price entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("price metrics must be a dict")
        if metrics.get("raw_bars", 0) == 0:
            raise PipelineError("no raw price bars available in price_source_daily_raw")

    def load(self, data) -> None:
        entries, metrics = data
        self._rows_written = self.repository.replace_price_history(entries)
        self._latest_rows = self.repository.rebuild_price_latest()
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_bars", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        result.metrics["latest_rows"] = self._latest_rows
        return result
