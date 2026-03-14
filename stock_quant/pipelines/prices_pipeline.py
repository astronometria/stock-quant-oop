from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildPricesPipeline(BasePipeline):
    pipeline_name = "build_prices"

    def __init__(self, repository: DuckDbPriceRepository) -> None:
        self.repository = repository
        self._metrics: dict[str, int] = {}
        self._rows_written = 0
        self._latest_rows = 0

    def extract(self):
        raw_bars = self.repository.count_raw_price_bars()
        allowed_symbols = self.repository.count_included_symbols()
        skipped_not_in_universe = self.repository.count_skipped_not_in_universe()
        skipped_invalid = self.repository.count_skipped_invalid_on_included_symbols()

        return {
            "raw_bars": raw_bars,
            "allowed_symbols": allowed_symbols,
            "skipped_not_in_universe": skipped_not_in_universe,
            "skipped_invalid": skipped_invalid,
        }

    def transform(self, data):
        return data

    def validate(self, data) -> None:
        if int(data.get("raw_bars", 0)) == 0:
            raise PipelineError("no raw price bars available in price_source_daily_raw")

    def load(self, data) -> None:
        self._rows_written = self.repository.replace_price_history_from_raw_sql()
        self._latest_rows = self.repository.rebuild_price_latest()

        self._metrics = dict(data)
        self._metrics["accepted_bars"] = self._rows_written

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_bars", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        result.metrics["latest_rows"] = self._latest_rows
        return result
