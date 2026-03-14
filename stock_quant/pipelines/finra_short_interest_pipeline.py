from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.domain.policies.finra_market_selection_policy import FinraMarketSelectionPolicy
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import DuckDbShortInterestRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildFinraShortInterestPipeline(BasePipeline):
    pipeline_name = "build_finra_short_interest"

    def __init__(self, repository: DuckDbShortInterestRepository, source_market: str = "regular") -> None:
        self.repository = repository
        self.source_market = source_market
        self.service = ShortInterestService(market_policy=FinraMarketSelectionPolicy())
        self._metrics: dict[str, int] = {}
        self._rows_written = 0
        self._latest_rows = 0
        self._source_rows = 0

    def extract(self):
        raw_records = self.repository.load_raw_short_interest_records()
        allowed_symbols = self.repository.load_included_symbols()
        return raw_records, allowed_symbols

    def transform(self, data):
        raw_records, allowed_symbols = data
        return self.service.build(
            raw_records,
            allowed_symbols=allowed_symbols,
            source_market=self.source_market,
        )

    def validate(self, data) -> None:
        history_entries, source_entries, metrics = data
        if not isinstance(history_entries, list):
            raise PipelineError("short interest history entries must be a list")
        if not isinstance(source_entries, list):
            raise PipelineError("short interest source entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("short interest metrics must be a dict")
        if metrics.get("raw_records", 0) == 0:
            raise PipelineError("no raw short interest records available in finra_short_interest_source_raw")

    def load(self, data) -> None:
        history_entries, source_entries, metrics = data
        self._rows_written = self.repository.replace_short_interest_history(history_entries)
        self._latest_rows = self.repository.rebuild_short_interest_latest()
        self._source_rows = self.repository.replace_short_interest_sources(source_entries)
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_records", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        result.metrics["latest_rows"] = self._latest_rows
        result.metrics["source_rows"] = self._source_rows
        return result
