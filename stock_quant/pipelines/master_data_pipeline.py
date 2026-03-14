from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.master_data_service import MasterDataService
from stock_quant.infrastructure.repositories.duckdb_master_data_repository import DuckDbMasterDataRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildMasterDataPipeline(BasePipeline):
    pipeline_name = "build_master_data"

    def __init__(self, repository: DuckDbMasterDataRepository) -> None:
        self.repository = repository
        self.service = MasterDataService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "market_universe": self.repository.load_market_universe_rows(),
            "symbol_reference": self.repository.load_symbol_reference_rows(),
        }

    def transform(self, data):
        return self.service.build(
            market_universe_rows=data["market_universe"],
            symbol_reference_rows=data["symbol_reference"],
        )

    def validate(self, data) -> None:
        companies, instruments, ticker_history, identifier_map, metrics = data
        if not isinstance(companies, list):
            raise PipelineError("companies must be a list")
        if not isinstance(instruments, list):
            raise PipelineError("instruments must be a list")
        if not isinstance(ticker_history, list):
            raise PipelineError("ticker_history must be a list")
        if not isinstance(identifier_map, list):
            raise PipelineError("identifier_map must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("market_universe_rows", 0) == 0:
            raise PipelineError("no market_universe rows available")

    def load(self, data) -> None:
        companies, instruments, ticker_history, identifier_map, metrics = data
        written_companies = self.repository.replace_company_master(companies)
        written_instruments = self.repository.replace_instrument_master(instruments)
        written_ticker_history = self.repository.replace_ticker_history(ticker_history)
        written_identifier_map = self.repository.replace_identifier_map(identifier_map)

        self._rows_written = (
            written_companies
            + written_instruments
            + written_ticker_history
            + written_identifier_map
        )
        self._metrics = dict(metrics)
        self._metrics["written_companies"] = written_companies
        self._metrics["written_instruments"] = written_instruments
        self._metrics["written_ticker_history"] = written_ticker_history
        self._metrics["written_identifier_map"] = written_identifier_map

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(
            self._metrics.get("market_universe_rows", 0)
            + self._metrics.get("symbol_reference_rows", 0)
        )
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
