from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.master_data_service import MasterDataService
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildMasterDataPipeline(BasePipeline):
    pipeline_name = "build_master_data"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildMasterDataPipeline requires repository or uow")

        self.service = MasterDataService(self.uow)
        self._metrics: dict[str, int] = {}
        self._rows_read = 0
        self._rows_written = 0

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        price_rows = int(
            self.con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0]
        )
        self._rows_read = price_rows
        if price_rows == 0:
            raise PipelineError("no rows available in price_bars_adjusted")

    def load(self, data) -> None:
        self._metrics = self.service.rebuild_master_from_price_history()

        company_master_rows = int(
            self.con.execute("SELECT COUNT(*) FROM company_master").fetchone()[0]
        )
        market_universe_rows = int(
            self.con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0]
        )

        self._metrics["company_master_rows"] = company_master_rows
        self._metrics["market_universe_rows"] = market_universe_rows
        self._metrics["written_companies"] = company_master_rows
        self._metrics["written_instruments"] = self._metrics["instrument_master_rows"]
        self._metrics["written_ticker_history"] = self._metrics["ticker_history_rows"]
        self._metrics["written_identifier_map"] = self._metrics["identifier_map_rows"]
        self._metrics["pit_price_history_symbol_matches"] = self._metrics["price_symbol_count"]
        self._metrics["symbol_reference_rows"] = int(
            self.con.execute("SELECT COUNT(*) FROM symbol_reference").fetchone()[0]
        )

        self._rows_written = (
            self._metrics["instrument_master_rows"]
            + self._metrics["ticker_history_rows"]
            + self._metrics["identifier_map_rows"]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
