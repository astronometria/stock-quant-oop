from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.research_universe_service import ResearchUniverseService
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildResearchUniversePipeline(BasePipeline):
    pipeline_name = "build_research_universe"

    def __init__(self, uow: DuckDbUnitOfWork | None = None) -> None:
        self.uow = uow
        if self.uow is None:
            raise ValueError("BuildResearchUniversePipeline requires uow")
        self.service = ResearchUniverseService(self.uow)
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
        self._rows_read = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM price_source_daily_raw_all
                WHERE asset_class = 'STOCK'
                  AND venue_group IN ('NASDAQ', 'NYSE', 'NYSEMKT')
                """
            ).fetchone()[0]
        )
        if self._rows_read == 0:
            raise PipelineError("no eligible STOCK rows available in price_source_daily_raw_all")

    def load(self, data) -> None:
        self._metrics = self.service.rebuild_conservative_research_universe()
        self._rows_written = int(self._metrics.get("research_universe_rows", 0))

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
