from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.symbol_reference_service import SymbolReferenceService
from stock_quant.domain.policies.alias_generation_policy import AliasGenerationPolicy
from stock_quant.infrastructure.repositories.duckdb_symbol_reference_repository import DuckDbSymbolReferenceRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildSymbolReferencePipeline(BasePipeline):
    pipeline_name = "build_symbol_reference"

    def __init__(self, repository: DuckDbSymbolReferenceRepository) -> None:
        self.repository = repository
        self.service = SymbolReferenceService(alias_policy=AliasGenerationPolicy())
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return self.repository.load_included_universe_entries()

    def transform(self, data):
        return self.service.build(data)

    def validate(self, data) -> None:
        entries, metrics = data
        if not isinstance(entries, list):
            raise PipelineError("symbol_reference entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("symbol_reference metrics must be a dict")
        if metrics.get("input_entries", 0) == 0:
            raise PipelineError("no included market_universe rows available")

    def load(self, data) -> None:
        entries, metrics = data
        self._rows_written = self.repository.replace_symbol_reference(entries)
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("input_entries", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
