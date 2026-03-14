from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.market_universe_service import MarketUniverseService
from stock_quant.domain.policies.exchange_normalization_policy import ExchangeNormalizationPolicy
from stock_quant.domain.policies.security_classification_policy import SecurityClassificationPolicy
from stock_quant.domain.policies.universe_conflict_resolution_policy import UniverseConflictResolutionPolicy
from stock_quant.domain.policies.universe_inclusion_policy import UniverseInclusionPolicy
from stock_quant.infrastructure.repositories.duckdb_universe_repository import DuckDbUniverseRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildMarketUniversePipeline(BasePipeline):
    pipeline_name = "build_market_universe"

    def __init__(self, repository: DuckDbUniverseRepository, allow_adr: bool = True) -> None:
        self.repository = repository
        self.allow_adr = allow_adr
        self.service = MarketUniverseService(
            exchange_policy=ExchangeNormalizationPolicy(),
            classification_policy=SecurityClassificationPolicy(),
            inclusion_policy=UniverseInclusionPolicy(),
            conflict_policy=UniverseConflictResolutionPolicy(),
        )
        self._metrics: dict[str, int] = {}
        self._written_universe = 0
        self._written_conflicts = 0

    def extract(self):
        return self.repository.load_raw_candidates()

    def transform(self, data):
        return self.service.build(data, allow_adr=self.allow_adr)

    def validate(self, data) -> None:
        final_entries, _, metrics = data
        if not isinstance(final_entries, list):
            raise PipelineError("final_entries must be a list")
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("raw_candidates", 0) == 0:
            raise PipelineError("no raw candidates available in symbol_reference_source_raw")

    def load(self, data) -> None:
        final_entries, conflicts, metrics = data
        self._written_universe = self.repository.replace_universe(final_entries)
        self._written_conflicts = self.repository.replace_conflicts(conflicts)
        self._metrics = dict(metrics)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_candidates", 0))
        result.rows_written = self._written_universe
        result.metrics.update(self._metrics)
        result.metrics["written_conflicts"] = self._written_conflicts
        return result
