from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.fundamentals_service import FundamentalsService
from stock_quant.infrastructure.repositories.duckdb_fundamentals_repository import DuckDbFundamentalsRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildFundamentalsPipeline(BasePipeline):
    pipeline_name = "build_fundamentals"

    def __init__(self, repository: DuckDbFundamentalsRepository) -> None:
        self.repository = repository
        self.service = FundamentalsService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "sec_filing_rows": self.repository.load_sec_filing_rows(),
            "sec_fact_rows": self.repository.load_sec_fact_normalized_rows(),
        }

    def transform(self, data):
        quarterly, annual, ttm, metrics = self.service.build_snapshots(
            sec_fact_rows=data["sec_fact_rows"],
            sec_filing_rows=data["sec_filing_rows"],
        )
        features, feature_metrics = self.service.build_features_daily(
            quarterly_rows=quarterly,
            annual_rows=annual,
            ttm_rows=ttm,
        )
        metrics.update(feature_metrics)
        return {
            "quarterly": quarterly,
            "annual": annual,
            "ttm": ttm,
            "features": features,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("sec_filing_rows", 0) == 0:
            raise PipelineError("no sec_filing rows available")

    def load(self, data) -> None:
        written_quarterly = self.repository.replace_fundamental_snapshot_quarterly(data["quarterly"])
        written_annual = self.repository.replace_fundamental_snapshot_annual(data["annual"])
        written_ttm = self.repository.replace_fundamental_ttm(data["ttm"])
        written_features = self.repository.replace_fundamental_features_daily(data["features"])

        self._rows_written = written_quarterly + written_annual + written_ttm + written_features
        self._metrics = dict(data["metrics"])
        self._metrics["written_quarterly"] = written_quarterly
        self._metrics["written_annual"] = written_annual
        self._metrics["written_ttm"] = written_ttm
        self._metrics["written_features"] = written_features

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("sec_fact_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
