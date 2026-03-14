from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.repositories.duckdb_label_engine_repository import DuckDbLabelEngineRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.label_engine.forward_returns_labeler import ForwardReturnsLabeler
from stock_quant.shared.exceptions import PipelineError


class BuildLabelEnginePipeline(BasePipeline):
    pipeline_name = "build_label_engine"

    def __init__(self, repository: DuckDbLabelEngineRepository) -> None:
        self.repository = repository
        self.labeler = ForwardReturnsLabeler()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "price_rows": self.repository.load_price_bars_adjusted_rows(),
            "instrument_company_map": self.repository.load_instrument_company_map(),
        }

    def transform(self, data):
        return_rows, vol_rows, metrics = self.labeler.build(
            price_rows=data["price_rows"],
            instrument_company_map=data["instrument_company_map"],
        )
        return {
            "return_rows": return_rows,
            "vol_rows": vol_rows,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("return_label_rows", 0) == 0:
            raise PipelineError("no return label rows built")

    def load(self, data) -> None:
        written_return = self.repository.replace_return_labels_daily(data["return_rows"])
        written_vol = self.repository.replace_volatility_labels_daily(data["vol_rows"])

        self._rows_written = written_return + written_vol
        self._metrics = dict(data["metrics"])
        self._metrics["written_return_labels"] = written_return
        self._metrics["written_volatility_labels"] = written_vol

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("return_label_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
