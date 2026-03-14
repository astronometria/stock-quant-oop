from __future__ import annotations

from datetime import datetime

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.dataset_version_service import DatasetVersionService
from stock_quant.infrastructure.repositories.duckdb_dataset_builder_repository import DuckDbDatasetBuilderRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.research.datasets.dataset_builder import DatasetBuilder
from stock_quant.shared.exceptions import PipelineError


class BuildDatasetBuilderPipeline(BasePipeline):
    pipeline_name = "build_dataset_builder"

    def __init__(self, repository: DuckDbDatasetBuilderRepository, dataset_name: str, dataset_version: str) -> None:
        self.repository = repository
        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.builder = DatasetBuilder()
        self.version_service = DatasetVersionService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "feature_rows": self.repository.load_research_features_rows(),
            "return_label_rows": self.repository.load_return_labels_rows(),
            "volatility_label_rows": self.repository.load_volatility_labels_rows(),
        }

    def transform(self, data):
        dataset_rows, metrics = self.builder.build(
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
            feature_rows=data["feature_rows"],
            return_label_rows=data["return_label_rows"],
            volatility_label_rows=data["volatility_label_rows"],
        )
        return {
            "dataset_rows": dataset_rows,
            "metrics": metrics,
        }

    def validate(self, data) -> None:
        metrics = data["metrics"]
        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")
        if metrics.get("research_dataset_rows", 0) == 0:
            raise PipelineError("no dataset rows built")

    def load(self, data) -> None:
        written_dataset = self.repository.replace_research_dataset_daily(data["dataset_rows"])

        max_as_of = max(row.as_of_date for row in data["dataset_rows"])
        version_row = self.version_service.build_dataset_version(
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
            as_of_date=str(max_as_of),
            row_count=written_dataset,
        )
        written_version = self.repository.insert_dataset_version(version_row)

        self._rows_written = written_dataset + written_version
        self._metrics = dict(data["metrics"])
        self._metrics["written_dataset_rows"] = written_dataset
        self._metrics["written_dataset_version"] = written_version

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("research_dataset_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
