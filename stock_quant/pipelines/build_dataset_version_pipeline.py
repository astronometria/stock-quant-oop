from __future__ import annotations

from dataclasses import dataclass

from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class BuildDatasetVersionResult:
    dataset_name: str
    dataset_version: str
    row_count: int
    start_date: str | None
    end_date: str | None
    written_rows: int


class BuildDatasetVersionPipeline:

    pipeline_name = "build_dataset_version"

    def __init__(self, repository) -> None:
        self.repository = repository

    def run(
        self,
        dataset_name: str,
        dataset_version: str,
        replace_existing: bool = True,
    ) -> BuildDatasetVersionResult:
        self.repository.ensure_tables()

        row_count = self.repository.count_training_dataset_rows()
        if row_count == 0:
            raise PipelineError("training_dataset_daily is empty")

        start_date, end_date = self.repository.resolve_training_dataset_date_range()

        if replace_existing:
            self.repository.delete_dataset_version(
                dataset_name=dataset_name,
                dataset_version=dataset_version,
            )

        written_rows = self.repository.insert_dataset_version(
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            row_count=row_count,
            start_date=start_date,
            end_date=end_date,
            source_tables=[
                "price_history",
                "fundamental_features_daily",
                "short_features_daily",
                "market_universe",
                "training_dataset_daily",
            ],
        )

        return BuildDatasetVersionResult(
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            row_count=row_count,
            start_date=start_date,
            end_date=end_date,
            written_rows=written_rows,
        )
