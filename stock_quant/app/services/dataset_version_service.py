from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.research.datasets.dataset_version import DatasetVersion


class DatasetVersionService:
    def build_dataset_version(
        self,
        dataset_name: str,
        dataset_version: str,
        as_of_date: str,
        row_count: int,
        feature_run_id: int | None = None,
        label_run_id: int | None = None,
        universe_name: str = "default",
    ) -> DatasetVersion:
        return DatasetVersion(
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            universe_name=universe_name,
            as_of_date=as_of_date,
            feature_run_id=feature_run_id,
            label_run_id=label_run_id,
            row_count=row_count,
            config_json="{}",
            created_at=datetime.utcnow(),
        )

    def dataset_version_dict(self, row: DatasetVersion) -> dict[str, Any]:
        return row.as_dict()
