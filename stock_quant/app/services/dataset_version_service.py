from __future__ import annotations

from datetime import date, datetime
from typing import Any

from stock_quant.research.datasets.dataset_version import DatasetVersion


class DatasetVersionService:
    def build_dataset_version(
        self,
        dataset_name: str,
        dataset_version: str,
        as_of_date: str | date,
        row_count: int,
        feature_run_id: int | None = None,
        label_run_id: int | None = None,
        universe_name: str = "default",
        config_json: str = "{}",
        created_at: datetime | None = None,
    ) -> DatasetVersion:
        normalized_as_of_date = (
            as_of_date.isoformat() if isinstance(as_of_date, date) else str(as_of_date)
        )

        return DatasetVersion(
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            universe_name=universe_name,
            as_of_date=normalized_as_of_date,
            feature_run_id=feature_run_id,
            label_run_id=label_run_id,
            row_count=int(row_count),
            config_json=config_json if config_json else "{}",
            created_at=created_at or datetime.utcnow(),
        )

    def dataset_version_dict(self, row: DatasetVersion) -> dict[str, Any]:
        return row.as_dict()
