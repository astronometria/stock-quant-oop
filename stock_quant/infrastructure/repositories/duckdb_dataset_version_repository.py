from __future__ import annotations

import json
from typing import Any


class DuckDbDatasetVersionRepository:

    def __init__(self, con: Any) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_versions (
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                built_at TIMESTAMP,
                row_count BIGINT,
                start_date DATE,
                end_date DATE,
                source_tables_json VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def count_training_dataset_rows(self) -> int:
        row = self.con.execute(
            "SELECT COUNT(*) FROM training_dataset_daily"
        ).fetchone()
        return int(row[0]) if row else 0

    def resolve_training_dataset_date_range(self) -> tuple[str | None, str | None]:
        row = self.con.execute(
            """
            SELECT
                CAST(MIN(price_date) AS VARCHAR),
                CAST(MAX(price_date) AS VARCHAR)
            FROM training_dataset_daily
            """
        ).fetchone()
        if not row:
            return None, None
        return row[0], row[1]

    def delete_dataset_version(self, dataset_name: str, dataset_version: str) -> int:
        self.con.execute(
            """
            DELETE FROM dataset_versions
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [dataset_name, dataset_version],
        )
        try:
            row = self.con.execute("SELECT changes()").fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0

    def insert_dataset_version(
        self,
        dataset_name: str,
        dataset_version: str,
        row_count: int,
        start_date: str | None,
        end_date: str | None,
        source_tables: list[str],
    ) -> int:
        self.con.execute(
            """
            INSERT INTO dataset_versions (
                dataset_name,
                dataset_version,
                built_at,
                row_count,
                start_date,
                end_date,
                source_tables_json,
                created_at
            )
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, CAST(? AS DATE), CAST(? AS DATE), ?, CURRENT_TIMESTAMP)
            """,
            [
                dataset_name,
                dataset_version,
                row_count,
                start_date,
                end_date,
                json.dumps(source_tables, ensure_ascii=False),
            ],
        )
        return 1
