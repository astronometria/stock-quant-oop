from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import duckdb

from stock_quant.infrastructure.db.research_experiment_schema import (
    ResearchExperimentSchemaManager,
)


@dataclass(frozen=True)
class ResearchExperimentManifest:
    experiment_id: str
    snapshot_id: str
    experiment_name: str
    git_commit: str | None
    parameters_json: str | None
    metrics_json: str | None
    status: str
    notes: str | None
    created_by_pipeline: str | None


class DuckDbResearchExperimentRepository:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        ResearchExperimentSchemaManager(self.con).ensure_tables()

    def get_snapshot_manifest(self, snapshot_id: str) -> Optional[tuple]:
        return self.con.execute(
            """
            SELECT
                snapshot_id,
                dataset_name,
                git_commit,
                start_date,
                end_date,
                source_count,
                total_row_count,
                status,
                created_at
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
            """,
            [snapshot_id],
        ).fetchone()

    def insert_experiment_manifest(
        self,
        manifest: ResearchExperimentManifest,
    ) -> None:
        self.con.execute(
            """
            INSERT INTO research_experiment_manifest (
                experiment_id,
                snapshot_id,
                experiment_name,
                git_commit,
                parameters_json,
                metrics_json,
                status,
                notes,
                created_by_pipeline
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                manifest.experiment_id,
                manifest.snapshot_id,
                manifest.experiment_name,
                manifest.git_commit,
                manifest.parameters_json,
                manifest.metrics_json,
                manifest.status,
                manifest.notes,
                manifest.created_by_pipeline,
            ],
        )
