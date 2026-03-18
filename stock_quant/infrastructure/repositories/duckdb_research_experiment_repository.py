from __future__ import annotations

from dataclasses import dataclass

import duckdb

from stock_quant.infrastructure.db.research_experiment_schema import (
    ResearchExperimentSchemaManager,
)


@dataclass(frozen=True)
class ResearchExperimentManifest:
    experiment_id: str
    snapshot_id: str
    dataset_id: str
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

    def insert(self, m: ResearchExperimentManifest) -> None:
        self.con.execute("""
            INSERT INTO research_experiment_manifest
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            m.experiment_id,
            m.snapshot_id,
            m.dataset_id,
            m.experiment_name,
            m.git_commit,
            m.parameters_json,
            m.metrics_json,
            m.status,
            m.notes,
            m.created_by_pipeline,
        ])
