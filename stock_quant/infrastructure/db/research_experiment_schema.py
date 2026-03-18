from __future__ import annotations

"""
Research experiment schema.

Objectif
--------
Stocker un registre minimal mais robuste des expériences de recherche.

Philosophie
-----------
- une ligne = une expérience
- référence explicite au snapshot_id
- paramètres JSON normalisés
- métriques JSON normalisées
- status explicite pour audit / reproductibilité
"""

import duckdb


class ResearchExperimentSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_experiment_manifest (
                experiment_id VARCHAR PRIMARY KEY,
                snapshot_id VARCHAR NOT NULL,
                experiment_name VARCHAR NOT NULL,
                git_commit VARCHAR,
                parameters_json JSON,
                metrics_json JSON,
                status VARCHAR NOT NULL,
                notes VARCHAR,
                created_by_pipeline VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_experiment_manifest_snapshot_id
            ON research_experiment_manifest(snapshot_id)
            """
        )

        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_research_experiment_manifest_experiment_name
            ON research_experiment_manifest(experiment_name)
            """
        )
