from __future__ import annotations

import duckdb


class ResearchExperimentSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS research_experiment_manifest (
                experiment_id VARCHAR PRIMARY KEY,
                snapshot_id VARCHAR,
                dataset_id VARCHAR,
                experiment_name VARCHAR,
                git_commit VARCHAR,
                parameters_json JSON,
                metrics_json JSON,
                status VARCHAR,
                notes VARCHAR,
                created_by_pipeline VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
