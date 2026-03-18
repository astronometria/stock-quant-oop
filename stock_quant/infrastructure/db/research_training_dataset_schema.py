from __future__ import annotations

"""
Research training dataset schema.

Objectif
--------
Stocker des datasets figés construits à partir d’un snapshot.

Chaque ligne = (symbol, as_of_date)

Important:
- strictement point-in-time safe
- aucune donnée future
- dataset reproductible via dataset_id
"""

import duckdb


class ResearchTrainingDatasetSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_training_dataset (
                dataset_id VARCHAR,
                snapshot_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,

                -- === FEATURES (V1) ===
                close DOUBLE,
                short_volume_ratio DOUBLE,

                -- === META ===
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rtd_dataset_id
            ON research_training_dataset(dataset_id)
            """
        )

        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rtd_symbol_date
            ON research_training_dataset(symbol, as_of_date)
            """
        )
