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

Évolution
---------
On ajoute maintenant `rsi_14` au schéma du dataset research moderne afin
de supporter des signaux prix research-grade sans rester limité au seul
`short_volume_ratio`.
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

                -- === FEATURES (V1/V2 transition) ===
                close DOUBLE,
                short_volume_ratio DOUBLE,
                rsi_14 DOUBLE,

                -- === META ===
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        existing_columns = {
            str(row[1]).strip()
            for row in self.con.execute(
                "PRAGMA table_info('research_training_dataset')"
            ).fetchall()
        }

        if "rsi_14" not in existing_columns:
            self.con.execute(
                """
                ALTER TABLE research_training_dataset
                ADD COLUMN rsi_14 DOUBLE
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
