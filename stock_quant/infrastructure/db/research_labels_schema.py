from __future__ import annotations

"""
Research labels schema.

Chaque ligne = label calculé à partir d’un snapshot.

Important:
- forward looking ONLY ici
- jamais utilisé dans features
"""

import duckdb


class ResearchLabelsSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_labels (
                dataset_id VARCHAR,
                snapshot_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,

                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        self.con.execute("""
            CREATE INDEX IF NOT EXISTS idx_rl_dataset
            ON research_labels(dataset_id)
        """)
