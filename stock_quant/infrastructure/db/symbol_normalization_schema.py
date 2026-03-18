from __future__ import annotations

import duckdb


class SymbolNormalizationSchemaManager:
    """
    Gère la table de normalisation des symboles.

    Objectif :
    - Centraliser les règles de mapping de symboles
    - Permettre un join SQL-first propre dans les pipelines
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def create_table(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_normalization (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                normalization_type VARCHAR,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def insert_default_rules(self) -> None:
        """
        Insère des règles de base (idempotent).
        """
        self.con.execute(
            """
            INSERT INTO symbol_normalization
            SELECT * FROM (
                VALUES
                    ('BRK/B', 'BRK.B', 'slash', TRUE),
                    ('BF/B', 'BF.B', 'slash', TRUE),
                    ('PBR/A', 'PBR.A', 'slash', TRUE)
            )
            WHERE NOT EXISTS (
                SELECT 1 FROM symbol_normalization t
                WHERE t.raw_symbol = column1
            )
            """
        )
