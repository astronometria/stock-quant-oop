from __future__ import annotations

import duckdb


class SymbolNormalizationSchemaManager:
    """
    Gère la table de normalisation des symboles.

    Objectif :
    - centraliser les règles de mapping de symboles
    - permettre un join SQL-first propre dans les pipelines
    - rendre l'insertion idempotente et explicite
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def create_table(self) -> None:
        """
        Crée la table de normalisation si elle n'existe pas.
        """
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
        Insère des règles de base de manière idempotente.

        Important :
        - DuckDB nomme les colonnes d'un VALUES inline en col0, col1, col2...
        - on évite donc column1/column2 qui ne sont pas valides ici
        - on préfère un alias explicite pour garder le SQL lisible
        """
        self.con.execute(
            """
            INSERT INTO symbol_normalization (
                raw_symbol,
                normalized_symbol,
                normalization_type,
                is_active
            )
            SELECT
                v.raw_symbol,
                v.normalized_symbol,
                v.normalization_type,
                v.is_active
            FROM (
                VALUES
                    ('BRK/B', 'BRK.B', 'slash', TRUE),
                    ('BF/B', 'BF.B', 'slash', TRUE),
                    ('PBR/A', 'PBR.A', 'slash', TRUE)
            ) AS v(raw_symbol, normalized_symbol, normalization_type, is_active)
            WHERE NOT EXISTS (
                SELECT 1
                FROM symbol_normalization t
                WHERE t.raw_symbol = v.raw_symbol
            )
            """
        )
