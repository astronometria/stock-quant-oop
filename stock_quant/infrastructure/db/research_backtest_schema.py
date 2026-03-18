from __future__ import annotations

"""
Research backtest schema manager.

Objectif
--------
Gérer un schéma de backtest compatible avec le mode scientific-grade:
- split-aware
- partition-aware
- transaction-cost aware

Important
---------
Cette version est MIGRATOIRE:
- crée la table si absente
- ajoute les colonnes manquantes si la table existe déjà
- évite de casser les bases DuckDB déjà construites
"""

import duckdb


class ResearchBacktestSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def _table_exists(self, table_name: str) -> bool:
        row = self.con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE lower(table_name) = lower(?)
            """,
            [table_name],
        ).fetchone()
        return bool(row and int(row[0]) > 0)

    def _column_exists(self, table_name: str, column_name: str) -> bool:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        existing = {str(row[1]).strip().lower() for row in rows}
        return column_name.lower() in existing

    def _add_column_if_missing(self, table_name: str, column_name: str, column_type: str) -> None:
        if not self._column_exists(table_name, column_name):
            self.con.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )

    def ensure_tables(self) -> None:
        """
        Assure un schéma backtest moderne.

        Cas 1:
        - la table n'existe pas -> création complète

        Cas 2:
        - la table existe déjà -> ajout progressif des colonnes manquantes
        """
        if not self._table_exists("research_backtest"):
            self.con.execute("""
                CREATE TABLE research_backtest (
                    backtest_id VARCHAR,
                    dataset_id VARCHAR,
                    split_id VARCHAR,
                    partition_name VARCHAR,
                    signal_name VARCHAR,
                    transaction_cost_bps DOUBLE,
                    gross_return DOUBLE,
                    total_cost DOUBLE,
                    net_return DOUBLE,
                    avg_return DOUBLE,
                    volatility DOUBLE,
                    sharpe DOUBLE,
                    turnover DOUBLE,
                    n_obs BIGINT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            return

        # Migration non destructive de l'ancien schéma vers le nouveau.
        self._add_column_if_missing("research_backtest", "split_id", "VARCHAR")
        self._add_column_if_missing("research_backtest", "partition_name", "VARCHAR")
        self._add_column_if_missing("research_backtest", "transaction_cost_bps", "DOUBLE")
        self._add_column_if_missing("research_backtest", "gross_return", "DOUBLE")
        self._add_column_if_missing("research_backtest", "total_cost", "DOUBLE")
        self._add_column_if_missing("research_backtest", "net_return", "DOUBLE")
        self._add_column_if_missing("research_backtest", "turnover", "DOUBLE")
