"""
Research Split Manifest Schema

But:
- définir des splits temporels reproductibles
- empêcher toute fuite future (look-ahead bias)
- permettre comparaison scientifique entre expériences

IMPORTANT:
- SQL-first
- aucune logique métier ici
"""

from __future__ import annotations

import duckdb


class ResearchSplitSchemaManager:
    """
    Gère la création des tables liées aux splits de recherche.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection):
        self.con = con

    def ensure_tables(self) -> None:
        """
        Crée la table des splits si elle n'existe pas.
        """

        self.con.execute("""
        CREATE TABLE IF NOT EXISTS research_split_manifest (
            split_id VARCHAR PRIMARY KEY,

            train_start DATE,
            train_end DATE,

            valid_start DATE,
            valid_end DATE,

            test_start DATE,
            test_end DATE,

            embargo_days INTEGER DEFAULT 0,

            notes VARCHAR,

            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
