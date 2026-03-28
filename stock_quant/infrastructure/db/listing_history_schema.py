#!/usr/bin/env python3
from __future__ import annotations

"""
listing_history_schema.py

Schéma SQL-first pour la couche history / PIT.

Objectifs:
- garder une séparation claire entre:
  1) listing_status_history            -> existence / identité observée d'un listing
  2) market_universe_history           -> éligibilité à l'univers
  3) symbol_reference_history          -> mapping ticker/cik/nom historisé
  4) listing_event_history             -> événements atomiques reconstruits / observés
  5) history_reconstruction_audit      -> audit trail scientifique
- éviter toute destruction silencieuse
- rester compatible avec des évolutions incrémentales du schéma

Important:
- cette couche ne doit pas "inventer" des snapshots
- elle doit seulement stocker des observations, versions, événements et audit
"""

from typing import Any


class ListingHistorySchemaManager:
    """
    Gestionnaire de schéma pour la couche history / PIT.

    Convention:
    - on reçoit un UnitOfWork existant
    - on utilise sa connexion active
    - on crée / fait évoluer les tables sans drop destructif
    """

    def __init__(self, uow: Any) -> None:
        self.uow = uow

    @property
    def con(self):
        """
        Retourne la connexion active.

        Le repo utilise déjà cette convention.
        """
        connection = getattr(self.uow, "connection", None)
        if connection is None:
            raise RuntimeError("active DB connection is required")
        return connection

    def initialize(self) -> None:
        """
        Initialise et fait évoluer tout le schéma history.

        Ordre:
        - tables principales
        - colonnes évolutives
        - index
        """
        self._create_listing_status_history()
        self._create_market_universe_history()
        self._create_symbol_reference_history()
        self._create_listing_event_history()
        self._create_history_reconstruction_audit()

        self._evolve_listing_status_history()
        self._evolve_market_universe_history()
        self._evolve_symbol_reference_history()
        self._evolve_listing_event_history()
        self._evolve_history_reconstruction_audit()

        self._create_indexes()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]) for row in rows}

    def _add_column_if_missing(
        self,
        table_name: str,
        column_name: str,
        column_type_sql: str,
    ) -> None:
        """
        Ajoute une colonne si absente.

        On reste volontairement minimal et robuste.
        """
        existing = self._table_columns(table_name)
        if column_name in existing:
            return
        self.con.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"
        )

    def _create_index_if_possible(
        self,
        table_name: str,
        index_name: str,
        columns_sql: str,
    ) -> None:
        """
        Crée un index si la table existe.

        DuckDB accepte CREATE INDEX IF NOT EXISTS.
        """
        self.con.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_sql})"
        )

    # ------------------------------------------------------------------
    # Table creation
    # ------------------------------------------------------------------
    def _create_listing_status_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_status_history (
                listing_id VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                symbol VARCHAR,
                company_name VARCHAR,
                company_name_clean VARCHAR,
                exchange VARCHAR,
                security_type VARCHAR,
                source_name VARCHAR,
                listing_status VARCHAR,
                status_reason VARCHAR,
                first_seen_at TIMESTAMP,
                last_seen_at TIMESTAMP,
                effective_from DATE,
                effective_to DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_market_universe_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS market_universe_history (
                listing_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                cik VARCHAR,
                exchange VARCHAR,
                security_type VARCHAR,
                eligible_flag BOOLEAN,
                eligible_reason VARCHAR,
                rule_version VARCHAR,
                effective_from DATE,
                effective_to DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_symbol_reference_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                listing_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                cik VARCHAR,
                company_name VARCHAR,
                company_name_clean VARCHAR,
                exchange VARCHAR,
                security_type VARCHAR,
                source_name VARCHAR,
                symbol_match_enabled BOOLEAN,
                name_match_enabled BOOLEAN,
                effective_from DATE,
                effective_to DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_listing_event_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_event_history (
                event_id VARCHAR,
                listing_id VARCHAR,
                symbol VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                event_type VARCHAR,
                event_date DATE,
                old_symbol VARCHAR,
                new_symbol VARCHAR,
                old_name VARCHAR,
                new_name VARCHAR,
                old_exchange VARCHAR,
                new_exchange VARCHAR,
                old_security_type VARCHAR,
                new_security_type VARCHAR,
                source_name VARCHAR,
                source_url VARCHAR,
                evidence_type VARCHAR,
                confidence_level VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_history_reconstruction_audit(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS history_reconstruction_audit (
                run_id VARCHAR,
                entity_type VARCHAR,
                entity_key VARCHAR,
                action_type VARCHAR,
                source_name VARCHAR,
                evidence_type VARCHAR,
                confidence_level VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    # ------------------------------------------------------------------
    # Schema evolution
    # ------------------------------------------------------------------
    def _evolve_listing_status_history(self) -> None:
        self._add_column_if_missing("listing_status_history", "listing_id", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "company_id", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "cik", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "symbol", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "company_name", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "company_name_clean", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "exchange", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "security_type", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "source_name", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "listing_status", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "status_reason", "VARCHAR")
        self._add_column_if_missing("listing_status_history", "first_seen_at", "TIMESTAMP")
        self._add_column_if_missing("listing_status_history", "last_seen_at", "TIMESTAMP")
        self._add_column_if_missing("listing_status_history", "effective_from", "DATE")
        self._add_column_if_missing("listing_status_history", "effective_to", "DATE")
        self._add_column_if_missing("listing_status_history", "is_active", "BOOLEAN")
        self._add_column_if_missing("listing_status_history", "created_at", "TIMESTAMP")
        self._add_column_if_missing("listing_status_history", "updated_at", "TIMESTAMP")

    def _evolve_market_universe_history(self) -> None:
        self._add_column_if_missing("market_universe_history", "listing_id", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "company_id", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "symbol", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "cik", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "exchange", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "security_type", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "eligible_flag", "BOOLEAN")
        self._add_column_if_missing("market_universe_history", "eligible_reason", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "rule_version", "VARCHAR")
        self._add_column_if_missing("market_universe_history", "effective_from", "DATE")
        self._add_column_if_missing("market_universe_history", "effective_to", "DATE")
        self._add_column_if_missing("market_universe_history", "is_active", "BOOLEAN")
        self._add_column_if_missing("market_universe_history", "created_at", "TIMESTAMP")
        self._add_column_if_missing("market_universe_history", "updated_at", "TIMESTAMP")

    def _evolve_symbol_reference_history(self) -> None:
        self._add_column_if_missing("symbol_reference_history", "listing_id", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "company_id", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "symbol", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "cik", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "company_name", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "company_name_clean", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "exchange", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "security_type", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "source_name", "VARCHAR")
        self._add_column_if_missing("symbol_reference_history", "symbol_match_enabled", "BOOLEAN")
        self._add_column_if_missing("symbol_reference_history", "name_match_enabled", "BOOLEAN")
        self._add_column_if_missing("symbol_reference_history", "effective_from", "DATE")
        self._add_column_if_missing("symbol_reference_history", "effective_to", "DATE")
        self._add_column_if_missing("symbol_reference_history", "is_active", "BOOLEAN")
        self._add_column_if_missing("symbol_reference_history", "created_at", "TIMESTAMP")
        self._add_column_if_missing("symbol_reference_history", "updated_at", "TIMESTAMP")

    def _evolve_listing_event_history(self) -> None:
        self._add_column_if_missing("listing_event_history", "event_id", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "listing_id", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "symbol", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "company_id", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "cik", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "event_type", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "event_date", "DATE")
        self._add_column_if_missing("listing_event_history", "old_symbol", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "new_symbol", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "old_name", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "new_name", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "old_exchange", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "new_exchange", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "old_security_type", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "new_security_type", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "source_name", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "source_url", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "evidence_type", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "confidence_level", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "notes", "VARCHAR")
        self._add_column_if_missing("listing_event_history", "created_at", "TIMESTAMP")

    def _evolve_history_reconstruction_audit(self) -> None:
        self._add_column_if_missing("history_reconstruction_audit", "run_id", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "entity_type", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "entity_key", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "action_type", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "source_name", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "evidence_type", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "confidence_level", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "notes", "VARCHAR")
        self._add_column_if_missing("history_reconstruction_audit", "created_at", "TIMESTAMP")

    # ------------------------------------------------------------------
    # Indexes
    # ------------------------------------------------------------------
    def _create_indexes(self) -> None:
        # Listing history
        self._create_index_if_possible(
            "listing_status_history",
            "idx_listing_status_history_listing_id",
            "listing_id",
        )
        self._create_index_if_possible(
            "listing_status_history",
            "idx_listing_status_history_symbol",
            "symbol",
        )
        self._create_index_if_possible(
            "listing_status_history",
            "idx_listing_status_history_cik",
            "cik",
        )
        self._create_index_if_possible(
            "listing_status_history",
            "idx_listing_status_history_active",
            "is_active, effective_to",
        )
        self._create_index_if_possible(
            "listing_status_history",
            "idx_listing_status_history_dates",
            "effective_from, effective_to",
        )

        # Market universe history
        self._create_index_if_possible(
            "market_universe_history",
            "idx_market_universe_history_listing_id",
            "listing_id",
        )
        self._create_index_if_possible(
            "market_universe_history",
            "idx_market_universe_history_symbol",
            "symbol",
        )
        self._create_index_if_possible(
            "market_universe_history",
            "idx_market_universe_history_active",
            "is_active, effective_to",
        )
        self._create_index_if_possible(
            "market_universe_history",
            "idx_market_universe_history_dates",
            "effective_from, effective_to",
        )

        # Symbol reference history
        self._create_index_if_possible(
            "symbol_reference_history",
            "idx_symbol_reference_history_listing_id",
            "listing_id",
        )
        self._create_index_if_possible(
            "symbol_reference_history",
            "idx_symbol_reference_history_symbol",
            "symbol",
        )
        self._create_index_if_possible(
            "symbol_reference_history",
            "idx_symbol_reference_history_cik",
            "cik",
        )
        self._create_index_if_possible(
            "symbol_reference_history",
            "idx_symbol_reference_history_active",
            "is_active, effective_to",
        )

        # Event / audit
        self._create_index_if_possible(
            "listing_event_history",
            "idx_listing_event_history_event_date",
            "event_date",
        )
        self._create_index_if_possible(
            "listing_event_history",
            "idx_listing_event_history_symbol",
            "symbol",
        )
        self._create_index_if_possible(
            "history_reconstruction_audit",
            "idx_history_reconstruction_audit_run_id",
            "run_id",
        )
