"""
listing_history_schema.py

Schema manager pour les tables historisées liées aux listings.

Objectif
--------

Introduire une couche survivor-bias-aware / point-in-time (PIT) pour :

- l'existence des listings
- l'éligibilité à l'univers
- la référence symbole

Pourquoi ce fichier est important
---------------------------------

Le design historique évite les problèmes suivants :

- disparition des delistings dans un snapshot courant
- contamination rétroactive d'un backtest par les symboles "encore vivants"
- perte des changements de ticker / exchange / security_type
- joins historiques incorrects sur un snapshot courant

Tables gérées
-------------

1. listing_status_history
   Historique des versions de listing

2. market_universe_history
   Historique de l'éligibilité d'un listing à l'univers

3. symbol_reference_history
   Historique de la couche de référence symbole

Vues courantes
--------------

Pour préserver la compatibilité avec l'existant, on expose aussi :

- market_universe_current
- symbol_reference_current

Ces vues lisent les lignes actives de l'historique.

Remarque importante
-------------------

On ne remplace pas automatiquement les tables existantes `market_universe`
et `symbol_reference` ici, pour éviter de casser brutalement le projet.
On crée d'abord des vues `_current` explicites.
"""

from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ListingHistorySchemaManager:
    """
    Initialise le schéma historisé des listings et des dimensions dérivées.
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def _require_connection(self):
        if self.uow.connection is None:
            raise RuntimeError("DuckDbUnitOfWork has no active connection")
        return self.uow.connection

    def initialize(self) -> None:
        """
        Crée les tables historisées et les vues courantes.
        """
        con = self._require_connection()

        # ------------------------------------------------------------------
        # 1) Historique principal des listings
        # ------------------------------------------------------------------
        con.execute(
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
                updated_at TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_status_symbol
            ON listing_status_history(symbol)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_status_cik
            ON listing_status_history(cik)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_status_company_id
            ON listing_status_history(company_id)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_listing_status_active
            ON listing_status_history(is_active)
            """
        )

        # ------------------------------------------------------------------
        # 2) Historique de l'univers de marché
        # ------------------------------------------------------------------
        con.execute(
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
                updated_at TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_universe_history_listing_id
            ON market_universe_history(listing_id)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_universe_history_symbol
            ON market_universe_history(symbol)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_market_universe_history_active
            ON market_universe_history(is_active)
            """
        )

        # ------------------------------------------------------------------
        # 3) Historique de la référence symbole
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                symbol_reference_id VARCHAR,
                listing_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                cik VARCHAR,
                company_name VARCHAR,
                company_name_clean VARCHAR,
                aliases_json VARCHAR,
                exchange VARCHAR,
                symbol_match_enabled BOOLEAN,
                name_match_enabled BOOLEAN,
                effective_from DATE,
                effective_to DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_listing_id
            ON symbol_reference_history(listing_id)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_symbol
            ON symbol_reference_history(symbol)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_cik
            ON symbol_reference_history(cik)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_company_id
            ON symbol_reference_history(company_id)
            """
        )

        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_symbol_reference_history_active
            ON symbol_reference_history(is_active)
            """
        )

        # ------------------------------------------------------------------
        # 4) Vues courantes
        #
        # On expose des vues *_current pour ne pas casser brutalement
        # les tables existantes du projet.
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE OR REPLACE VIEW market_universe_current AS
            SELECT
                listing_id,
                company_id,
                symbol,
                cik,
                exchange,
                security_type,
                eligible_flag,
                eligible_reason,
                rule_version,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            FROM market_universe_history
            WHERE is_active = TRUE
              AND effective_to IS NULL
            """
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW symbol_reference_current AS
            SELECT
                symbol,
                cik,
                company_name,
                company_name_clean,
                aliases_json,
                exchange,
                symbol_match_enabled,
                name_match_enabled,
                created_at
            FROM symbol_reference_history
            WHERE is_active = TRUE
              AND effective_to IS NULL
            """
        )
