"""
listing_history_schema.py

Schema manager pour les tables historisées des listings.

Objectif
--------

Introduire une couche survivorship-bias-aware pour les symboles et listings.

Pourquoi cette table existe
---------------------------

Les marchés changent continuellement :

- nouveaux listings
- delistings
- changement de ticker
- migration d'exchange
- changement de structure

Si on conserve seulement un snapshot courant (comme symbol_reference),
on introduit du survivorship bias.

Cette table conserve l'historique complet des listings.

Concept clé
-----------

Chaque ligne représente une VERSION d'un listing.

Une nouvelle ligne est créée si :

- ticker change
- exchange change
- security_type change
- company change
- delisting détecté

Colonnes importantes
--------------------

listing_id
    identifiant technique du listing

company_id
    identifiant société (souvent CIK)

symbol
    ticker

exchange
    NYSE / NASDAQ / etc

effective_from
    date à partir de laquelle cette version est valide

effective_to
    date de fin de validité

is_active
    listing actif ou non

first_seen_at
    première apparition du listing dans les données

last_seen_at
    dernière apparition du listing dans les données
"""

from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ListingHistorySchemaManager:
    """
    Initialise le schéma historisé des listings.
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def initialize(self) -> None:
        """
        Création de la table listing_status_history.

        Cette table est append-only avec fermeture d'intervalle.
        """
        if self.uow.connection is None:
            raise RuntimeError("DuckDbUnitOfWork has no active connection")

        con = self.uow.connection

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
            CREATE INDEX IF NOT EXISTS idx_listing_status_active
            ON listing_status_history(is_active)
            """
        )
