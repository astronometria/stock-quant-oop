"""
duckdb_market_universe_history_repository.py

Repository DuckDB pour l'historique de l'univers de marché.

Note importante
---------------

Ce fichier suppose l'existence future de la table :

    market_universe_history

Elle sera créée dans une étape de schéma dédiée.

But
---

Historiser l'éligibilité des listings au lieu de recalculer seulement
un snapshot courant destructif.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from stock_quant.domain.entities.universe_membership_version import UniverseMembershipVersion
from stock_quant.shared.exceptions import RepositoryError


class DuckDbMarketUniverseHistoryRepository:
    """
    Repository de l'historique d'éligibilité à l'univers.
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    def _require_connection(self) -> None:
        if self.con is None:
            raise RepositoryError("DuckDbMarketUniverseHistoryRepository requires an active connection")

    @staticmethod
    def _row_to_entity(row: tuple[Any, ...]) -> UniverseMembershipVersion:
        return UniverseMembershipVersion(
            listing_id=row[0],
            company_id=row[1],
            symbol=row[2],
            cik=row[3],
            exchange=row[4],
            security_type=row[5],
            eligible_flag=row[6],
            eligible_reason=row[7],
            rule_version=row[8],
            effective_from=row[9],
            effective_to=row[10],
            is_active=row[11],
            created_at=row[12],
            updated_at=row[13],
        )

    def find_active_by_listing_id(
        self,
        listing_id: str,
    ) -> UniverseMembershipVersion | None:
        """
        Charge la version active d'univers pour un listing.
        """
        self._require_connection()

        row = self.con.execute(
            """
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
            WHERE listing_id = ?
              AND is_active = TRUE
              AND effective_to IS NULL
            LIMIT 1
            """,
            [listing_id],
        ).fetchone()

        return None if row is None else self._row_to_entity(row)

    def insert_version(
        self,
        entity: UniverseMembershipVersion,
    ) -> None:
        """
        Insère une nouvelle version d'éligibilité.
        """
        self._require_connection()

        self.con.execute(
            """
            INSERT INTO market_universe_history (
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
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entity.listing_id,
                entity.company_id,
                entity.symbol,
                entity.cik,
                entity.exchange,
                entity.security_type,
                entity.eligible_flag,
                entity.eligible_reason,
                entity.rule_version,
                entity.effective_from,
                entity.effective_to,
                entity.is_active,
                entity.created_at,
                entity.updated_at,
            ],
        )

    def close_active_version(
        self,
        *,
        listing_id: str,
        effective_to: date,
        updated_at: datetime | None,
    ) -> None:
        """
        Ferme la version active d'éligibilité.
        """
        self._require_connection()

        self.con.execute(
            """
            UPDATE market_universe_history
            SET
                effective_to = ?,
                is_active = FALSE,
                updated_at = ?
            WHERE listing_id = ?
              AND is_active = TRUE
              AND effective_to IS NULL
            """,
            [
                effective_to,
                updated_at,
                listing_id,
            ],
        )
