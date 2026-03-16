"""
duckdb_symbol_reference_history_repository.py

Repository DuckDB pour l'historique de référence symbole.

Note importante
---------------

Ce repository suppose l'existence future de la table :

    symbol_reference_history

Elle sera créée dans une prochaine étape de schéma.

But
---

Permettre :
- le matching PIT
- la dérivation d'une vue courante `symbol_reference`
- la conservation de l'historique des symboles, y compris delistings,
  ticker changes et réutilisation de symboles
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from stock_quant.domain.entities.symbol_reference_version import SymbolReferenceVersion
from stock_quant.shared.exceptions import RepositoryError


class DuckDbSymbolReferenceHistoryRepository:
    """
    Repository de l'historique des références symbole.
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    def _require_connection(self) -> None:
        if self.con is None:
            raise RepositoryError("DuckDbSymbolReferenceHistoryRepository requires an active connection")

    @staticmethod
    def _row_to_entity(row: tuple[Any, ...]) -> SymbolReferenceVersion:
        return SymbolReferenceVersion(
            symbol_reference_id=row[0],
            listing_id=row[1],
            company_id=row[2],
            symbol=row[3],
            cik=row[4],
            company_name=row[5],
            company_name_clean=row[6],
            aliases_json=row[7],
            exchange=row[8],
            symbol_match_enabled=row[9],
            name_match_enabled=row[10],
            effective_from=row[11],
            effective_to=row[12],
            is_active=row[13],
            created_at=row[14],
            updated_at=row[15],
        )

    def find_active_by_listing_id(
        self,
        listing_id: str,
    ) -> SymbolReferenceVersion | None:
        """
        Charge la version active de symbol_reference pour un listing.
        """
        self._require_connection()

        row = self.con.execute(
            """
            SELECT
                symbol_reference_id,
                listing_id,
                company_id,
                symbol,
                cik,
                company_name,
                company_name_clean,
                aliases_json,
                exchange,
                symbol_match_enabled,
                name_match_enabled,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            FROM symbol_reference_history
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
        entity: SymbolReferenceVersion,
    ) -> None:
        """
        Insère une nouvelle version historisée de symbol reference.
        """
        self._require_connection()

        self.con.execute(
            """
            INSERT INTO symbol_reference_history (
                symbol_reference_id,
                listing_id,
                company_id,
                symbol,
                cik,
                company_name,
                company_name_clean,
                aliases_json,
                exchange,
                symbol_match_enabled,
                name_match_enabled,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                entity.symbol_reference_id,
                entity.listing_id,
                entity.company_id,
                entity.symbol,
                entity.cik,
                entity.company_name,
                entity.company_name_clean,
                entity.aliases_json,
                entity.exchange,
                entity.symbol_match_enabled,
                entity.name_match_enabled,
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
        Ferme la version active de symbol_reference pour un listing.
        """
        self._require_connection()

        self.con.execute(
            """
            UPDATE symbol_reference_history
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
