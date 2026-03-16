from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.entities.universe import UniverseEntry
from stock_quant.domain.ports.repositories import SymbolReferenceRepositoryPort
from stock_quant.infrastructure.db.table_names import MARKET_UNIVERSE, SYMBOL_REFERENCE
from stock_quant.shared.exceptions import RepositoryError


class DuckDbSymbolReferenceRepository(SymbolReferenceRepositoryPort):
    """
    Repository DuckDB de la table `symbol_reference`.

    Convention de refactor
    ----------------------
    Ce repository prend désormais une connexion DuckDB active (`con`).

    Pourquoi
    --------
    - aligner tout le repo sur une seule convention
    - garder le UnitOfWork hors des repositories
    - simplifier les tests et l'injection de dépendances
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    def _require_connection(self):
        """
        Vérifie la disponibilité d'une connexion active.
        """
        if self.con is None:
            raise RepositoryError("active DB connection is required")
        return self.con

    def replace_symbol_reference(self, entries: list[SymbolReferenceEntry]) -> int:
        """
        Remplace complètement le contenu de `symbol_reference`.

        Note :
        - comportement volontairement destructif pour la table courante
        - l'historisation PIT vit dans la nouvelle couche history
        """
        con = self._require_connection()

        try:
            con.execute(f"DELETE FROM {SYMBOL_REFERENCE}")

            if not entries:
                return 0

            rows = [
                (
                    e.symbol,
                    e.cik,
                    e.company_name,
                    e.company_name_clean,
                    e.aliases_json,
                    e.exchange,
                    e.source_name,
                    e.symbol_match_enabled,
                    e.name_match_enabled,
                    e.created_at,
                )
                for e in entries
            ]

            con.executemany(
                f"""
                INSERT INTO {SYMBOL_REFERENCE} (
                    symbol,
                    cik,
                    company_name,
                    company_name_clean,
                    aliases_json,
                    exchange,
                    source_name,
                    symbol_match_enabled,
                    name_match_enabled,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace symbol_reference: {exc}") from exc

    def load_included_universe_entries(self) -> list[UniverseEntry]:
        """
        Charge les lignes de `market_universe` incluses dans l'univers.

        Ces lignes servent ensuite à construire la table courante
        `symbol_reference`.
        """
        con = self._require_connection()

        try:
            rows = con.execute(
                f"""
                SELECT
                    symbol,
                    company_name,
                    cik,
                    exchange_raw,
                    exchange_normalized,
                    security_type,
                    include_in_universe,
                    exclusion_reason,
                    is_common_stock,
                    is_adr,
                    is_etf,
                    is_preferred,
                    is_warrant,
                    is_right,
                    is_unit,
                    source_name,
                    as_of_date,
                    created_at
                FROM {MARKET_UNIVERSE}
                WHERE include_in_universe = TRUE
                ORDER BY symbol
                """
            ).fetchall()

            out: list[UniverseEntry] = []
            for row in rows:
                out.append(
                    UniverseEntry(
                        symbol=row[0],
                        company_name=row[1],
                        cik=row[2],
                        exchange_raw=row[3],
                        exchange_normalized=row[4],
                        security_type=row[5],
                        include_in_universe=row[6],
                        exclusion_reason=row[7],
                        is_common_stock=row[8],
                        is_adr=row[9],
                        is_etf=row[10],
                        is_preferred=row[11],
                        is_warrant=row[12],
                        is_right=row[13],
                        is_unit=row[14],
                        source_name=row[15],
                        as_of_date=row[16],
                        created_at=row[17] or datetime.utcnow(),
                    )
                )

            return out
        except Exception as exc:
            raise RepositoryError(f"failed to load included universe entries: {exc}") from exc
