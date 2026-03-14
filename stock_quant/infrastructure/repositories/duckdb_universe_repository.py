from __future__ import annotations

from datetime import datetime

from stock_quant.domain.entities.universe import RawUniverseCandidate, UniverseConflict, UniverseEntry
from stock_quant.domain.ports.repositories import UniverseRepositoryPort
from stock_quant.infrastructure.db.table_names import MARKET_UNIVERSE, MARKET_UNIVERSE_CONFLICTS, SYMBOL_REFERENCE_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbUniverseRepository(UniverseRepositoryPort):
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def replace_universe(self, entries: list[UniverseEntry]) -> int:
        try:
            self.con.execute(f"DELETE FROM {MARKET_UNIVERSE}")
            if not entries:
                return 0
            rows = [
                (
                    e.symbol,
                    e.company_name,
                    e.cik,
                    e.exchange_raw,
                    e.exchange_normalized,
                    e.security_type,
                    e.include_in_universe,
                    e.exclusion_reason,
                    e.is_common_stock,
                    e.is_adr,
                    e.is_etf,
                    e.is_preferred,
                    e.is_warrant,
                    e.is_right,
                    e.is_unit,
                    e.source_name,
                    e.as_of_date,
                    e.created_at,
                )
                for e in entries
            ]
            self.con.executemany(
                f"""
                INSERT INTO {MARKET_UNIVERSE} (
                    symbol, company_name, cik, exchange_raw, exchange_normalized, security_type,
                    include_in_universe, exclusion_reason, is_common_stock, is_adr, is_etf,
                    is_preferred, is_warrant, is_right, is_unit, source_name, as_of_date, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace universe table: {exc}") from exc

    def replace_conflicts(self, conflicts: list[UniverseConflict]) -> int:
        try:
            self.con.execute(f"DELETE FROM {MARKET_UNIVERSE_CONFLICTS}")
            if not conflicts:
                return 0
            rows = [
                (
                    c.symbol,
                    c.chosen_source,
                    c.rejected_source,
                    c.reason,
                    c.payload_json,
                    c.created_at,
                )
                for c in conflicts
            ]
            self.con.executemany(
                f"""
                INSERT INTO {MARKET_UNIVERSE_CONFLICTS} (
                    symbol, chosen_source, rejected_source, reason, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace universe conflicts table: {exc}") from exc

    def fetch_all_universe_entries(self) -> list[UniverseEntry]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    symbol, company_name, cik, exchange_raw, exchange_normalized, security_type,
                    include_in_universe, exclusion_reason, is_common_stock, is_adr, is_etf,
                    is_preferred, is_warrant, is_right, is_unit, source_name, as_of_date, created_at
                FROM {MARKET_UNIVERSE}
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
            raise RepositoryError(f"failed to fetch universe entries: {exc}") from exc

    def load_raw_candidates(self) -> list[RawUniverseCandidate]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    company_name,
                    cik,
                    exchange_raw,
                    security_type_raw,
                    source_name,
                    as_of_date
                FROM {SYMBOL_REFERENCE_SOURCE_RAW}
                ORDER BY symbol, source_name, as_of_date
                """
            ).fetchall()

            out: list[RawUniverseCandidate] = []
            for row in rows:
                out.append(
                    RawUniverseCandidate(
                        symbol=row[0],
                        company_name=row[1],
                        cik=row[2],
                        exchange_raw=row[3],
                        security_type_raw=row[4],
                        source_name=row[5] or "unknown_source",
                        as_of_date=row[6],
                    )
                )
            return out
        except Exception as exc:
            raise RepositoryError(f"failed to load raw universe candidates: {exc}") from exc
