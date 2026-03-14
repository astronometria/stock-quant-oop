from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.company import CompanyMaster
from stock_quant.domain.entities.instrument import InstrumentMaster
from stock_quant.domain.entities.ticker_history import IdentifierMapEntry, TickerHistoryEntry
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbMasterDataRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_market_universe_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    company_name,
                    cik,
                    exchange_raw,
                    exchange_normalized,
                    security_type,
                    include_in_universe,
                    as_of_date
                FROM market_universe
                ORDER BY symbol
                """
            ).fetchall()

            out: list[dict[str, Any]] = []
            for row in rows:
                out.append(
                    {
                        "symbol": row[0],
                        "company_name": row[1],
                        "cik": row[2],
                        "exchange_raw": row[3],
                        "exchange_normalized": row[4],
                        "security_type": row[5],
                        "include_in_universe": row[6],
                        "as_of_date": row[7],
                    }
                )
            return out
        except Exception as exc:
            raise RepositoryError(f"failed to load market_universe rows: {exc}") from exc

    def load_symbol_reference_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    cik,
                    company_name,
                    exchange
                FROM symbol_reference
                ORDER BY symbol
                """
            ).fetchall()

            out: list[dict[str, Any]] = []
            for row in rows:
                out.append(
                    {
                        "symbol": row[0],
                        "cik": row[1],
                        "company_name": row[2],
                        "exchange": row[3],
                    }
                )
            return out
        except Exception as exc:
            raise RepositoryError(f"failed to load symbol_reference rows: {exc}") from exc

    def replace_company_master(self, rows: list[CompanyMaster]) -> int:
        try:
            self.con.execute("DELETE FROM company_master")
            if not rows:
                return 0
            payload = [
                (
                    row.company_id,
                    row.company_name,
                    row.cik,
                    row.issuer_name_normalized,
                    row.country_code,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO company_master (
                    company_id,
                    company_name,
                    cik,
                    issuer_name_normalized,
                    country_code,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace company_master: {exc}") from exc

    def replace_instrument_master(self, rows: list[InstrumentMaster]) -> int:
        try:
            self.con.execute("DELETE FROM instrument_master")
            if not rows:
                return 0
            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.exchange,
                    row.security_type,
                    row.share_class,
                    row.is_active,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO instrument_master (
                    instrument_id,
                    company_id,
                    symbol,
                    exchange,
                    security_type,
                    share_class,
                    is_active,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace instrument_master: {exc}") from exc

    def replace_ticker_history(self, rows: list[TickerHistoryEntry]) -> int:
        try:
            self.con.execute("DELETE FROM ticker_history")
            if not rows:
                return 0
            payload = [
                (
                    row.instrument_id,
                    row.symbol,
                    row.exchange,
                    row.valid_from,
                    row.valid_to,
                    row.is_current,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO ticker_history (
                    instrument_id,
                    symbol,
                    exchange,
                    valid_from,
                    valid_to,
                    is_current,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace ticker_history: {exc}") from exc

    def replace_identifier_map(self, rows: list[IdentifierMapEntry]) -> int:
        try:
            self.con.execute("DELETE FROM instrument_identifier_map")
            if not rows:
                return 0
            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.identifier_type,
                    row.identifier_value,
                    row.is_primary,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO instrument_identifier_map (
                    instrument_id,
                    company_id,
                    identifier_type,
                    identifier_value,
                    is_primary,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace instrument_identifier_map: {exc}") from exc
