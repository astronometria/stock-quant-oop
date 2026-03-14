from __future__ import annotations

from stock_quant.domain.entities.short_interest import RawShortInterestRecord, ShortInterestRecord, ShortInterestSourceFile
from stock_quant.domain.ports.repositories import ShortInterestRepositoryPort
from stock_quant.infrastructure.db.table_names import (
    FINRA_SHORT_INTEREST_HISTORY,
    FINRA_SHORT_INTEREST_LATEST,
    FINRA_SHORT_INTEREST_SOURCE_RAW,
    FINRA_SHORT_INTEREST_SOURCES,
    MARKET_UNIVERSE,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbShortInterestRepository(ShortInterestRepositoryPort):
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    shares_float,
                    revision_flag,
                    source_market,
                    source_file,
                    source_date
                FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                ORDER BY settlement_date, symbol, source_file
                """
            ).fetchall()
            return [
                RawShortInterestRecord(
                    symbol=row[0],
                    settlement_date=row[1],
                    short_interest=row[2],
                    previous_short_interest=row[3],
                    avg_daily_volume=row[4],
                    shares_float=row[5],
                    revision_flag=row[6],
                    source_market=row[7] or "unknown",
                    source_file=row[8] or "unknown",
                    source_date=row[9],
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw short interest records: {exc}") from exc

    def load_included_symbols(self) -> set[str]:
        try:
            rows = self.con.execute(
                f"""
                SELECT symbol
                FROM {MARKET_UNIVERSE}
                WHERE include_in_universe = TRUE
                """
            ).fetchall()
            return {row[0] for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to load included symbols: {exc}") from exc

    def replace_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        try:
            self.con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_HISTORY}")
            if not entries:
                return 0

            rows = [
                (
                    e.symbol,
                    e.settlement_date,
                    e.short_interest,
                    e.previous_short_interest,
                    e.avg_daily_volume,
                    e.days_to_cover,
                    e.shares_float,
                    e.short_interest_pct_float,
                    e.revision_flag,
                    e.source_market,
                    e.source_file,
                    e.ingested_at,
                )
                for e in entries
            ]
            self.con.executemany(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_HISTORY} (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace finra_short_interest_history: {exc}") from exc

    def rebuild_short_interest_latest(self) -> int:
        try:
            self.con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_LATEST}")
            self.con.execute(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_LATEST} (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    updated_at
                )
                SELECT
                    h.symbol,
                    h.settlement_date,
                    h.short_interest,
                    h.previous_short_interest,
                    h.avg_daily_volume,
                    h.days_to_cover,
                    h.shares_float,
                    h.short_interest_pct_float,
                    h.revision_flag,
                    h.source_market,
                    h.source_file,
                    CURRENT_TIMESTAMP
                FROM {FINRA_SHORT_INTEREST_HISTORY} h
                INNER JOIN (
                    SELECT symbol, MAX(settlement_date) AS max_settlement_date
                    FROM {FINRA_SHORT_INTEREST_HISTORY}
                    GROUP BY symbol
                ) latest
                    ON h.symbol = latest.symbol
                   AND h.settlement_date = latest.max_settlement_date
                """
            )
            count = self.con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}").fetchone()[0]
            return int(count)
        except Exception as exc:
            raise RepositoryError(f"failed to rebuild finra_short_interest_latest: {exc}") from exc

    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        try:
            self.con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_SOURCES}")
            if not entries:
                return 0

            rows = [
                (
                    e.source_file,
                    e.source_market,
                    e.source_date,
                    e.row_count,
                    e.loaded_at,
                )
                for e in entries
            ]
            self.con.executemany(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_SOURCES} (
                    source_file,
                    source_market,
                    source_date,
                    row_count,
                    loaded_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace finra_short_interest_sources: {exc}") from exc
