from __future__ import annotations

from stock_quant.domain.entities.prices import PriceBar, RawPriceBar
from stock_quant.domain.ports.repositories import PriceRepositoryPort
from stock_quant.infrastructure.db.table_names import (
    MARKET_UNIVERSE,
    PRICE_HISTORY,
    PRICE_LATEST,
    PRICE_SOURCE_DAILY_RAW,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbPriceRepository(PriceRepositoryPort):
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    # --- Legacy interface kept for compatibility/tests if needed ---

    def load_raw_price_bars(self) -> list[RawPriceBar]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name
                FROM {PRICE_SOURCE_DAILY_RAW}
                ORDER BY symbol, price_date
                """
            ).fetchall()
            return [
                RawPriceBar(
                    symbol=row[0],
                    price_date=row[1],
                    open=row[2],
                    high=row[3],
                    low=row[4],
                    close=row[5],
                    volume=row[6],
                    source_name=row[7] or "unknown_source",
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw price bars: {exc}") from exc

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

    def replace_price_history(self, entries: list[PriceBar]) -> int:
        try:
            self.con.execute(f"DELETE FROM {PRICE_HISTORY}")

            if not entries:
                return 0

            rows = [
                (
                    e.symbol,
                    e.price_date,
                    e.open,
                    e.high,
                    e.low,
                    e.close,
                    e.volume,
                    e.source_name,
                    e.ingested_at,
                )
                for e in entries
            ]

            self.con.executemany(
                f"""
                INSERT INTO {PRICE_HISTORY} (
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_history: {exc}") from exc

    # --- SQL-first fast path for production-sized loads ---

    def count_raw_price_bars(self) -> int:
        try:
            return int(
                self.con.execute(
                    f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}"
                ).fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to count raw price bars: {exc}") from exc

    def count_included_symbols(self) -> int:
        try:
            return int(
                self.con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {MARKET_UNIVERSE}
                    WHERE include_in_universe = TRUE
                    """
                ).fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to count included symbols: {exc}") from exc

    def count_skipped_not_in_universe(self) -> int:
        try:
            return int(
                self.con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {PRICE_SOURCE_DAILY_RAW} r
                    LEFT JOIN {MARKET_UNIVERSE} mu
                        ON r.symbol = mu.symbol
                       AND mu.include_in_universe = TRUE
                    WHERE mu.symbol IS NULL
                    """
                ).fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to count skipped_not_in_universe: {exc}") from exc

    def count_skipped_invalid_on_included_symbols(self) -> int:
        try:
            return int(
                self.con.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {PRICE_SOURCE_DAILY_RAW} r
                    INNER JOIN {MARKET_UNIVERSE} mu
                        ON r.symbol = mu.symbol
                       AND mu.include_in_universe = TRUE
                    WHERE
                        r.price_date IS NULL
                        OR r.close IS NULL
                        OR COALESCE(r.open, r.close) < 0
                        OR COALESCE(r.high, r.close) < 0
                        OR COALESCE(r.low, r.close) < 0
                        OR r.close < 0
                        OR COALESCE(r.volume, 0) < 0
                        OR COALESCE(r.high, r.close) < COALESCE(r.low, r.close)
                    """
                ).fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to count skipped_invalid: {exc}") from exc

    def replace_price_history_from_raw_sql(self) -> int:
        try:
            self.con.execute(f"DELETE FROM {PRICE_HISTORY}")

            self.con.execute(
                f"""
                INSERT INTO {PRICE_HISTORY} (
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                )
                SELECT
                    r.symbol,
                    r.price_date,
                    CAST(COALESCE(r.open, r.close) AS DOUBLE) AS open,
                    CAST(COALESCE(r.high, r.close) AS DOUBLE) AS high,
                    CAST(COALESCE(r.low, r.close) AS DOUBLE) AS low,
                    CAST(r.close AS DOUBLE) AS close,
                    CAST(COALESCE(r.volume, 0) AS BIGINT) AS volume,
                    r.source_name,
                    CURRENT_TIMESTAMP AS ingested_at
                FROM {PRICE_SOURCE_DAILY_RAW} r
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON r.symbol = mu.symbol
                   AND mu.include_in_universe = TRUE
                WHERE
                    r.price_date IS NOT NULL
                    AND r.close IS NOT NULL
                    AND COALESCE(r.open, r.close) >= 0
                    AND COALESCE(r.high, r.close) >= 0
                    AND COALESCE(r.low, r.close) >= 0
                    AND r.close >= 0
                    AND COALESCE(r.volume, 0) >= 0
                    AND COALESCE(r.high, r.close) >= COALESCE(r.low, r.close)
                """
            )

            count = self.con.execute(
                f"SELECT COUNT(*) FROM {PRICE_HISTORY}"
            ).fetchone()[0]
            return int(count)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_history from raw SQL: {exc}") from exc

    def rebuild_price_latest(self) -> int:
        try:
            self.con.execute(f"DELETE FROM {PRICE_LATEST}")

            self.con.execute(
                f"""
                INSERT INTO {PRICE_LATEST} (
                    symbol,
                    latest_price_date,
                    close,
                    volume,
                    source_name,
                    updated_at
                )
                SELECT
                    ph.symbol,
                    ph.price_date AS latest_price_date,
                    ph.close,
                    ph.volume,
                    ph.source_name,
                    CURRENT_TIMESTAMP
                FROM {PRICE_HISTORY} ph
                INNER JOIN (
                    SELECT symbol, MAX(price_date) AS max_price_date
                    FROM {PRICE_HISTORY}
                    GROUP BY symbol
                ) latest
                    ON ph.symbol = latest.symbol
                   AND ph.price_date = latest.max_price_date
                """
            )

            count = self.con.execute(
                f"SELECT COUNT(*) FROM {PRICE_LATEST}"
            ).fetchone()[0]
            return int(count)
        except Exception as exc:
            raise RepositoryError(f"failed to rebuild price_latest: {exc}") from exc
