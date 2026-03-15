from __future__ import annotations

from typing import Any

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbPriceRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def list_allowed_symbols(self) -> list[str]:
        try:
            rows = self.con.execute(
                """
                SELECT DISTINCT symbol
                FROM market_universe
                WHERE include_in_universe = TRUE
                ORDER BY symbol
                """
            ).fetchall()
            return [row[0] for row in rows]
        except Exception as exc:
            raise RepositoryError(f"failed to list allowed symbols: {exc}") from exc

    def upsert_price_history(self, frame) -> int:
        try:
            self.con.execute("DELETE FROM price_history")

            if frame is None or frame.empty:
                return 0

            self.con.register("tmp_price_history_frame", frame)
            try:
                self.con.execute(
                    """
                    INSERT INTO price_history (
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
                        symbol,
                        CAST(price_date AS DATE),
                        CAST(open AS DOUBLE),
                        CAST(high AS DOUBLE),
                        CAST(low AS DOUBLE),
                        CAST(close AS DOUBLE),
                        CAST(volume AS BIGINT),
                        source_name,
                        CAST(ingested_at AS TIMESTAMP)
                    FROM tmp_price_history_frame
                    """
                )
                written = int(self.con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])
            finally:
                self.con.unregister("tmp_price_history_frame")

            return written
        except Exception as exc:
            raise RepositoryError(f"failed to upsert price_history: {exc}") from exc

    def refresh_price_latest(self) -> int:
        try:
            self.con.execute("DELETE FROM price_latest")
            self.con.execute(
                """
                INSERT INTO price_latest (
                    symbol,
                    latest_price_date,
                    close,
                    volume,
                    source_name,
                    updated_at
                )
                SELECT
                    h.symbol,
                    h.price_date AS latest_price_date,
                    h.close,
                    h.volume,
                    h.source_name,
                    CURRENT_TIMESTAMP AS updated_at
                FROM price_history h
                INNER JOIN (
                    SELECT symbol, MAX(price_date) AS max_price_date
                    FROM price_history
                    GROUP BY symbol
                ) latest
                    ON h.symbol = latest.symbol
                   AND h.price_date = latest.max_price_date
                """
            )
            return int(self.con.execute("SELECT COUNT(*) FROM price_latest").fetchone()[0])
        except Exception as exc:
            raise RepositoryError(f"failed to refresh price_latest: {exc}") from exc

    def load_price_source_daily_raw_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                FROM price_source_daily_raw
                ORDER BY symbol, price_date
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "price_date": row[1],
                    "open": row[2],
                    "high": row[3],
                    "low": row[4],
                    "close": row[5],
                    "volume": row[6],
                    "source_name": row[7],
                    "ingested_at": row[8],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_source_daily_raw rows: {exc}") from exc

    def load_price_source_daily_raw_all_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    source_path,
                    asset_class,
                    venue_group,
                    ingested_at
                FROM price_source_daily_raw_all
                ORDER BY symbol, price_date, source_name
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "price_date": row[1],
                    "open": row[2],
                    "high": row[3],
                    "low": row[4],
                    "close": row[5],
                    "volume": row[6],
                    "source_name": row[7],
                    "source_path": row[8],
                    "asset_class": row[9],
                    "venue_group": row[10],
                    "ingested_at": row[11],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_source_daily_raw_all rows: {exc}") from exc
