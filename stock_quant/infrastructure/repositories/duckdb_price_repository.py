from __future__ import annotations

from datetime import datetime
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

    def load_price_source_daily_raw_rows(self) -> list[dict[str, Any]]:
        """
        Read canonical daily raw prices without filtering by the current universe.

        Important:
        - No join to market_universe
        - No WHERE include_in_universe = TRUE
        - Historical raw tables must stay historically complete
        """
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
        """
        Read the fully staged raw-all table without current-universe filtering.

        This table is useful for diagnostics / source arbitration and must not
        depend on the current market_universe snapshot.
        """
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

    def replace_price_history(self, rows: list[dict[str, Any]]) -> int:
        """
        Replace canonical price_history from already prepared rows.

        This remains a historical table and should not be scoped by the current
        universe membership.
        """
        try:
            self.con.execute("DELETE FROM price_history")
            if not rows:
                return 0

            payload = [
                (
                    row.get("symbol"),
                    row.get("price_date"),
                    row.get("open"),
                    row.get("high"),
                    row.get("low"),
                    row.get("close"),
                    row.get("volume"),
                    row.get("source_name"),
                    row.get("ingested_at") or datetime.utcnow(),
                )
                for row in rows
            ]

            self.con.executemany(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_history: {exc}") from exc

    def replace_price_latest(self, rows: list[dict[str, Any]]) -> int:
        """
        Replace latest-price snapshot.

        This snapshot may later be filtered by active universe elsewhere, but the
        repository itself should not impose current-universe survivorship bias.
        """
        try:
            self.con.execute("DELETE FROM price_latest")
            if not rows:
                return 0

            payload = [
                (
                    row.get("symbol"),
                    row.get("latest_price_date"),
                    row.get("close"),
                    row.get("volume"),
                    row.get("source_name"),
                    row.get("updated_at") or datetime.utcnow(),
                )
                for row in rows
            ]

            self.con.executemany(
                """
                INSERT INTO price_latest (
                    symbol,
                    latest_price_date,
                    close,
                    volume,
                    source_name,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_latest: {exc}") from exc
