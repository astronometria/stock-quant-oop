from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from stock_quant.domain.entities.corporate_action import PriceQualityFlag
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbPricesResearchRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_price_history_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT symbol, price_date, open, high, low, close, volume, source_name
                FROM price_history
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
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_history rows: {exc}") from exc

    def load_symbol_instrument_map(self) -> dict[str, str]:
        try:
            rows = self.con.execute(
                """
                SELECT symbol, instrument_id
                FROM instrument_master
                ORDER BY symbol
                """
            ).fetchall()
            return {str(row[0]).strip().upper(): row[1] for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to load symbol instrument map: {exc}") from exc

    def replace_price_bars_unadjusted(self, frame: pd.DataFrame) -> int:
        try:
            self.con.execute("DELETE FROM price_bars_unadjusted")
            if frame.empty:
                return 0

            payload = list(frame.itertuples(index=False, name=None))
            self.con.executemany(
                """
                INSERT INTO price_bars_unadjusted (
                    instrument_id,
                    symbol,
                    bar_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_bars_unadjusted: {exc}") from exc

    def replace_price_bars_adjusted(self, frame: pd.DataFrame) -> int:
        try:
            self.con.execute("DELETE FROM price_bars_adjusted")
            if frame.empty:
                return 0

            payload = list(frame.itertuples(index=False, name=None))
            self.con.executemany(
                """
                INSERT INTO price_bars_adjusted (
                    instrument_id,
                    symbol,
                    bar_date,
                    adj_open,
                    adj_high,
                    adj_low,
                    adj_close,
                    volume,
                    adjustment_factor,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_bars_adjusted: {exc}") from exc

    def replace_price_quality_flags(self, rows: list[PriceQualityFlag]) -> int:
        try:
            self.con.execute("DELETE FROM price_quality_flags")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.price_date,
                    row.flag_type,
                    row.flag_value,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO price_quality_flags (
                    instrument_id,
                    price_date,
                    flag_type,
                    flag_value,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_quality_flags: {exc}") from exc
