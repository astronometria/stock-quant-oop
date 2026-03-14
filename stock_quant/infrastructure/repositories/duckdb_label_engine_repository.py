from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.research_label import ReturnLabelDaily, VolatilityLabelDaily
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbLabelEngineRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_price_bars_adjusted_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT instrument_id, symbol, bar_date, adj_close
                FROM price_bars_adjusted
                ORDER BY instrument_id, bar_date
                """
            ).fetchall()
            return [
                {
                    "instrument_id": row[0],
                    "symbol": row[1],
                    "bar_date": row[2],
                    "adj_close": row[3],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_bars_adjusted rows: {exc}") from exc

    def load_instrument_company_map(self) -> dict[str, str | None]:
        try:
            rows = self.con.execute(
                """
                SELECT instrument_id, company_id
                FROM instrument_master
                ORDER BY instrument_id
                """
            ).fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to load instrument company map: {exc}") from exc

    def replace_return_labels_daily(self, rows: list[ReturnLabelDaily]) -> int:
        try:
            self.con.execute("DELETE FROM return_labels_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.fwd_return_1d,
                    row.fwd_return_5d,
                    row.fwd_return_20d,
                    row.direction_1d,
                    row.direction_5d,
                    row.direction_20d,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO return_labels_daily (
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    fwd_return_1d,
                    fwd_return_5d,
                    fwd_return_20d,
                    direction_1d,
                    direction_5d,
                    direction_20d,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace return_labels_daily: {exc}") from exc

    def replace_volatility_labels_daily(self, rows: list[VolatilityLabelDaily]) -> int:
        try:
            self.con.execute("DELETE FROM volatility_labels_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.realized_vol_20d,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO volatility_labels_daily (
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    realized_vol_20d,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace volatility_labels_daily: {exc}") from exc
