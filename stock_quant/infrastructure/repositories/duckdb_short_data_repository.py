from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.short_data import (
    DailyShortVolumeSnapshot,
    ShortFeatureDaily,
    ShortInterestSnapshot,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbShortDataRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_symbol_map(self) -> dict[str, dict[str, str | None]]:
        try:
            rows = self.con.execute(
                """
                SELECT symbol, instrument_id, company_id
                FROM instrument_master
                ORDER BY symbol
                """
            ).fetchall()
            return {
                str(row[0]).strip().upper(): {
                    "instrument_id": row[1],
                    "company_id": row[2],
                }
                for row in rows
            }
        except Exception as exc:
            raise RepositoryError(f"failed to load symbol map: {exc}") from exc

    def load_finra_short_interest_raw_rows(self) -> list[dict[str, Any]]:
        try:
            info = self.con.execute("PRAGMA table_info('finra_short_interest_source_raw')").fetchall()
            column_names = {str(row[1]).strip() for row in info}

            settlement_expr = "settlement_date"
            if "settlement_date" not in column_names and "source_date" in column_names:
                settlement_expr = "source_date"

            days_to_cover_expr = "days_to_cover"
            if "days_to_cover" not in column_names:
                if "short_interest" in column_names and "avg_daily_volume" in column_names:
                    days_to_cover_expr = """
                    CASE
                        WHEN avg_daily_volume IS NOT NULL AND avg_daily_volume <> 0 AND short_interest IS NOT NULL
                        THEN short_interest / avg_daily_volume
                        ELSE NULL
                    END
                    """
                else:
                    days_to_cover_expr = "NULL"

            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    {settlement_expr} AS settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    {days_to_cover_expr} AS days_to_cover,
                    source_file
                FROM finra_short_interest_source_raw
                ORDER BY symbol, settlement_date
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "settlement_date": row[1],
                    "short_interest": row[2],
                    "previous_short_interest": row[3],
                    "avg_daily_volume": row[4],
                    "days_to_cover": row[5],
                    "source_file": row[6],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load finra_short_interest_source_raw: {exc}") from exc

    def load_finra_daily_short_volume_raw_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    trade_date,
                    short_volume,
                    total_volume,
                    source_name
                FROM finra_daily_short_volume_source_raw
                ORDER BY symbol, trade_date
                """
            ).fetchall()
            return [
                {
                    "symbol": row[0],
                    "trade_date": row[1],
                    "short_volume": row[2],
                    "total_volume": row[3],
                    "source_name": row[4],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load finra_daily_short_volume_source_raw: {exc}") from exc

    def replace_daily_short_volume_source_raw(self, rows: list[dict[str, Any]]) -> int:
        try:
            self.con.execute("DELETE FROM finra_daily_short_volume_source_raw")
            if not rows:
                return 0

            payload = [
                (
                    row.get("symbol"),
                    row.get("trade_date"),
                    row.get("short_volume"),
                    row.get("total_volume"),
                    row.get("source_name", "finra"),
                    row.get("ingested_at", datetime.utcnow()),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO finra_daily_short_volume_source_raw (
                    symbol,
                    trade_date,
                    short_volume,
                    total_volume,
                    source_name,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace finra_daily_short_volume_source_raw: {exc}") from exc

    def replace_short_interest_history(self, rows: list[ShortInterestSnapshot]) -> int:
        try:
            self.con.execute("DELETE FROM short_interest_history")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.settlement_date,
                    row.short_interest,
                    row.previous_short_interest,
                    row.avg_daily_volume,
                    row.days_to_cover,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO short_interest_history (
                    instrument_id,
                    company_id,
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace short_interest_history: {exc}") from exc

    def replace_daily_short_volume_history(self, rows: list[DailyShortVolumeSnapshot]) -> int:
        try:
            self.con.execute("DELETE FROM daily_short_volume_history")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.trade_date,
                    row.short_volume,
                    row.total_volume,
                    row.short_volume_ratio,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO daily_short_volume_history (
                    instrument_id,
                    company_id,
                    symbol,
                    trade_date,
                    short_volume,
                    total_volume,
                    short_volume_ratio,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace daily_short_volume_history: {exc}") from exc

    def replace_short_features_daily(self, rows: list[ShortFeatureDaily]) -> int:
        try:
            self.con.execute("DELETE FROM short_features_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.short_interest,
                    row.avg_daily_volume,
                    row.days_to_cover,
                    row.short_volume,
                    row.total_volume,
                    row.short_volume_ratio,
                    row.short_interest_change,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO short_features_daily (
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    short_volume,
                    total_volume,
                    short_volume_ratio,
                    short_interest_change,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace short_features_daily: {exc}") from exc
