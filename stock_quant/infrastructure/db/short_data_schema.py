from __future__ import annotations

"""
Canonical short-data schema manager.

Purpose
-------
Creates and evolves the SQL-first foundation for:
- finra_daily_short_volume_source_raw
- daily_short_volume_history
- short_features_daily

Design notes
------------
- Raw tables preserve source fidelity and source metadata.
- Canonical history tables are point-in-time safe and historical-safe.
- Derived tables are research-facing and must only be joined using available_at.
"""

from typing import Any


class ShortDataSchemaManager:
    """Creates and evolves the short-data schema in DuckDB."""

    def ensure_all(self, con: Any) -> None:
        self._create_finra_daily_short_volume_source_raw(con)
        self._create_daily_short_volume_history(con)
        self._create_short_features_daily(con)

        self._evolve_finra_daily_short_volume_source_raw(con)
        self._evolve_daily_short_volume_history(con)
        self._evolve_short_features_daily(con)

    def _create_finra_daily_short_volume_source_raw(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS finra_daily_short_volume_source_raw (
                trade_date DATE,
                symbol VARCHAR,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                market_code VARCHAR,
                source_name VARCHAR,
                source_file VARCHAR,
                publication_date DATE,
                available_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_finra_daily_short_volume_source_raw_symbol_trade_date
            ON finra_daily_short_volume_source_raw(symbol, trade_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_finra_daily_short_volume_source_raw_available_at
            ON finra_daily_short_volume_source_raw(available_at)
            """
        )

    def _create_daily_short_volume_history(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_short_volume_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                market_code VARCHAR,
                source_name VARCHAR,
                source_file VARCHAR,
                publication_date DATE,
                available_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_short_volume_history_symbol_trade_date
            ON daily_short_volume_history(symbol, trade_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_short_volume_history_instrument_trade_date
            ON daily_short_volume_history(instrument_id, trade_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_short_volume_history_available_at
            ON daily_short_volume_history(available_at)
            """
        )

    def _create_short_features_daily(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest DOUBLE,
                previous_short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                shares_float DOUBLE,
                short_interest_pct_float DOUBLE,
                short_interest_change DOUBLE,
                short_interest_change_pct DOUBLE,
                short_volume_20d_avg DOUBLE,
                short_volume_ratio_20d_avg DOUBLE,
                days_to_cover_zscore DOUBLE,
                short_pressure_zscore DOUBLE,
                short_squeeze_score DOUBLE,
                max_source_available_at TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_features_daily_symbol_asof
            ON short_features_daily(symbol, as_of_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_features_daily_instrument_asof
            ON short_features_daily(instrument_id, as_of_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_features_daily_available_at
            ON short_features_daily(max_source_available_at)
            """
        )

    def _column_exists(self, con: Any, table_name: str, column_name: str) -> bool:
        rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return any(str(row[1]).strip().lower() == column_name.lower() for row in rows)

    def _add_column_if_missing(self, con: Any, table_name: str, column_name: str, column_type: str) -> None:
        if not self._column_exists(con, table_name, column_name):
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def _evolve_finra_daily_short_volume_source_raw(self, con: Any) -> None:
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "short_exempt_volume", "DOUBLE")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "market_code", "VARCHAR")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "source_file", "VARCHAR")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "publication_date", "DATE")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "available_at", "TIMESTAMP")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "ingested_at", "TIMESTAMP")

    def _evolve_daily_short_volume_history(self, con: Any) -> None:
        self._add_column_if_missing(con, "daily_short_volume_history", "instrument_id", "VARCHAR")
        self._add_column_if_missing(con, "daily_short_volume_history", "company_id", "VARCHAR")
        self._add_column_if_missing(con, "daily_short_volume_history", "short_exempt_volume", "DOUBLE")
        self._add_column_if_missing(con, "daily_short_volume_history", "short_volume_ratio", "DOUBLE")
        self._add_column_if_missing(con, "daily_short_volume_history", "market_code", "VARCHAR")
        self._add_column_if_missing(con, "daily_short_volume_history", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "daily_short_volume_history", "source_file", "VARCHAR")
        self._add_column_if_missing(con, "daily_short_volume_history", "publication_date", "DATE")
        self._add_column_if_missing(con, "daily_short_volume_history", "available_at", "TIMESTAMP")
        self._add_column_if_missing(con, "daily_short_volume_history", "ingested_at", "TIMESTAMP")

    def _evolve_short_features_daily(self, con: Any) -> None:
        self._add_column_if_missing(con, "short_features_daily", "instrument_id", "VARCHAR")
        self._add_column_if_missing(con, "short_features_daily", "company_id", "VARCHAR")
        self._add_column_if_missing(con, "short_features_daily", "short_exempt_volume", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_volume_ratio", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_interest", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "previous_short_interest", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "avg_daily_volume", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "days_to_cover", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "shares_float", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_interest_pct_float", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_interest_change", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_interest_change_pct", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_volume_20d_avg", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_volume_ratio_20d_avg", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "days_to_cover_zscore", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_pressure_zscore", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "short_squeeze_score", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "max_source_available_at", "TIMESTAMP")
        self._add_column_if_missing(con, "short_features_daily", "created_at", "TIMESTAMP")
        self._add_column_if_missing(con, "short_features_daily", "updated_at", "TIMESTAMP")
