from __future__ import annotations

"""
Short-data schema foundation.

Ce module crée la fondation canonique pour :
- finra_daily_short_volume_source_raw
- finra_daily_short_volume_sources
- daily_short_volume_history
- short_features_daily

Objectifs:
- SQL-first
- schéma idempotent
- colonnes PIT explicites
- aucune dépendance au current universe pour l'historique
"""

from typing import Any


class ShortDataSchemaManager:
    """
    Manager minimaliste et idempotent pour la fondation short-data.
    L'API est volontairement simple:
    - ensure_all(con)
    - ensure_schema(con)
    - apply(con)

    Les trois sont supportées pour compatibilité avec les différents scripts.
    """

    def ensure_all(self, con: Any) -> None:
        self._create_finra_daily_short_volume_source_raw(con)
        self._create_finra_daily_short_volume_sources(con)
        self._create_daily_short_volume_history(con)
        self._create_short_features_daily(con)
        self._create_indexes(con)

    def ensure_schema(self, con: Any) -> None:
        self.ensure_all(con)

    def apply(self, con: Any) -> None:
        self.ensure_all(con)

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
                source_file VARCHAR,
                publication_date DATE,
                available_at TIMESTAMP,
                source_name VARCHAR
            )
            """
        )

    def _create_finra_daily_short_volume_sources(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS finra_daily_short_volume_sources (
                source_file VARCHAR,
                publication_date DATE,
                market_code VARCHAR,
                row_count BIGINT,
                loaded_at TIMESTAMP,
                source_name VARCHAR
            )
            """
        )

    def _create_daily_short_volume_history(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_short_volume_history (
                symbol VARCHAR,
                trade_date DATE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_exempt_ratio DOUBLE,
                source_name VARCHAR,
                source_file VARCHAR,
                market_code VARCHAR,
                publication_date DATE,
                available_at TIMESTAMP,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_short_features_daily(self, con: Any) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_features_daily (
                symbol VARCHAR,
                as_of_date DATE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_exempt_ratio DOUBLE,
                short_volume_ratio_20d_avg DOUBLE,
                short_volume_ratio_20d_zscore DOUBLE,
                short_interest BIGINT,
                previous_short_interest BIGINT,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                short_interest_pct_float DOUBLE,
                source_file VARCHAR,
                source_market VARCHAR,
                short_volume_available_at TIMESTAMP,
                short_interest_available_at TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
        )

    def _create_indexes(self, con: Any) -> None:
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
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_short_volume_history_symbol_trade_date
            ON daily_short_volume_history(symbol, trade_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_features_daily_symbol_as_of_date
            ON short_features_daily(symbol, as_of_date)
            """
        )
