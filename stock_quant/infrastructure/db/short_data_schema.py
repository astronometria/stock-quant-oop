from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ShortDataSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_short_interest_history()
        self._create_daily_short_volume_history()
        self._create_short_features_daily()
        self._create_finra_daily_short_volume_source_raw()

    def _create_short_interest_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_interest_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                settlement_date DATE,
                short_interest DOUBLE,
                previous_short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_daily_short_volume_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_short_volume_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                short_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_short_features_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                short_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

        existing_columns = {
            row[1]
            for row in self.con.execute("PRAGMA table_info('short_features_daily')").fetchall()
        }

        alter_statements = [
            ("short_interest_change_pct", "ALTER TABLE short_features_daily ADD COLUMN short_interest_change_pct DOUBLE"),
            ("short_squeeze_score", "ALTER TABLE short_features_daily ADD COLUMN short_squeeze_score DOUBLE"),
            ("short_pressure_zscore", "ALTER TABLE short_features_daily ADD COLUMN short_pressure_zscore DOUBLE"),
            ("days_to_cover_zscore", "ALTER TABLE short_features_daily ADD COLUMN days_to_cover_zscore DOUBLE"),
        ]
        for column_name, sql in alter_statements:
            if column_name not in existing_columns:
                self.con.execute(sql)

    def _create_finra_daily_short_volume_source_raw(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS finra_daily_short_volume_source_raw (
                symbol VARCHAR,
                trade_date DATE,
                short_volume DOUBLE,
                total_volume DOUBLE,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )
