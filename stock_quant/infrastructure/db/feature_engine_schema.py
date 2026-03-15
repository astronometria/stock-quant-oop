from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class FeatureEngineSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_technical_features_daily()
        self._create_research_features_daily()

    def _create_technical_features_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS technical_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close_to_sma_20 DOUBLE,
                rsi_14 DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_research_features_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close_to_sma_20 DOUBLE,
                rsi_14 DOUBLE,
                revenue DOUBLE,
                net_income DOUBLE,
                net_margin DOUBLE,
                debt_to_equity DOUBLE,
                return_on_assets DOUBLE,
                short_interest DOUBLE,
                days_to_cover DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                article_count_1d BIGINT,
                unique_cluster_count_1d BIGINT,
                avg_link_confidence DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

        existing_columns = {
            row[1]
            for row in self.con.execute("PRAGMA table_info('research_features_daily')").fetchall()
        }

        alter_statements = [
            ("short_interest_change_pct", "ALTER TABLE research_features_daily ADD COLUMN short_interest_change_pct DOUBLE"),
            ("short_squeeze_score", "ALTER TABLE research_features_daily ADD COLUMN short_squeeze_score DOUBLE"),
            ("short_pressure_zscore", "ALTER TABLE research_features_daily ADD COLUMN short_pressure_zscore DOUBLE"),
            ("days_to_cover_zscore", "ALTER TABLE research_features_daily ADD COLUMN days_to_cover_zscore DOUBLE"),
        ]
        for column_name, sql in alter_statements:
            if column_name not in existing_columns:
                self.con.execute(sql)
