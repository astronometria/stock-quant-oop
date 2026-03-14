from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class DatasetBuilderSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_research_dataset_daily()

    def _create_research_dataset_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_dataset_daily (
                dataset_name VARCHAR,
                dataset_version VARCHAR,
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
                article_count_1d BIGINT,
                unique_cluster_count_1d BIGINT,
                avg_link_confidence DOUBLE,
                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,
                direction_1d INTEGER,
                direction_5d INTEGER,
                direction_20d INTEGER,
                realized_vol_20d DOUBLE,
                created_at TIMESTAMP
            )
            """
        )
