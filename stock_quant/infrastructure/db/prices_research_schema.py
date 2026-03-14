from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class PricesResearchSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_corporate_actions()
        self._create_dividends()
        self._create_splits()
        self._create_price_bars_unadjusted()
        self._create_price_bars_adjusted()
        self._create_price_quality_flags()

    def _create_corporate_actions(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS corporate_actions (
                instrument_id VARCHAR,
                action_type VARCHAR,
                action_date DATE,
                effective_date DATE,
                value_numeric DOUBLE,
                value_text VARCHAR,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_dividends(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS dividends (
                instrument_id VARCHAR,
                ex_date DATE,
                payment_date DATE,
                amount DOUBLE,
                currency VARCHAR,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_splits(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS splits (
                instrument_id VARCHAR,
                split_date DATE,
                split_from DOUBLE,
                split_to DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_price_bars_unadjusted(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bars_unadjusted (
                instrument_id VARCHAR,
                symbol VARCHAR,
                bar_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_price_bars_adjusted(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS price_bars_adjusted (
                instrument_id VARCHAR,
                symbol VARCHAR,
                bar_date DATE,
                adj_open DOUBLE,
                adj_high DOUBLE,
                adj_low DOUBLE,
                adj_close DOUBLE,
                volume BIGINT,
                adjustment_factor DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_price_quality_flags(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS price_quality_flags (
                instrument_id VARCHAR,
                price_date DATE,
                flag_type VARCHAR,
                flag_value VARCHAR,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
