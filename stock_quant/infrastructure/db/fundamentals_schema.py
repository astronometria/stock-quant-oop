from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class FundamentalsSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_fundamental_snapshot_quarterly()
        self._create_fundamental_snapshot_annual()
        self._create_fundamental_ttm()
        self._create_fundamental_features_daily()

    def _create_fundamental_snapshot_quarterly(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS fundamental_snapshot_quarterly (
                company_id VARCHAR,
                cik VARCHAR,
                period_type VARCHAR,
                period_end_date DATE,
                available_at TIMESTAMP,
                revenue DOUBLE,
                net_income DOUBLE,
                assets DOUBLE,
                liabilities DOUBLE,
                equity DOUBLE,
                operating_cash_flow DOUBLE,
                shares_outstanding DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_fundamental_snapshot_annual(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS fundamental_snapshot_annual (
                company_id VARCHAR,
                cik VARCHAR,
                period_type VARCHAR,
                period_end_date DATE,
                available_at TIMESTAMP,
                revenue DOUBLE,
                net_income DOUBLE,
                assets DOUBLE,
                liabilities DOUBLE,
                equity DOUBLE,
                operating_cash_flow DOUBLE,
                shares_outstanding DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_fundamental_ttm(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS fundamental_ttm (
                company_id VARCHAR,
                cik VARCHAR,
                period_type VARCHAR,
                period_end_date DATE,
                available_at TIMESTAMP,
                revenue DOUBLE,
                net_income DOUBLE,
                assets DOUBLE,
                liabilities DOUBLE,
                equity DOUBLE,
                operating_cash_flow DOUBLE,
                shares_outstanding DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_fundamental_features_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS fundamental_features_daily (
                company_id VARCHAR,
                as_of_date DATE,
                period_type VARCHAR,
                revenue DOUBLE,
                net_income DOUBLE,
                assets DOUBLE,
                liabilities DOUBLE,
                equity DOUBLE,
                operating_cash_flow DOUBLE,
                shares_outstanding DOUBLE,
                net_margin DOUBLE,
                debt_to_equity DOUBLE,
                return_on_assets DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
