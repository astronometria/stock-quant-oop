from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class LabelEngineSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_return_labels_daily()
        self._create_volatility_labels_daily()

    def _create_return_labels_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS return_labels_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,
                direction_1d INTEGER,
                direction_5d INTEGER,
                direction_20d INTEGER,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_volatility_labels_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS volatility_labels_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                realized_vol_20d DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
