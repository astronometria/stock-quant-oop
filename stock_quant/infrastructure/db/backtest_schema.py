from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class BacktestSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_experiment_results_daily()

    def _create_experiment_results_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS experiment_results_daily (
                experiment_name VARCHAR,
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                signal_value DOUBLE,
                selected_flag BOOLEAN,
                realized_return_1d DOUBLE,
                created_at TIMESTAMP
            )
            """
        )
