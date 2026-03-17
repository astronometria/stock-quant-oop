from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class BacktestSchemaManager:
    """
    Schéma du moteur de backtest recherche.

    Tables :
    - experiment_results_daily : compat existante
    - backtest_runs            : résumé global JSON
    - backtest_positions       : positions quotidiennes sélectionnées
    - backtest_daily           : agrégats journaliers du backtest
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_experiment_results_daily()
        self._create_backtest_runs()
        self._create_backtest_positions()
        self._create_backtest_daily()

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

    def _create_backtest_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                backtest_name VARCHAR,
                status VARCHAR,
                dataset_version_id BIGINT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                metrics_json VARCHAR,
                config_json VARCHAR,
                error_message VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_backtest_positions(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_positions (
                backtest_name VARCHAR,
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                signal_column VARCHAR,
                label_column VARCHAR,
                signal_value DOUBLE,
                realized_return DOUBLE,
                rank_position INTEGER,
                weight DOUBLE,
                created_at TIMESTAMP
            )
            """
        )

    def _create_backtest_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_daily (
                backtest_name VARCHAR,
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                as_of_date DATE,
                signal_column VARCHAR,
                label_column VARCHAR,
                selected_count INTEGER,
                gross_return DOUBLE,
                hit_rate DOUBLE,
                created_at TIMESTAMP
            )
            """
        )
