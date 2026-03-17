from __future__ import annotations

from typing import Any

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbBacktestRepository:
    """
    Repository du moteur de backtest.

    Responsabilités :
    - lire research_dataset_daily
    - écrire experiment_results_daily
    - écrire backtest_positions
    - écrire backtest_daily
    - écrire backtest_runs
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_dataset_rows(
        self,
        dataset_name: str,
        dataset_version: str,
    ) -> list[dict[str, Any]]:
        try:
            frame = self.con.execute(
                """
                SELECT *
                FROM research_dataset_daily
                WHERE dataset_name = ? AND dataset_version = ?
                ORDER BY as_of_date, symbol
                """,
                [dataset_name, dataset_version],
            ).fetchdf()
            return frame.to_dict(orient="records")
        except Exception as exc:
            raise RepositoryError(f"failed to load research dataset rows: {exc}") from exc

    def clear_outputs(self) -> None:
        try:
            self.con.execute("DELETE FROM experiment_results_daily")
            self.con.execute("DELETE FROM backtest_positions")
            self.con.execute("DELETE FROM backtest_daily")
            self.con.execute("DELETE FROM backtest_runs")
        except Exception as exc:
            raise RepositoryError(f"failed to clear backtest outputs: {exc}") from exc

    def insert_experiment_results_daily(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        try:
            self.con.executemany(
                """
                INSERT INTO experiment_results_daily (
                    experiment_name,
                    dataset_name,
                    dataset_version,
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    signal_value,
                    selected_flag,
                    realized_return_1d,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to insert experiment_results_daily rows: {exc}") from exc

    def insert_backtest_positions(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        try:
            self.con.executemany(
                """
                INSERT INTO backtest_positions (
                    backtest_name,
                    dataset_name,
                    dataset_version,
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    signal_column,
                    label_column,
                    signal_value,
                    realized_return,
                    rank_position,
                    weight,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to insert backtest_positions rows: {exc}") from exc

    def insert_backtest_daily(self, rows: list[tuple]) -> int:
        if not rows:
            return 0
        try:
            self.con.executemany(
                """
                INSERT INTO backtest_daily (
                    backtest_name,
                    dataset_name,
                    dataset_version,
                    as_of_date,
                    signal_column,
                    label_column,
                    selected_count,
                    gross_return,
                    hit_rate,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to insert backtest_daily rows: {exc}") from exc

    def insert_backtest_run(
        self,
        *,
        backtest_name: str,
        status: str,
        metrics_json: str,
        config_json: str,
    ) -> int:
        try:
            self.con.execute(
                """
                INSERT INTO backtest_runs (
                    backtest_name,
                    status,
                    dataset_version_id,
                    started_at,
                    finished_at,
                    metrics_json,
                    config_json,
                    error_message,
                    created_at
                )
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    backtest_name,
                    status,
                    None,
                    metrics_json,
                    config_json,
                    None,
                ],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to insert backtest_runs row: {exc}") from exc
