from __future__ import annotations

from typing import Any

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbPipelineRunRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def insert_pipeline_run(self, payload: dict[str, Any]) -> int:
        try:
            self.con.execute(
                """
                INSERT INTO pipeline_runs (
                    pipeline_name,
                    status,
                    started_at,
                    finished_at,
                    rows_read,
                    rows_written,
                    metrics_json,
                    config_json,
                    error_message,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["pipeline_name"],
                    payload["status"],
                    payload["started_at"],
                    payload["finished_at"],
                    payload["rows_read"],
                    payload["rows_written"],
                    payload["metrics_json"],
                    payload["config_json"],
                    payload["error_message"],
                    payload["created_at"],
                ],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to insert pipeline_runs: {exc}") from exc

    def delete_dataset_version(self, dataset_name: str, dataset_version: str) -> int:
        try:
            self.con.execute(
                """
                DELETE FROM dataset_versions
                WHERE dataset_name = ? AND dataset_version = ?
                """,
                [dataset_name, dataset_version],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to delete dataset_versions rows: {exc}") from exc

    def delete_experiment_runs(self, experiment_name: str) -> int:
        try:
            self.con.execute(
                """
                DELETE FROM experiment_runs
                WHERE experiment_name = ?
                """,
                [experiment_name],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to delete experiment_runs rows: {exc}") from exc

    def delete_backtest_runs(self, backtest_name: str) -> int:
        try:
            self.con.execute(
                """
                DELETE FROM backtest_runs
                WHERE backtest_name = ?
                """,
                [backtest_name],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to delete backtest_runs rows: {exc}") from exc

    def delete_llm_runs(self, run_name: str) -> int:
        try:
            self.con.execute(
                """
                DELETE FROM llm_runs
                WHERE run_name = ?
                """,
                [run_name],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to delete llm_runs rows: {exc}") from exc
