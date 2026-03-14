from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from stock_quant.domain.entities.backtest_result import BacktestResultSummary, ExperimentResultDaily
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbBacktestRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_dataset_rows(self, dataset_name: str, dataset_version: str) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    dataset_name,
                    dataset_version,
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    close_to_sma_20,
                    fwd_return_1d
                FROM research_dataset_daily
                WHERE dataset_name = ? AND dataset_version = ?
                ORDER BY symbol, as_of_date
                """,
                [dataset_name, dataset_version],
            ).fetchall()

            return [
                {
                    "dataset_name": row[0],
                    "dataset_version": row[1],
                    "instrument_id": row[2],
                    "company_id": row[3],
                    "symbol": row[4],
                    "as_of_date": row[5],
                    "close_to_sma_20": row[6],
                    "fwd_return_1d": row[7],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load research_dataset_daily rows: {exc}") from exc

    def lookup_dataset_version_id(self, dataset_name: str, dataset_version: str) -> int | None:
        try:
            row = self.con.execute(
                """
                SELECT rowid
                FROM dataset_versions
                WHERE dataset_name = ? AND dataset_version = ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                [dataset_name, dataset_version],
            ).fetchone()
            return int(row[0]) if row else None
        except Exception as exc:
            raise RepositoryError(f"failed to lookup dataset_versions rowid: {exc}") from exc

    def replace_experiment_results_daily(self, rows: list[ExperimentResultDaily]) -> int:
        try:
            self.con.execute("DELETE FROM experiment_results_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.experiment_name,
                    row.dataset_name,
                    row.dataset_version,
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.signal_value,
                    row.selected_flag,
                    row.realized_return_1d,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
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
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace experiment_results_daily: {exc}") from exc

    def insert_experiment_run(self, payload: dict[str, Any]) -> int:
        try:
            self.con.execute(
                """
                INSERT INTO experiment_runs (
                    experiment_name,
                    status,
                    dataset_version_id,
                    started_at,
                    finished_at,
                    metrics_json,
                    config_json,
                    error_message,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["experiment_name"],
                    payload["status"],
                    payload["dataset_version_id"],
                    payload["started_at"],
                    payload["finished_at"],
                    payload["metrics_json"],
                    payload["config_json"],
                    payload["error_message"],
                    payload["created_at"],
                ],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to insert experiment_runs: {exc}") from exc

    def insert_backtest_run(self, payload: dict[str, Any]) -> int:
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["backtest_name"],
                    payload["status"],
                    payload["dataset_version_id"],
                    payload["started_at"],
                    payload["finished_at"],
                    payload["metrics_json"],
                    payload["config_json"],
                    payload["error_message"],
                    payload["created_at"],
                ],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to insert backtest_runs: {exc}") from exc

    def summary_as_metrics(self, row: BacktestResultSummary) -> dict[str, Any]:
        return {
            "observations": row.observations,
            "selected_observations": row.selected_observations,
            "mean_return_1d": row.mean_return_1d,
            "hit_rate_1d": row.hit_rate_1d,
            "cumulative_return_proxy": row.cumulative_return_proxy,
        }
