from __future__ import annotations

from typing import Any

from stock_quant.shared.exceptions import RepositoryError


class DuckDbPipelineRunRepository:
    """
    Repository DuckDB pour les écritures et nettoyages liés au suivi des runs.

    Convention de refactor importante
    ---------------------------------
    Ce repository prend désormais une connexion DuckDB active (`con`) et non
    plus un UnitOfWork.

    Pourquoi
    --------
    - homogénéiser tous les repositories du projet
    - éviter le mélange `repository(uow)` vs `repository(uow.connection)`
    - garder les repositories focalisés uniquement sur la persistance SQL

    Le UnitOfWork reste géré au niveau CLI / pipeline / service.
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    def _require_connection(self):
        """
        Vérifie qu'une connexion active est bien disponible.
        """
        if self.con is None:
            raise RepositoryError("active DB connection is required")
        return self.con

    def insert_pipeline_run(self, payload: dict[str, Any]) -> int:
        """
        Insère une ligne dans `pipeline_runs`.

        Retourne 1 si l'insertion a réussi.
        """
        con = self._require_connection()

        try:
            con.execute(
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
        """
        Supprime les lignes correspondant à un dataset versionné donné.
        """
        con = self._require_connection()

        try:
            con.execute(
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
        """
        Supprime les runs d'expérience correspondant au nom fourni.
        """
        con = self._require_connection()

        try:
            con.execute(
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
        """
        Supprime les runs de backtest correspondant au nom fourni.
        """
        con = self._require_connection()

        try:
            con.execute(
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
        """
        Supprime les runs LLM correspondant au nom logique fourni.
        """
        con = self._require_connection()

        try:
            con.execute(
                """
                DELETE FROM llm_runs
                WHERE run_name = ?
                """,
                [run_name],
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to delete llm_runs rows: {exc}") from exc
