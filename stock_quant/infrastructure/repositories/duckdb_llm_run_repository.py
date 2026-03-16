from __future__ import annotations

from typing import Any

from stock_quant.shared.exceptions import RepositoryError


class DuckDbLlmRunRepository:
    """
    Repository DuckDB pour les écritures de suivi LLM.

    Convention de refactor
    ----------------------
    Ce repository consomme une connexion DuckDB brute (`con`) et non plus
    un UnitOfWork.
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

    def count_news_llm_enrichment_rows(self) -> int:
        """
        Retourne le nombre de lignes actuellement présentes dans
        `news_llm_enrichment`.
        """
        con = self._require_connection()

        try:
            row = con.execute(
                "SELECT COUNT(*) FROM news_llm_enrichment"
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception as exc:
            raise RepositoryError(
                f"failed to count news_llm_enrichment rows: {exc}"
            ) from exc

    def insert_llm_run(self, payload: dict[str, Any]) -> int:
        """
        Insère une ligne dans `llm_runs`.

        Retourne 1 si l'insertion a réussi.
        """
        con = self._require_connection()

        try:
            con.execute(
                """
                INSERT INTO llm_runs (
                    run_name,
                    model_name,
                    prompt_version,
                    status,
                    input_table,
                    output_table,
                    started_at,
                    finished_at,
                    metrics_json,
                    config_json,
                    error_message,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    payload["run_name"],
                    payload["model_name"],
                    payload["prompt_version"],
                    payload["status"],
                    payload["input_table"],
                    payload["output_table"],
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
            raise RepositoryError(f"failed to insert llm_runs: {exc}") from exc
