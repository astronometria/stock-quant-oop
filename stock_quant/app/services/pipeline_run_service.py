from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class PipelineRunService:
    def __init__(self, uow: DuckDbUnitOfWork):
        self.uow = uow

    def start_run(self, pipeline_name: str, config_json: str = "{}") -> int:
        with self.uow as uow:
            con = uow.connection

            run_id = con.execute(
                "SELECT nextval('pipeline_run_id_seq')"
            ).fetchone()[0]

            con.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id,
                    pipeline_name,
                    started_at,
                    status,
                    config_json
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    pipeline_name,
                    datetime.utcnow(),
                    "RUNNING",
                    config_json,
                ),
            )

            return int(run_id)

    def finish_run(
        self,
        run_id: int,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
    ) -> None:
        with self.uow as uow:
            con = uow.connection

            con.execute(
                """
                UPDATE pipeline_runs
                SET
                    finished_at = ?,
                    status = ?,
                    rows_read = ?,
                    rows_written = ?
                WHERE run_id = ?
                """,
                (
                    datetime.utcnow(),
                    status,
                    rows_read,
                    rows_written,
                    run_id,
                ),
            )
