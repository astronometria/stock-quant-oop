from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class PipelineRunSchema:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def initialize(self) -> None:
        with self.uow as uow:
            con = uow.connection

            con.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id BIGINT,
                    pipeline_name VARCHAR,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    status VARCHAR,
                    rows_read BIGINT,
                    rows_written BIGINT,
                    config_json VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            con.execute(
                """
                CREATE SEQUENCE IF NOT EXISTS pipeline_run_id_seq
                """
            )
