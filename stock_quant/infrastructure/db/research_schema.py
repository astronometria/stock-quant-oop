from __future__ import annotations

import json
from datetime import datetime

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.research.catalog.feature_catalog import feature_catalog_rows
from stock_quant.shared.time_conventions import time_field_catalog


class ResearchSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_schema_versions()
        self._create_pipeline_runs()
        self._create_feature_runs()
        self._create_label_runs()
        self._create_dataset_versions()
        self._create_experiment_runs()
        self._create_backtest_runs()
        self._create_llm_runs()
        self._create_feature_catalog()
        self._create_time_field_catalog()
        self._seed_feature_catalog()
        self._seed_time_field_catalog()
        self._seed_schema_version()

    def _create_schema_versions(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_versions (
                schema_name VARCHAR,
                schema_version VARCHAR,
                applied_at TIMESTAMP,
                notes_json VARCHAR
            )
            """
        )

    def _create_pipeline_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                pipeline_run_id BIGINT,
                pipeline_name VARCHAR,
                status VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                rows_read BIGINT,
                rows_written BIGINT,
                rows_skipped BIGINT,
                error_message VARCHAR,
                metrics_json VARCHAR,
                config_json VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_feature_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_runs (
                feature_run_id BIGINT,
                feature_set_name VARCHAR,
                feature_version VARCHAR,
                status VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                as_of_date DATE,
                input_tables_json VARCHAR,
                output_table VARCHAR,
                metrics_json VARCHAR,
                config_json VARCHAR,
                error_message VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_label_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS label_runs (
                label_run_id BIGINT,
                label_name VARCHAR,
                label_version VARCHAR,
                status VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                horizon_days INTEGER,
                output_table VARCHAR,
                metrics_json VARCHAR,
                config_json VARCHAR,
                error_message VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_dataset_versions(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS dataset_versions (
                dataset_version_id BIGINT,
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                universe_name VARCHAR,
                as_of_date DATE,
                feature_run_id BIGINT,
                label_run_id BIGINT,
                row_count BIGINT,
                config_json VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_experiment_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS experiment_runs (
                experiment_run_id BIGINT,
                experiment_name VARCHAR,
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

    def _create_backtest_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                backtest_run_id BIGINT,
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

    def _create_llm_runs(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_runs (
                llm_run_id BIGINT,
                run_name VARCHAR,
                model_name VARCHAR,
                prompt_version VARCHAR,
                status VARCHAR,
                input_table VARCHAR,
                output_table VARCHAR,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                metrics_json VARCHAR,
                config_json VARCHAR,
                error_message VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_feature_catalog(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS feature_catalog (
                feature_name VARCHAR,
                feature_family VARCHAR,
                feature_group VARCHAR,
                description VARCHAR,
                input_tables_json VARCHAR,
                output_table VARCHAR,
                parameters_json VARCHAR,
                feature_version VARCHAR,
                is_active BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )

    def _create_time_field_catalog(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS time_field_catalog (
                field_name VARCHAR,
                description VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _seed_feature_catalog(self) -> None:
        count = self.con.execute("SELECT COUNT(*) FROM feature_catalog").fetchone()[0]
        if count:
            return

        rows = []
        now = datetime.utcnow()
        for item in feature_catalog_rows():
            rows.append(
                (
                    item["feature_name"],
                    item["feature_family"],
                    item["feature_group"],
                    item["description"],
                    json.dumps(list(item["input_tables"])),
                    item["output_table"],
                    item["parameters_json"],
                    item["feature_version"],
                    item["is_active"],
                    now,
                )
            )

        self.con.executemany(
            """
            INSERT INTO feature_catalog (
                feature_name,
                feature_family,
                feature_group,
                description,
                input_tables_json,
                output_table,
                parameters_json,
                feature_version,
                is_active,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    def _seed_time_field_catalog(self) -> None:
        count = self.con.execute("SELECT COUNT(*) FROM time_field_catalog").fetchone()[0]
        if count:
            return

        now = datetime.utcnow()
        rows = [
            (item["field_name"], item["description"], now)
            for item in time_field_catalog()
        ]

        self.con.executemany(
            """
            INSERT INTO time_field_catalog (
                field_name,
                description,
                created_at
            )
            VALUES (?, ?, ?)
            """,
            rows,
        )

    def _seed_schema_version(self) -> None:
        count = self.con.execute(
            "SELECT COUNT(*) FROM schema_versions WHERE schema_name = 'research_foundation' AND schema_version = 'v1'"
        ).fetchone()[0]
        if count:
            return

        self.con.execute(
            """
            INSERT INTO schema_versions (
                schema_name,
                schema_version,
                applied_at,
                notes_json
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                "research_foundation",
                "v1",
                datetime.utcnow(),
                json.dumps({"status": "initialized"}),
            ),
        )
