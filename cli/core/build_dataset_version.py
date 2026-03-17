#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_dataset_version_repository import (
    DuckDbDatasetVersionRepository,
)
from stock_quant.pipelines.build_dataset_version_pipeline import (
    BuildDatasetVersionPipeline,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create or replace a version record for training_dataset_daily."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--dataset-name",
        default="training_dataset_daily",
        help="Logical dataset name.",
    )
    parser.add_argument(
        "--dataset-version",
        default="v1",
        help="Logical dataset version label.",
    )
    parser.add_argument(
        "--no-replace-existing",
        action="store_true",
        help="Do not delete an existing row before insert.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_dataset_version] project_root={config.project_root}")
        print(f"[build_dataset_version] db_path={config.db_path}")
        print(f"[build_dataset_version] dataset_name={args.dataset_name}")
        print(f"[build_dataset_version] dataset_version={args.dataset_version}")

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        if uow.connection is None:
            raise RuntimeError("missing active DB connection")

        repository = DuckDbDatasetVersionRepository(uow.connection)
        pipeline = BuildDatasetVersionPipeline(repository=repository)
        result = pipeline.run(
            dataset_name=args.dataset_name,
            dataset_version=args.dataset_version,
            replace_existing=not args.no_replace_existing,
        )

    print(json.dumps(result.__dict__, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
