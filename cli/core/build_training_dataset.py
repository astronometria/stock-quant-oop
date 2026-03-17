#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_training_dataset_repository import (
    DuckDbTrainingDatasetRepository,
)
from stock_quant.pipelines.build_training_dataset_pipeline import (
    BuildTrainingDatasetPipeline,
)


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--db-path", default=None)

    return parser.parse_args()


def main():

    args = parse_args()

    config = build_app_config(db_path=args.db_path)

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:

        repo = DuckDbTrainingDatasetRepository(uow.connection)

        pipeline = BuildTrainingDatasetPipeline(repo)

        result = pipeline.run()

    print(json.dumps(result.__dict__, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
