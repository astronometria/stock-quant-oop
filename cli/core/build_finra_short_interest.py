#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import (
    DuckDbShortInterestRepository,
)
from stock_quant.pipelines.build_short_interest_pipeline import BuildShortInterestPipeline


def parse_args() -> argparse.Namespace:
    """
    Point d'entrée core pour reconstruire les tables FINRA / short interest.

    Ce CLI reste volontairement mince :
    - prépare la config
    - valide le schéma
    - injecte le repository
    - exécute le pipeline canonique
    """
    parser = argparse.ArgumentParser(
        description="Build FINRA short interest tables for stock-quant-oop."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market filter.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_finra_short_interest] project_root={config.project_root}")
        print(f"[build_finra_short_interest] db_path={config.db_path}")
        print(f"[build_finra_short_interest] source_market={args.source_market}")

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        if uow.connection is None:
            raise RuntimeError("missing active DB connection")

        repository = DuckDbShortInterestRepository(uow.connection)
        pipeline = BuildShortInterestPipeline(
            repository=repository,
            uow=uow,
        )
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
