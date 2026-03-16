#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.app.services.listing_history_service import ListingHistoryService
from stock_quant.domain.policies.delisting_policy import DelistingPolicy
from stock_quant.domain.policies.new_listing_policy import NewListingPolicy
from stock_quant.domain.policies.ticker_change_policy import TickerChangePolicy
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.listing_history_schema import ListingHistorySchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.pipelines.listing_history_pipeline import BuildListingHistoryPipeline


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes :
    - --db-path : cible la base DuckDB
    - --grace-missing-snapshots : contrôle la prudence de détection
      des listings absents / delistings
    """
    parser = argparse.ArgumentParser(
        description="Build listing_status_history from symbol_reference_source_raw."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--grace-missing-snapshots",
        type=int,
        default=2,
        help="Number of consecutive missing snapshots before closing a listing version.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Point d'entrée principal.

    Pattern standard :
    - config
    - session factory
    - unit of work
    - schema init
    - repositories
    - services
    - pipeline
    - JSON result
    """
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        ListingHistorySchemaManager(uow).initialize()

        repository = DuckDbListingHistoryRepository(uow.connection)

        service = ListingHistoryService(
            repository=repository,
            new_listing_policy=NewListingPolicy(),
            delisting_policy=DelistingPolicy(
                grace_missing_snapshots=args.grace_missing_snapshots,
                allow_immediate_explicit_delisting=True,
            ),
            ticker_change_policy=TickerChangePolicy(),
        )

        pipeline = BuildListingHistoryPipeline(service)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2))
    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
