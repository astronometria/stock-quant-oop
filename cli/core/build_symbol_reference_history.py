#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.db.listing_history_schema import ListingHistorySchemaManager

from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.infrastructure.repositories.duckdb_symbol_reference_history_repository import (
    DuckDbSymbolReferenceHistoryRepository,
)

from stock_quant.app.services.symbol_reference_history_service import (
    SymbolReferenceHistoryService,
)

from stock_quant.pipelines.symbol_reference_history_pipeline import (
    BuildSymbolReferenceHistoryPipeline,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build symbol_reference_history from listing history."
    )
    parser.add_argument("--db-path", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:

        ListingHistorySchemaManager(uow).initialize()

        listing_repo = DuckDbListingHistoryRepository(uow.connection)

        symbol_ref_repo = DuckDbSymbolReferenceHistoryRepository(
            uow.connection
        )

        service = SymbolReferenceHistoryService(
            listing_repository=listing_repo,
            symbol_reference_repository=symbol_ref_repo,
        )

        pipeline = BuildSymbolReferenceHistoryPipeline(service)

        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2))

    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
