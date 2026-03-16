#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.app.services.market_universe_history_service import (
    MarketUniverseHistoryService,
)
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.listing_history_schema import ListingHistorySchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.infrastructure.repositories.duckdb_market_universe_history_repository import (
    DuckDbMarketUniverseHistoryRepository,
)
from stock_quant.pipelines.market_universe_history_pipeline import (
    BuildMarketUniverseHistoryPipeline,
)


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes :
    - include_adr : permet d'inclure les ADR/ADS dans l'univers si désiré
    - allowed_exchanges : liste d'exchanges éligibles
    """
    parser = argparse.ArgumentParser(
        description="Build market_universe_history from active listing history."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--include-adr",
        action="store_true",
        help="Include ADR/ADS in the eligible universe.",
    )
    parser.add_argument(
        "--allowed-exchanges",
        nargs="+",
        default=["NASDAQ", "NYSE", "AMEX"],
        help="Allowed exchanges for inclusion in the market universe.",
    )
    parser.add_argument(
        "--rule-version",
        default="v1",
        help="Rule version label stored in market_universe_history.",
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
    - service
    - pipeline
    - JSON result
    """
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        ListingHistorySchemaManager(uow).initialize()

        listing_repository = DuckDbListingHistoryRepository(uow.connection)
        universe_repository = DuckDbMarketUniverseHistoryRepository(uow.connection)

        service = MarketUniverseHistoryService(
            listing_repository=listing_repository,
            universe_repository=universe_repository,
            include_adr=args.include_adr,
            allowed_exchanges=tuple(args.allowed_exchanges),
            rule_version=args.rule_version,
        )

        pipeline = BuildMarketUniverseHistoryPipeline(service)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2))
    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
