#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.news_intelligence_schema import NewsIntelligenceSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize news intelligence foundation tables.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--drop-existing", action="store_true", help="Drop and recreate news intelligence tables.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[init_news_intelligence_foundation] project_root={config.project_root}")
        print(f"[init_news_intelligence_foundation] db_path={config.db_path}")
        print(f"[init_news_intelligence_foundation] drop_existing={args.drop_existing}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        manager = NewsIntelligenceSchemaManager(uow)
        manager.initialize(drop_existing=bool(args.drop_existing))

    print(f"News intelligence foundation initialized successfully: {config.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
