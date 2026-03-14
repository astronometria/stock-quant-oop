#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from stock_quant.app.dto.commands import InitDbCommand
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize stock-quant-oop DuckDB schema.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--drop-existing", action="store_true", help="Drop existing tables before re-creating them.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    command = InitDbCommand(
        db_path=Path(config.db_path),
        verbose=bool(args.verbose),
        drop_existing=bool(args.drop_existing),
    )

    if command.verbose:
        print(f"[init_market_db] project_root={config.project_root}")
        print(f"[init_market_db] db_path={command.db_path}")
        print(f"[init_market_db] logs_dir={config.logs_dir}")
        print(f"[init_market_db] data_dir={config.data_dir}")
        print(f"[init_market_db] drop_existing={command.drop_existing}")

    session_factory = DuckDbSessionFactory(command.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.initialize(drop_existing=command.drop_existing)
        schema_manager.validate()

    print(f"Schema initialized successfully: {command.db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
