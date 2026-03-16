#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.sec.sec_source_loader import SecSourceLoader
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load SEC raw filing index into DuckDB.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--source", action="append", dest="sources", default=[], help="CSV source path. Repeat this flag.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    loader = SecSourceLoader(source_paths=args.sources)
    frame = loader.load_index_frame()

    rows = frame.to_dict(orient="records")
    for row in rows:
        row["ingested_at"] = datetime.utcnow()

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema = SecSchemaManager(uow)
        schema.initialize()

        repository = DuckDbSecRepository(uow.connection)
        written = repository.replace_sec_filing_raw_index(rows)

    result = {
        "rows_input": len(rows),
        "rows_written": written,
        "columns": list(frame.columns),
    }
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
