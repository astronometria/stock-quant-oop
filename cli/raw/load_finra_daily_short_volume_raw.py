#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_short_data_repository import DuckDbShortDataRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FINRA daily short volume raw CSV.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--source", action="append", dest="sources", default=[], help="CSV source path. Repeat this flag.")
    return parser.parse_args()


def load_rows(paths: list[str]) -> list[dict]:
    rows: list[dict] = []
    for path in paths:
        frame = pd.read_csv(Path(path).expanduser().resolve())
        frame.columns = [str(col).strip() for col in frame.columns]
        rename_map = {
            "Date": "trade_date",
            "date": "trade_date",
            "Symbol": "symbol",
            "symbol": "symbol",
            "ShortVolume": "short_volume",
            "short_volume": "short_volume",
            "TotalVolume": "total_volume",
            "total_volume": "total_volume",
        }
        frame = frame.rename(columns=rename_map)

        if "trade_date" in frame.columns:
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        if "symbol" in frame.columns:
            frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()

        for col in ["short_volume", "total_volume"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")

        frame["source_name"] = "finra"
        records = frame.to_dict(orient="records")
        for row in records:
            row["ingested_at"] = datetime.utcnow()
        rows.extend(records)
    return rows


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    rows = load_rows(args.sources)

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema = ShortDataSchemaManager(uow)
        schema.initialize()
        repository = DuckDbShortDataRepository(uow)
        written = repository.replace_daily_short_volume_source_raw(rows)

    print(json.dumps({"rows_input": len(rows), "rows_written": written}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
