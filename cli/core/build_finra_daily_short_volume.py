#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager
from stock_quant.pipelines.build_daily_short_volume_pipeline import BuildDailyShortVolumePipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical FINRA daily short volume history.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    print(f"[build_finra_daily_short_volume] db_path={db_path}", flush=True)

    con = duckdb.connect(db_path)
    try:
        ShortDataSchemaManager().ensure_all(con)
        pipeline = BuildDailyShortVolumePipeline(con=con)
        result = pipeline.run()

        try:
            print(result.to_json(), flush=True)
        except Exception:
            print(json.dumps(result.__dict__, default=str), flush=True)

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
