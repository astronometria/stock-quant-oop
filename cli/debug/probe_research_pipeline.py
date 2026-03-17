#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe research pipeline tables and schemas."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[dict]:
    rows = con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return [
        {"column_name": row[0], "data_type": row[1]}
        for row in rows
    ]


def get_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))

    targets = [
        "research_universe",
        "price_history",
        "fundamental_features_daily",
        "short_features_daily",
        "feature_engine_daily",
        "label_engine_daily",
        "research_dataset_daily",
        "backtest_runs",
        "backtest_daily",
        "backtest_positions",
    ]

    result = {
        "db_path": str(db_path),
        "tables": [],
    }

    for table_name in targets:
        exists = table_exists(con, table_name)
        payload = {
            "table_name": table_name,
            "exists": exists,
            "row_count": get_count(con, table_name) if exists else None,
            "columns": get_columns(con, table_name) if exists else [],
        }
        result["tables"].append(payload)

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
