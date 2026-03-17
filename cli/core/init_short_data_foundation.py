#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize short-data foundation tables in DuckDB."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    print(f"[init_short_data_foundation] db_path={db_path}", flush=True)

    con = duckdb.connect(db_path)
    try:
        manager = ShortDataSchemaManager()
        # Important: la classe actuelle ne prend pas de constructor arg.
        # On lui passe explicitement la connexion active.
        if hasattr(manager, "ensure_all"):
            manager.ensure_all(con)
        elif hasattr(manager, "ensure_schema"):
            manager.ensure_schema(con)
        elif hasattr(manager, "apply"):
            manager.apply(con)
        else:
            raise RuntimeError(
                "ShortDataSchemaManager does not expose ensure_all(con), ensure_schema(con) or apply(con)."
            )

        for table_name in [
            "finra_daily_short_volume_source_raw",
            "daily_short_volume_history",
            "short_features_daily",
        ]:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[init_short_data_foundation] {table_name}={count}", flush=True)

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
