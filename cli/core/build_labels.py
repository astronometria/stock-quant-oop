#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb
import json
from datetime import datetime

from stock_quant.pipelines.build_label_engine_pipeline import run


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build label tables (forward returns + volatility).")
    p.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print execution metadata",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_labels] db_path={db_path}")

    con = duckdb.connect(str(db_path))

    started_at = datetime.utcnow()

    metrics = run(con)

    finished_at = datetime.utcnow()

    result = {
        "pipeline_name": "build_labels",
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "metrics": metrics,
        "status": "SUCCESS",
    }

    if args.verbose:
        print(json.dumps(result, indent=2))

    con.close()


if __name__ == "__main__":
    main()
