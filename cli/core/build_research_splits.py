#!/usr/bin/env python3
from __future__ import annotations

"""
Build research splits

Crée un split temporel reproductible.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_split_schema import (
    ResearchSplitSchemaManager,
)


def _now():
    return datetime.now(timezone.utc)


def _split_id() -> str:
    return "split_" + _now().strftime("%Y%m%dT%H%M%SZ")


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument("--db-path", required=True)

    p.add_argument("--train-start", required=True)
    p.add_argument("--train-end", required=True)

    p.add_argument("--valid-start", required=True)
    p.add_argument("--valid-end", required=True)

    p.add_argument("--test-start", required=True)
    p.add_argument("--test-end", required=True)

    p.add_argument("--embargo-days", type=int, default=0)
    p.add_argument("--notes", default="")

    return p.parse_args()


def main():
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db))
    try:
        ResearchSplitSchemaManager(con).ensure_tables()

        split_id = _split_id()

        con.execute("""
        INSERT INTO research_split_manifest VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            split_id,
            args.train_start, args.train_end,
            args.valid_start, args.valid_end,
            args.test_start, args.test_end,
            args.embargo_days,
            args.notes
        ])

        print(json.dumps({
            "split_id": split_id,
            "train": [args.train_start, args.train_end],
            "valid": [args.valid_start, args.valid_end],
            "test": [args.test_start, args.test_end]
        }, indent=2))

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
