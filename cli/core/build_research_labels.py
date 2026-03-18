#!/usr/bin/env python3
from __future__ import annotations

"""
Build research labels (forward returns).

IMPORTANT
---------
- no look-ahead bias
- labels séparés des features
- schema assuré avant écriture
- robuste si le nom de la colonne date diffère dans price_history
"""

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_labels_schema import (
    ResearchLabelsSchemaManager,
)


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _choose_price_date_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str = "price_history",
) -> str:
    if not _table_exists(con, table_name):
        raise RuntimeError(f"{table_name} does not exist")

    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "price_date",
        "date",
        "trade_date",
        "as_of_date",
        "business_date",
    ]
    for col in preferred_order:
        if col in cols:
            return col

    for col in cols:
        if "date" in col or "time" in col:
            return col

    raise RuntimeError(
        f"unable to detect price date column in {table_name}; available columns={sorted(cols)}"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_labels] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        ResearchLabelsSchemaManager(con).ensure_tables()

        snap = con.execute(
            """
            SELECT status
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
            """,
            [args.snapshot_id],
        ).fetchone()

        if snap is None:
            raise RuntimeError("snapshot not found")

        if snap[0] != "completed":
            raise RuntimeError("snapshot not completed")

        price_date_col = _choose_price_date_column(con, "price_history")

        con.execute(
            """
            DELETE FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )

        con.execute(
            f"""
            INSERT INTO research_labels (
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d
            )
            SELECT
                ? AS dataset_id,
                ? AS snapshot_id,
                p.symbol,
                p.{price_date_col} AS as_of_date,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 1) OVER w IS NOT NULL
                    THEN LEAD(p.close, 1) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_1d,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 5) OVER w IS NOT NULL
                    THEN LEAD(p.close, 5) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_5d,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 20) OVER w IS NOT NULL
                    THEN LEAD(p.close, 20) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_20d

            FROM price_history p
            WHERE p.{price_date_col} IS NOT NULL
              AND p.symbol IS NOT NULL
              AND TRIM(p.symbol) <> ''

            WINDOW w AS (
                PARTITION BY p.symbol
                ORDER BY p.{price_date_col}
            )
            """,
            [args.dataset_id, args.snapshot_id],
        )

        count = con.execute(
            """
            SELECT COUNT(*)
            FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        ).fetchone()[0]

        print(
            json.dumps(
                {
                    "dataset_id": args.dataset_id,
                    "snapshot_id": args.snapshot_id,
                    "rows": int(count),
                    "price_date_column": price_date_col,
                },
                indent=2,
            ),
            flush=True,
        )
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
