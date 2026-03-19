#!/usr/bin/env python3
from __future__ import annotations

"""
Research labels builder (aligned runtime version).

Objectif
--------
Construire les labels forward-looking à partir de price_history.

Améliorations:
--------------
- support --verbose
- support memory_limit / threads / temp_directory
- logs propres et flush immédiat
- prêt pour future version chunked si nécessaire

Important:
----------
- forward-looking ONLY (normal)
- jamais utilisé comme feature
"""

import argparse
import json
from pathlib import Path

import duckdb


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _log(msg: str, verbose: bool) -> None:
    """Log conditionnel pour garder un terminal propre."""
    if verbose:
        print(msg, flush=True)


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


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--split-id", required=True)

    # 🔥 Alignement avec dataset builder
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")

    return p.parse_args()


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_labels] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # ------------------------------------------------------------------
        # DuckDB runtime tuning (IMPORTANT pour gros volume)
        # ------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        _log(f"[labels] memory_limit={args.memory_limit}", args.verbose)
        _log(f"[labels] threads={args.threads}", args.verbose)
        _log(f"[labels] temp_dir={temp_dir_sql}", args.verbose)

        # ------------------------------------------------------------------
        # Validation snapshot
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Split
        # ------------------------------------------------------------------
        split = con.execute(
            """
            SELECT
                split_id,
                train_start,
                train_end,
                valid_start,
                valid_end,
                test_start,
                test_end,
                embargo_days
            FROM research_split_manifest
            WHERE split_id = ?
            """,
            [args.split_id],
        ).fetchone()

        if split is None:
            raise RuntimeError("split_id not found")

        (
            split_id,
            train_start,
            train_end,
            valid_start,
            valid_end,
            test_start,
            test_end,
            embargo_days,
        ) = split

        price_date_col = _choose_price_date_column(con, "price_history")

        _log(f"[labels] price_date_column={price_date_col}", args.verbose)
        _log(f"[labels] date_range={train_start} -> {test_end}", args.verbose)

        # ------------------------------------------------------------------
        # Reset dataset
        # ------------------------------------------------------------------
        con.execute(
            """
            DELETE FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )

        _log("[labels] previous dataset cleared", args.verbose)

        # ------------------------------------------------------------------
        # Main insert (SQL-first)
        # ------------------------------------------------------------------
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
                END,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 5) OVER w IS NOT NULL
                    THEN LEAD(p.close, 5) OVER w / p.close - 1
                    ELSE NULL
                END,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 20) OVER w IS NOT NULL
                    THEN LEAD(p.close, 20) OVER w / p.close - 1
                    ELSE NULL
                END

            FROM price_history p
            WHERE p.{price_date_col} BETWEEN ? AND ?
              AND p.symbol IS NOT NULL
              AND TRIM(p.symbol) <> ''

            WINDOW w AS (
                PARTITION BY p.symbol
                ORDER BY p.{price_date_col}
            )
            """,
            [args.dataset_id, args.snapshot_id, train_start, test_end],
        )

        # ------------------------------------------------------------------
        # Stats
        # ------------------------------------------------------------------
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
                    "split_id": args.split_id,
                    "rows": int(count),
                    "price_date_column": price_date_col,
                    "date_window": {
                        "start": str(train_start),
                        "end": str(test_end),
                    },
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
