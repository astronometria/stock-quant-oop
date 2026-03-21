#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import duckdb


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS research_signal_performance (
            backtest_id VARCHAR,
            dataset_id VARCHAR,
            split_id VARCHAR,
            signal_name VARCHAR,
            signal_params_json JSON,
            partition_name VARCHAR,
            net_return DOUBLE,
            sharpe DOUBLE,
            turnover DOUBLE,
            n_obs BIGINT,
            created_at TIMESTAMP
        )
    """)


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def load_backtests(con: duckdb.DuckDBPyConnection):
    cols = _table_columns(con, "research_backtest")

    required = {
        "backtest_id",
        "dataset_id",
        "split_id",
        "partition_name",
        "signal_name",
        "net_return",
        "sharpe",
        "turnover",
        "n_obs",
    }
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            f"research_backtest missing required columns: {missing}"
        )

    signal_params_expr = (
        "CAST(signal_params_json AS VARCHAR) AS signal_params_json"
        if "signal_params_json" in cols
        else "NULL AS signal_params_json"
    )
    created_at_expr = (
        "created_at"
        if "created_at" in cols
        else "CURRENT_TIMESTAMP"
    )

    rows = con.execute(f"""
        SELECT
            backtest_id,
            dataset_id,
            split_id,
            signal_name,
            {signal_params_expr},
            partition_name,
            net_return,
            sharpe,
            turnover,
            n_obs,
            {created_at_expr} AS created_at
        FROM research_backtest
    """).fetchall()

    return rows



def refresh_table(con: duckdb.DuckDBPyConnection, rows) -> int:
    """
    Append-only avec déduplication
    """

    if not rows:
        return 0

    inserted = 0

    for r in rows:
        exists = con.execute(
            """
            SELECT 1
            FROM research_signal_performance
            WHERE backtest_id = ?
              AND partition_name = ?
            LIMIT 1
            """,
            [r[0], r[5]],
        ).fetchone()

        if exists:
            continue

        con.execute(
            """
            INSERT INTO research_signal_performance (
                backtest_id,
                dataset_id,
                split_id,
                signal_name,
                signal_params_json,
                partition_name,
                net_return,
                sharpe,
                turnover,
                n_obs,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            r,
        )
        inserted += 1

    return inserted

    con.execute("DELETE FROM research_signal_performance")

    if not rows:
        return 0

    con.executemany("""
        INSERT INTO research_signal_performance (
            backtest_id,
            dataset_id,
            split_id,
            signal_name,
            signal_params_json,
            partition_name,
            net_return,
            sharpe,
            turnover,
            n_obs,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    return len(rows)


def main() -> None:
    args = parse_args()

    print(f"[signal_perf] db_path={args.db_path}", flush=True)

    con = duckdb.connect(args.db_path)
    try:
        ensure_table(con)
        rows = load_backtests(con)
        inserted = refresh_table(con, rows)

        print(
            json.dumps(
                {
                    "inserted_rows": inserted,
                    "built_at": _now_utc().isoformat(),
                },
                indent=2,
            ),
            flush=True,
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
