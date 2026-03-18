#!/usr/bin/env python3
from __future__ import annotations

"""
Research backtest (SQL-first)

Signal simple:
- long si short_volume_ratio > 0.5
- sinon flat

Retour:
- utilise fwd_return_1d
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_backtest_schema import (
    ResearchBacktestSchemaManager,
)


def _now():
    return datetime.now(timezone.utc)


def _backtest_id(dataset_id: str) -> str:
    return f"{dataset_id}_bt_{_now().strftime('%Y%m%dT%H%M%SZ')}"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--dataset-id", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    db = Path(args.db_path).resolve()

    con = duckdb.connect(str(db))
    try:
        ResearchBacktestSchemaManager(con).ensure_tables()

        backtest_id = _backtest_id(args.dataset_id)

        # Signal + returns (SQL pur)
        stats = con.execute("""
            WITH base AS (
                SELECT
                    dataset_id,
                    symbol,
                    as_of_date,
                    fwd_return_1d,
                    short_volume_ratio,

                    CASE
                        WHEN short_volume_ratio > 0.5 THEN 1.0
                        ELSE 0.0
                    END AS signal
                FROM research_labels
                WHERE dataset_id = ?
            ),

            pnl AS (
                SELECT
                    *,
                    signal * fwd_return_1d AS strategy_return
                FROM base
            )

            SELECT
                AVG(strategy_return) AS avg_return,
                STDDEV(strategy_return) AS volatility,
                SUM(strategy_return) AS total_return,
                COUNT(*) AS n
            FROM pnl
        """, [args.dataset_id]).fetchone()

        avg_return = float(stats[0]) if stats[0] is not None else 0.0
        vol = float(stats[1]) if stats[1] is not None else 0.0
        total = float(stats[2]) if stats[2] is not None else 0.0
        n = int(stats[3])

        sharpe = avg_return / vol if vol > 0 else None

        con.execute("""
            INSERT INTO research_backtest
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            backtest_id,
            args.dataset_id,
            "short_volume_ratio_long",
            avg_return,
            vol,
            sharpe,
            total,
            n,
        ])

        output = {
            "backtest_id": backtest_id,
            "dataset_id": args.dataset_id,
            "avg_return": avg_return,
            "volatility": vol,
            "sharpe": sharpe,
            "total_return": total,
            "n": n,
        }

        print(json.dumps(output, indent=2))
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
