#!/usr/bin/env python3
from __future__ import annotations

"""
Research backtest (SQL-first)

Signal simple:
- long si short_volume_ratio > 0.5
- sinon flat

Retour:
- utilise fwd_return_1d

Important:
- les features viennent de research_training_dataset
- les labels viennent de research_labels
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_backtest_schema import (
    ResearchBacktestSchemaManager,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _backtest_id(dataset_id: str) -> str:
    return f"{dataset_id}_bt_{_now().strftime('%Y%m%dT%H%M%SZ')}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--dataset-id", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_backtest] db_path={db}", flush=True)

    con = duckdb.connect(str(db))
    try:
        ResearchBacktestSchemaManager(con).ensure_tables()

        backtest_id = _backtest_id(args.dataset_id)

        # Idempotence simple
        con.execute(
            """
            DELETE FROM research_backtest
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )

        stats = con.execute(
            """
            WITH base AS (
                SELECT
                    d.dataset_id,
                    d.symbol,
                    d.as_of_date,
                    d.short_volume_ratio,
                    l.fwd_return_1d,

                    CASE
                        WHEN d.short_volume_ratio > 0.5 THEN 1.0
                        ELSE 0.0
                    END AS signal
                FROM research_training_dataset d
                INNER JOIN research_labels l
                    ON l.dataset_id = d.dataset_id
                   AND l.symbol = d.symbol
                   AND l.as_of_date = d.as_of_date
                WHERE d.dataset_id = ?
            ),
            pnl AS (
                SELECT
                    *,
                    CASE
                        WHEN fwd_return_1d IS NOT NULL
                        THEN signal * fwd_return_1d
                        ELSE NULL
                    END AS strategy_return
                FROM base
            )
            SELECT
                AVG(strategy_return) AS avg_return,
                STDDEV_SAMP(strategy_return) AS volatility,
                SUM(strategy_return) AS total_return,
                COUNT(strategy_return) AS n
            FROM pnl
            """,
            [args.dataset_id],
        ).fetchone()

        avg_return = float(stats[0]) if stats and stats[0] is not None else 0.0
        vol = float(stats[1]) if stats and stats[1] is not None else 0.0
        total = float(stats[2]) if stats and stats[2] is not None else 0.0
        n = int(stats[3]) if stats and stats[3] is not None else 0

        sharpe = avg_return / vol if vol > 0 else None

        con.execute(
            """
            INSERT INTO research_backtest (
                backtest_id,
                dataset_id,
                signal_name,
                avg_return,
                volatility,
                sharpe,
                total_return,
                n_obs
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                backtest_id,
                args.dataset_id,
                "short_volume_ratio_long",
                avg_return,
                vol,
                sharpe,
                total,
                n,
            ],
        )

        output = {
            "backtest_id": backtest_id,
            "dataset_id": args.dataset_id,
            "avg_return": avg_return,
            "volatility": vol,
            "sharpe": sharpe,
            "total_return": total,
            "n": n,
        }

        print(json.dumps(output, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
