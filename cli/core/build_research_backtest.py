#!/usr/bin/env python3
from __future__ import annotations

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


def _backtest_id(dataset_id: str, split_id: str) -> str:
    return f"{dataset_id}_{split_id}_bt_{_now().strftime('%Y%m%dT%H%M%SZ')}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--transaction-cost-bps", type=float, default=10.0)
    p.add_argument("--signal-threshold", type=float, default=0.5)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_backtest] db_path={db}", flush=True)

    con = duckdb.connect(str(db))
    try:
        ResearchBacktestSchemaManager(con).ensure_tables()

        split = con.execute(
            """
            SELECT
                split_id,
                train_start,
                train_end,
                valid_start,
                valid_end,
                test_start,
                test_end
            FROM research_split_manifest
            WHERE split_id = ?
            """,
            [args.split_id],
        ).fetchone()

        if split is None:
            raise RuntimeError("split_id not found")

        _, train_start, train_end, valid_start, valid_end, test_start, test_end = split

        backtest_id = _backtest_id(args.dataset_id, args.split_id)

        con.execute(
            """
            DELETE FROM research_backtest
            WHERE dataset_id = ? AND split_id = ?
            """,
            [args.dataset_id, args.split_id],
        )

        rows = con.execute(
            """
            WITH joined AS (
                SELECT
                    d.dataset_id,
                    d.symbol,
                    d.as_of_date,
                    d.short_volume_ratio,
                    l.fwd_return_1d,
                    CASE
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'train'
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'valid'
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'test'
                        ELSE NULL
                    END AS partition_name,
                    CASE
                        WHEN d.short_volume_ratio > ? THEN 1.0
                        ELSE 0.0
                    END AS signal
                FROM research_training_dataset d
                INNER JOIN research_labels l
                    ON l.dataset_id = d.dataset_id
                   AND l.symbol = d.symbol
                   AND l.as_of_date = d.as_of_date
                WHERE d.dataset_id = ?
            ),
            positioned AS (
                SELECT
                    *,
                    LAG(signal, 1, 0.0) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) AS prev_signal
                FROM joined
                WHERE partition_name IS NOT NULL
            ),
            pnl AS (
                SELECT
                    partition_name,
                    symbol,
                    as_of_date,
                    signal,
                    prev_signal,
                    fwd_return_1d,
                    signal * fwd_return_1d AS gross_strategy_return,
                    ABS(signal - prev_signal) AS turnover_unit,
                    ABS(signal - prev_signal) * (? / 10000.0) AS transaction_cost,
                    (signal * fwd_return_1d) - (ABS(signal - prev_signal) * (? / 10000.0)) AS net_strategy_return
                FROM positioned
                WHERE fwd_return_1d IS NOT NULL
            )
            SELECT
                partition_name,
                AVG(gross_strategy_return) AS avg_gross,
                SUM(gross_strategy_return) AS gross_return,
                SUM(transaction_cost) AS total_cost,
                SUM(net_strategy_return) AS net_return,
                AVG(net_strategy_return) AS avg_net,
                STDDEV_SAMP(net_strategy_return) AS volatility,
                CASE
                    WHEN STDDEV_SAMP(net_strategy_return) IS NULL OR STDDEV_SAMP(net_strategy_return) = 0
                    THEN NULL
                    ELSE AVG(net_strategy_return) / STDDEV_SAMP(net_strategy_return)
                END AS sharpe,
                AVG(turnover_unit) AS turnover,
                COUNT(*) AS n_obs
            FROM pnl
            GROUP BY partition_name
            ORDER BY CASE partition_name
                WHEN 'train' THEN 1
                WHEN 'valid' THEN 2
                WHEN 'test' THEN 3
                ELSE 4
            END
            """,
            [
                train_start, train_end,
                valid_start, valid_end,
                test_start, test_end,
                args.signal_threshold,
                args.dataset_id,
                args.transaction_cost_bps,
                args.transaction_cost_bps,
            ],
        ).fetchall()

        results = []
        for row in rows:
            partition_name, avg_gross, gross_return, total_cost, net_return, avg_net, volatility, sharpe, turnover, n_obs = row

            con.execute(
                """
                INSERT INTO research_backtest (
                    backtest_id,
                    dataset_id,
                    split_id,
                    partition_name,
                    signal_name,
                    transaction_cost_bps,
                    gross_return,
                    total_cost,
                    net_return,
                    avg_return,
                    volatility,
                    sharpe,
                    turnover,
                    n_obs
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    backtest_id,
                    args.dataset_id,
                    args.split_id,
                    partition_name,
                    "short_volume_ratio_long_only",
                    args.transaction_cost_bps,
                    float(gross_return) if gross_return is not None else 0.0,
                    float(total_cost) if total_cost is not None else 0.0,
                    float(net_return) if net_return is not None else 0.0,
                    float(avg_net) if avg_net is not None else 0.0,
                    float(volatility) if volatility is not None else 0.0,
                    float(sharpe) if sharpe is not None else None,
                    float(turnover) if turnover is not None else 0.0,
                    int(n_obs) if n_obs is not None else 0,
                ],
            )

            results.append(
                {
                    "partition_name": partition_name,
                    "avg_gross": float(avg_gross) if avg_gross is not None else 0.0,
                    "gross_return": float(gross_return) if gross_return is not None else 0.0,
                    "total_cost": float(total_cost) if total_cost is not None else 0.0,
                    "net_return": float(net_return) if net_return is not None else 0.0,
                    "avg_return": float(avg_net) if avg_net is not None else 0.0,
                    "volatility": float(volatility) if volatility is not None else 0.0,
                    "sharpe": float(sharpe) if sharpe is not None else None,
                    "turnover": float(turnover) if turnover is not None else 0.0,
                    "n_obs": int(n_obs) if n_obs is not None else 0,
                }
            )

        print(json.dumps({
            "backtest_id": backtest_id,
            "dataset_id": args.dataset_id,
            "split_id": args.split_id,
            "transaction_cost_bps": args.transaction_cost_bps,
            "signal_threshold": args.signal_threshold,
            "partitions": results,
        }, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
