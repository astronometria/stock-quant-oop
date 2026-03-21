#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.domain.signals import build_default_signal_registry
from stock_quant.infrastructure.db.research_backtest_schema import (
    ResearchBacktestSchemaManager,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _backtest_id(dataset_id: str, split_id: str) -> str:
    return f"{dataset_id}_{split_id}_bt_{_now().strftime('%Y%m%dT%H%M%SZ')}"


def _log(message: str, verbose: bool) -> None:
    if verbose:
        print(message, flush=True)


def _normalize_signal_params_json(
    raw: str | None,
    *,
    signal_name: str,
    signal_threshold: float,
) -> dict[str, Any]:
    if raw is None or not raw.strip():
        if signal_name == "short_volume_ratio_threshold":
            return {
                "feature_name": "short_volume_ratio",
                "threshold": float(signal_threshold),
                "zero_below_threshold": True,
            }
        return {}

    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("signal-params-json must decode to a JSON object")
    return parsed


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--transaction-cost-bps", type=float, default=10.0)

    p.add_argument(
        "--signal-name",
        default="short_volume_ratio_threshold",
        help="Public signal name resolved through the signal registry.",
    )
    p.add_argument(
        "--signal-params-json",
        default=None,
        help="Optional JSON object for signal params. If omitted, a compatibility payload is built from --signal-threshold.",
    )
    p.add_argument(
        "--execution-lag-bars",
        type=int,
        default=1,
        help="Decision to execution lag in bars. Must be >= 1.",
    )

    p.add_argument(
        "--signal-threshold",
        type=float,
        default=0.5,
        help="Compatibility shortcut for the default short_volume_ratio_threshold signal.",
    )

    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def _ensure_signal_params_column(con: duckdb.DuckDBPyConnection) -> None:
    cols = {
        str(row[1]).strip()
        for row in con.execute("PRAGMA table_info('research_backtest')").fetchall()
    }
    if "signal_params_json" not in cols:
        con.execute("ALTER TABLE research_backtest ADD COLUMN signal_params_json JSON")


def main() -> int:
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_backtest] db_path={db}", flush=True)

    registry = build_default_signal_registry()
    signal_params = _normalize_signal_params_json(
        args.signal_params_json,
        signal_name=args.signal_name,
        signal_threshold=args.signal_threshold,
    )
    signal = registry.create(args.signal_name, signal_params)
    execution_lag_bars = signal.validate_execution_lag_bars(args.execution_lag_bars)

    _log(f"[backtest] signal_name={signal.signal_name}", args.verbose)
    _log(f"[backtest] signal_version={signal.signal_version}", args.verbose)
    _log(f"[backtest] signal_params={json.dumps(signal.params, sort_keys=True)}", args.verbose)
    _log(f"[backtest] execution_lag_bars={execution_lag_bars}", args.verbose)

    if signal.signal_name == "short_volume_ratio_threshold":
        feature_name = str(signal.params["feature_name"])
        threshold = float(signal.params["threshold"])
        zero_below_threshold = bool(signal.params["zero_below_threshold"])

        if feature_name != "short_volume_ratio":
            raise RuntimeError(
                "short_volume_ratio_threshold SQL-first implementation currently requires "
                "feature_name='short_volume_ratio'"
            )

        signal_sql_expr = (
            "CASE "
            "WHEN d.short_volume_ratio IS NULL THEN NULL "
            f"WHEN d.short_volume_ratio >= {threshold} THEN CAST(d.short_volume_ratio AS DOUBLE) "
            + ("ELSE 0.0 " if zero_below_threshold else "ELSE CAST(d.short_volume_ratio AS DOUBLE) ")
            + "END"
        )

    elif signal.signal_name == "rsi_threshold":
        feature_name = str(signal.params["feature_name"])
        oversold_threshold = float(signal.params["oversold_threshold"])
        overbought_threshold = float(signal.params["overbought_threshold"])

        if feature_name != "rsi_14":
            raise RuntimeError(
                "rsi_threshold SQL-first implementation currently requires "
                "feature_name='rsi_14'"
            )

        signal_sql_expr = (
            "CASE "
            "WHEN d.rsi_14 IS NULL THEN NULL "
            f"WHEN d.rsi_14 <= {oversold_threshold} THEN 1.0 "
            f"WHEN d.rsi_14 >= {overbought_threshold} THEN 0.0 "
            "ELSE 0.0 "
            "END"
        )

    elif signal.signal_name == "sma_cross":
        fast_feature_name = str(signal.params["fast_feature_name"])
        slow_feature_name = str(signal.params["slow_feature_name"])

        allowed = {"sma_20", "sma_50", "sma_200"}
        if fast_feature_name not in allowed:
            raise RuntimeError(
                f"sma_cross SQL-first implementation currently does not support "
                f"fast_feature_name={fast_feature_name!r}"
            )
        if slow_feature_name not in allowed:
            raise RuntimeError(
                f"sma_cross SQL-first implementation currently does not support "
                f"slow_feature_name={slow_feature_name!r}"
            )

        signal_sql_expr = (
            "CASE "
            f"WHEN d.{fast_feature_name} IS NULL OR d.{slow_feature_name} IS NULL THEN NULL "
            f"WHEN d.{fast_feature_name} > d.{slow_feature_name} THEN 1.0 "
            "ELSE 0.0 "
            "END"
        )

    elif signal.signal_name == "sma_cross_rsi_filter":
        fast_feature_name = str(signal.params["fast_feature_name"])
        slow_feature_name = str(signal.params["slow_feature_name"])
        rsi_feature_name = str(signal.params["rsi_feature_name"])
        rsi_threshold = float(signal.params["rsi_threshold"])

        allowed = {"sma_20", "sma_50", "sma_200"}
        if fast_feature_name not in allowed:
            raise RuntimeError(
                f"sma_cross_rsi_filter SQL-first implementation currently does not support "
                f"fast_feature_name={fast_feature_name!r}"
            )
        if slow_feature_name not in allowed:
            raise RuntimeError(
                f"sma_cross_rsi_filter SQL-first implementation currently does not support "
                f"slow_feature_name={slow_feature_name!r}"
            )
        if rsi_feature_name != "rsi_14":
            raise RuntimeError(
                "sma_cross_rsi_filter SQL-first implementation currently requires "
                "rsi_feature_name='rsi_14'"
            )

        signal_sql_expr = (
            "CASE "
            f"WHEN d.{fast_feature_name} IS NULL OR d.{slow_feature_name} IS NULL OR d.rsi_14 IS NULL THEN NULL "
            f"WHEN d.{fast_feature_name} > d.{slow_feature_name} AND d.rsi_14 <= {rsi_threshold} THEN 1.0 "
            "ELSE 0.0 "
            "END"
        )

    else:
        raise RuntimeError(
            f"build_research_backtest.py does not yet support signal_name={signal.signal_name!r} in SQL-first mode"
        )

    con = duckdb.connect(str(db))
    try:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")
        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        _log(f"[backtest] memory_limit={args.memory_limit}", args.verbose)
        _log(f"[backtest] threads={args.threads}", args.verbose)
        _log(f"[backtest] temp_dir={temp_dir_sql}", args.verbose)
        _log(f"[backtest] dataset_id={args.dataset_id}", args.verbose)
        _log(f"[backtest] split_id={args.split_id}", args.verbose)
        _log(f"[backtest] transaction_cost_bps={args.transaction_cost_bps}", args.verbose)

        ResearchBacktestSchemaManager(con).ensure_tables()
        _ensure_signal_params_column(con)

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

        rows = con.execute(
            f"""
            WITH joined AS (
                SELECT
                    d.dataset_id,
                    d.symbol,
                    d.as_of_date,
                    l.fwd_return_1d,
                    CASE
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'train'
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'valid'
                        WHEN d.as_of_date BETWEEN ? AND ? THEN 'test'
                        ELSE NULL
                    END AS partition_name,
                    {signal_sql_expr} AS signal_value
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
                    CASE
                        WHEN signal_value IS NULL THEN 0.0
                        WHEN signal_value > 0 THEN 1.0
                        ELSE 0.0
                    END AS signal_active,
                    LAG(
                        CASE
                            WHEN signal_value IS NULL THEN 0.0
                            WHEN signal_value > 0 THEN 1.0
                            ELSE 0.0
                        END,
                        1,
                        0.0
                    ) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) AS prev_signal_active
                FROM joined
                WHERE partition_name IS NOT NULL
            ),
            pnl AS (
                SELECT
                    partition_name,
                    symbol,
                    as_of_date,
                    signal_value,
                    signal_active,
                    prev_signal_active,
                    fwd_return_1d,
                    signal_active * fwd_return_1d AS gross_strategy_return,
                    ABS(signal_active - prev_signal_active) AS turnover_unit,
                    ABS(signal_active - prev_signal_active) * (? / 10000.0) AS transaction_cost,
                    (signal_active * fwd_return_1d) - (ABS(signal_active - prev_signal_active) * (? / 10000.0)) AS net_strategy_return
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
                    WHEN STDDEV_SAMP(net_strategy_return) IS NULL
                      OR STDDEV_SAMP(net_strategy_return) = 0 THEN NULL
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
                train_start,
                train_end,
                valid_start,
                valid_end,
                test_start,
                test_end,
                args.dataset_id,
                args.transaction_cost_bps,
                args.transaction_cost_bps,
            ],
        ).fetchall()

        results: list[dict[str, object]] = []
        signal_params_json = json.dumps(signal.params, sort_keys=True)

        for row in rows:
            (
                partition_name,
                avg_gross,
                gross_return,
                total_cost,
                net_return,
                avg_net,
                volatility,
                sharpe,
                turnover,
                n_obs,
            ) = row

            con.execute(
                """
                INSERT INTO research_backtest (
                    backtest_id,
                    dataset_id,
                    split_id,
                    partition_name,
                    signal_name,
                    signal_params_json,
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    backtest_id,
                    args.dataset_id,
                    args.split_id,
                    partition_name,
                    signal.signal_name,
                    signal_params_json,
                    args.transaction_cost_bps,
                    gross_return,
                    total_cost,
                    net_return,
                    avg_net,
                    volatility,
                    sharpe,
                    turnover,
                    n_obs,
                ],
            )

            results.append(
                {
                    "partition_name": partition_name,
                    "avg_gross": avg_gross,
                    "gross_return": gross_return,
                    "total_cost": total_cost,
                    "net_return": net_return,
                    "avg_net": avg_net,
                    "volatility": volatility,
                    "sharpe": sharpe,
                    "turnover": turnover,
                    "n_obs": n_obs,
                }
            )

        payload = {
            "backtest_id": backtest_id,
            "dataset_id": args.dataset_id,
            "split_id": args.split_id,
            "signal_name": signal.signal_name,
            "signal_version": signal.signal_version,
            "signal_params": signal.params,
            "required_features": list(signal.required_features()),
            "warmup_bars": signal.warmup_bars(),
            "execution_lag_bars": execution_lag_bars,
            "transaction_cost_bps": args.transaction_cost_bps,
            "results": results,
            "created_at": _now().isoformat(),
        }
        print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
