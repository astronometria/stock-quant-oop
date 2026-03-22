#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict

import duckdb
import joblib
import pandas as pd
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="/home/marty/stock-quant-oop/market.duckdb")
    parser.add_argument("--horizon", type=int, choices=[5, 10, 20], required=True)
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--cost-bps", type=float, default=10.0)
    parser.add_argument("--bucket-pct", type=float, default=0.05)
    parser.add_argument("--max-weight", type=float, default=0.02)
    parser.add_argument("--realized-ret-cap", type=float, default=2.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    HOLD_DAYS = int(args.horizon)

    print("===== STAGE 1/6: LOAD MODEL =====", flush=True)
    model = joblib.load(args.model_path)

    print("===== STAGE 2/6: LOAD PIT BACKTEST FRAME =====", flush=True)
    con = duckdb.connect(args.db_path)
    try:
        df = con.execute(f"""
        WITH future_prices AS (
            SELECT
                symbol,
                bar_date,
                adj_close,
                volume,
                LEAD(adj_close, {HOLD_DAYS}) OVER (
                    PARTITION BY symbol
                    ORDER BY bar_date
                ) AS adj_close_fwd
            FROM price_bars_adjusted
            WHERE adj_close IS NOT NULL
              AND adj_close > 0
              AND volume IS NOT NULL
              AND volume > 0
        )
        SELECT
            r.*,
            p.adj_close AS exec_close,
            p.volume AS exec_volume,
            (p.adj_close * p.volume) AS exec_dollar_volume,
            CASE
                WHEN p.adj_close_fwd IS NULL OR p.adj_close <= 0 OR p.adj_close_fwd <= 0 THEN NULL
                ELSE (p.adj_close_fwd / p.adj_close) - 1
            END AS realized_return
        FROM research_split_dataset r
        INNER JOIN research_universe_whitelist_20d_pit q
            ON r.symbol = q.symbol
           AND r.as_of_date = q.as_of_date
        INNER JOIN future_prices p
            ON r.symbol = p.symbol
           AND r.as_of_date = p.bar_date
        WHERE r.dataset_split = 'test'
          AND p.adj_close_fwd IS NOT NULL
        ORDER BY r.as_of_date, r.symbol
        """).fetchdf()
    finally:
        con.close()

    if df.empty:
        raise RuntimeError("Backtest frame is empty after PIT filtering.")

    df["realized_return"] = df["realized_return"].clip(
        lower=-float(args.realized_ret_cap),
        upper=float(args.realized_ret_cap),
    )

    print(f"loaded_rows = {len(df)}", flush=True)

    print("===== STAGE 3/6: SCORE UNIVERSE =====", flush=True)
    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split",
        "exec_close",
        "exec_volume",
        "exec_dollar_volume",
        "realized_return",
    ]
    X = df.drop(columns=drop_cols, errors="ignore")
    X = X.reindex(columns=model.booster_.feature_name())
    df["score"] = model.predict(X)

    print("===== STAGE 4/6: BUILD SLEEVES =====", flush=True)
    unique_dates = sorted(pd.to_datetime(df["as_of_date"]).unique())
    date_to_idx = {d: i for i, d in enumerate(unique_dates)}
    sleeves = []

    grouped = list(df.groupby("as_of_date"))
    for date, g in tqdm(grouped, desc=f"build_sleeves_{HOLD_DAYS}d", leave=True):
        if len(g) < 50:
            continue

        g = g.copy().sort_values("score")
        k = max(int(len(g) * float(args.bucket_pct)), 1)

        long = g.tail(k).copy()
        long["weight"] = 1.0 / len(long)
        long["weight"] = long["weight"].clip(upper=float(args.max_weight))

        wsum = float(long["weight"].sum())
        if wsum <= 0:
            continue
        long["weight"] = long["weight"] / wsum

        gross_h = float((long["weight"] * long["realized_return"]).sum())
        net_h = gross_h - (float(args.cost_bps) / 10000.0)

        sleeves.append({
            "date": pd.to_datetime(date),
            "gross_h": gross_h,
            "net_h": net_h,
            "names": int(len(long)),
        })

    sleeve_df = pd.DataFrame(sleeves)
    if sleeve_df.empty:
        raise RuntimeError("No sleeves produced in PIT backtest.")

    print("===== STAGE 5/6: BUILD DELEVERED EQUITY CURVE =====", flush=True)
    pnl_by_day = defaultdict(float)

    for _, row in tqdm(sleeve_df.iterrows(), total=len(sleeve_df), desc=f"amortize_{HOLD_DAYS}d", leave=True):
        start_idx = date_to_idx[row["date"]]
        daily_net = float(row["net_h"]) / (HOLD_DAYS * HOLD_DAYS)

        for offset in range(HOLD_DAYS):
            idx = start_idx + offset
            if idx >= len(unique_dates):
                break
            pnl_by_day[unique_dates[idx]] += daily_net

    capital = 1_000_000.0
    rows = []
    for d in tqdm(unique_dates, desc=f"compound_{HOLD_DAYS}d", leave=True):
        net = float(pnl_by_day.get(d, 0.0))
        if net <= -0.999:
            net = -0.999
        capital *= (1.0 + net)
        rows.append((d, capital, net))

    eq = pd.DataFrame(rows, columns=["date", "capital", "return"])

    print("===== STAGE 6/6: SUMMARIZE =====", flush=True)
    mean_daily = float(eq["return"].mean())
    std_daily = float(eq["return"].std())
    sharpe = mean_daily / (std_daily + 1e-9)

    print("===== RESULT =====", flush=True)
    print(json.dumps({
        "horizon": HOLD_DAYS,
        "model_path": args.model_path,
        "universe_table": "research_universe_whitelist_20d_pit",
        "final_capital": float(eq["capital"].iloc[-1]),
        "total_return": float(eq["capital"].iloc[-1] / 1_000_000.0 - 1.0),
        "mean_daily": mean_daily,
        "median_daily": float(eq["return"].median()),
        "sharpe": sharpe,
        "positive_days": float((eq["return"] > 0).mean()),
        "days": int(len(eq)),
        "avg_sleeve_gross": float(sleeve_df["gross_h"].mean()),
        "median_sleeve_gross": float(sleeve_df["gross_h"].median()),
        "avg_sleeve_net": float(sleeve_df["net_h"].mean()),
        "median_sleeve_net": float(sleeve_df["net_h"].median()),
        "avg_names": float(sleeve_df["names"].mean()),
    }, indent=2), flush=True)

    print("\n===== DAILY RETURN DISTRIBUTION =====", flush=True)
    print(
        eq["return"].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_string(),
        flush=True,
    )


if __name__ == "__main__":
    main()
