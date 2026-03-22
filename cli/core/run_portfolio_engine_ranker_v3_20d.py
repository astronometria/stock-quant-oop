#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Portfolio Engine V3 - 20D aligned holding backtest
#
# Goal:
# - use a model trained on a 20-day forward horizon
# - hold each daily sleeve for 20 trading days
# - use realized 20-day adjusted return for the sleeve pnl
# - aggregate overlapping sleeves into a smoother aligned portfolio test
#
# Important:
# - This is a research backtest, not an execution simulator.
# - We compute one sleeve per date from the top bucket.
# - Each sleeve contributes its realized 20D return, then we spread that pnl
#   evenly across the 20 holding days to create an overlapped daily equity curve.
# =============================================================================

import json
from collections import defaultdict

import duckdb
import joblib
import pandas as pd

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker_20d_aligned.pkl"

AUM = 1_000_000.0
BUCKET_PCT = 0.05
MAX_WEIGHT = 0.02
HOLD_DAYS = 20
COST_BPS = 20.0
REALIZED_RET_CAP = 2.0
EXCLUDED_SYMBOLS = {"ZVZZT"}


def main() -> None:
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    excluded_sql = ""
    if EXCLUDED_SYMBOLS:
        quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in sorted(EXCLUDED_SYMBOLS))
        excluded_sql = f"AND r.symbol NOT IN ({quoted})"

    # -------------------------------------------------------------------------
    # realized_return_20d is built directly from adjusted prices.
    # This matches the model horizon.
    # -------------------------------------------------------------------------
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
    )
    SELECT
        r.*,
        p.adj_close AS exec_close,
        p.volume AS exec_volume,
        p.adj_close * p.volume AS exec_dollar_volume,
        CASE
            WHEN p.adj_close_fwd IS NULL OR p.adj_close <= 0 OR p.adj_close_fwd <= 0 THEN NULL
            ELSE (p.adj_close_fwd / p.adj_close) - 1
        END AS realized_return_20d
    FROM research_split_dataset r
    INNER JOIN future_prices p
        ON r.symbol = p.symbol
       AND r.as_of_date = p.bar_date
    WHERE r.dataset_split = 'test'
      AND p.adj_close >= 5
      AND (p.adj_close * p.volume) >= 1000000
      AND p.adj_close_fwd IS NOT NULL
      {excluded_sql}
    ORDER BY r.as_of_date, r.symbol
    """).fetchdf()

    df["realized_return_20d_raw"] = df["realized_return_20d"]
    df["realized_return_20d"] = df["realized_return_20d"].clip(
        lower=-REALIZED_RET_CAP,
        upper=REALIZED_RET_CAP,
    )

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
        "realized_return_20d",
        "realized_return_20d_raw",
    ]
    X = df.drop(columns=drop_cols, errors="ignore")
    X = X.reindex(columns=model.booster_.feature_name())

    df["score"] = model.predict(X)

    # -------------------------------------------------------------------------
    # Build one daily sleeve from the top bucket.
    # We compute the 20D sleeve return, then amortize it over HOLD_DAYS so that
    # overlapping sleeves produce a daily equity curve.
    # -------------------------------------------------------------------------
    sleeve_rows = []
    unique_dates = sorted(pd.to_datetime(df["as_of_date"]).unique())
    date_to_idx = {d: i for i, d in enumerate(unique_dates)}

    for date, g in df.groupby("as_of_date"):
        if len(g) < 50:
            continue

        g = g.copy().sort_values("score")
        n = len(g)
        k = max(int(n * BUCKET_PCT), 1)

        long = g.tail(k).copy()
        long["weight"] = 1.0 / len(long)
        long["weight"] = long["weight"].clip(upper=MAX_WEIGHT)

        weight_sum = float(long["weight"].sum())
        if weight_sum <= 0:
            continue
        long["weight"] = long["weight"] / weight_sum

        sleeve_gross_20d = float((long["weight"] * long["realized_return_20d"]).sum())

        # Simple sleeve entry cost.
        # Since we are launching one new sleeve per day, this is a stable proxy.
        sleeve_cost = float(COST_BPS / 10000.0)
        sleeve_net_20d = float(sleeve_gross_20d - sleeve_cost)

        date_ts = pd.to_datetime(date)
        entry_idx = date_to_idx.get(date_ts)
        if entry_idx is None:
            continue

        sleeve_rows.append({
            "date": date_ts,
            "n": int(n),
            "k": int(k),
            "sleeve_gross_20d": sleeve_gross_20d,
            "sleeve_cost": sleeve_cost,
            "sleeve_net_20d": sleeve_net_20d,
        })

    sleeve_df = pd.DataFrame(sleeve_rows)
    if sleeve_df.empty:
        raise RuntimeError("No sleeve rows produced in 20D aligned backtest.")

    # -------------------------------------------------------------------------
    # Build daily overlapped pnl:
    # every sleeve contributes 1/HOLD_DAYS of its 20D pnl to each day in the
    # next HOLD_DAYS slots. This approximates a fully overlapped research book.
    # -------------------------------------------------------------------------
    pnl_by_day = defaultdict(float)
    cost_by_day = defaultdict(float)

    for _, row in sleeve_df.iterrows():
        start_idx = date_to_idx[row["date"]]
        daily_net = float(row["sleeve_net_20d"]) / HOLD_DAYS
        daily_cost = float(row["sleeve_cost"]) / HOLD_DAYS

        for offset in range(HOLD_DAYS):
            idx = start_idx + offset
            if idx >= len(unique_dates):
                break
            d = unique_dates[idx]
            pnl_by_day[d] += daily_net
            cost_by_day[d] += daily_cost

    equity_rows = []
    capital = AUM

    for d in unique_dates:
        net = float(pnl_by_day.get(d, 0.0))

        if net <= -0.999:
            net = -0.999

        capital *= (1.0 + net)
        equity_rows.append({
            "date": d,
            "capital": capital,
            "return": net,
            "cost_component": float(cost_by_day.get(d, 0.0)),
        })

    eq = pd.DataFrame(equity_rows)
    eq = eq[eq["return"].notna()].copy()

    mean_daily = float(eq["return"].mean())
    std_daily = float(eq["return"].std())
    sharpe = mean_daily / (std_daily + 1e-9)

    result = {
        "mode": "20d_aligned_overlapped_long_only",
        "model_path": MODEL_PATH,
        "excluded_symbols": sorted(EXCLUDED_SYMBOLS),
        "hold_days": HOLD_DAYS,
        "realized_ret_cap": REALIZED_RET_CAP,
        "final_capital": float(eq["capital"].iloc[-1]),
        "total_return": float(eq["capital"].iloc[-1] / AUM - 1.0),
        "mean_daily": mean_daily,
        "median_daily": float(eq["return"].median()),
        "sharpe": sharpe,
        "positive_days": float((eq["return"] > 0).mean()),
        "avg_daily_cost_component": float(eq["cost_component"].mean()),
        "days": int(len(eq)),
        "sleeves": int(len(sleeve_df)),
        "avg_sleeve_net_20d": float(sleeve_df["sleeve_net_20d"].mean()),
        "median_sleeve_net_20d": float(sleeve_df["sleeve_net_20d"].median()),
    }

    print("===== PORTFOLIO V3 20D ALIGNED =====")
    print(json.dumps(result, indent=2))
    print("\n===== DAILY RETURN DISTRIBUTION =====")
    print(eq["return"].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_string())
    print("\n===== SLEEVE RETURN DISTRIBUTION (20D) =====")
    print(sleeve_df["sleeve_net_20d"].describe(percentiles=[0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).to_string())


if __name__ == "__main__":
    main()
