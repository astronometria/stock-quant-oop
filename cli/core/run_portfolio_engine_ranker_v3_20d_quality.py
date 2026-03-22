#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import defaultdict

import duckdb
import joblib
import pandas as pd

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker_20d_quality.pkl"

AUM = 1_000_000.0
BUCKET_PCT = 0.05
MAX_WEIGHT = 0.02
HOLD_DAYS = 20
COST_BPS = 10.0
REALIZED_RET_CAP = 2.0


def main() -> None:
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    # -------------------------------------------------------------------------
    # PIT universe:
    # - join on symbol + as_of_date
    # - realized 20d return comes from canonical adjusted prices
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
        END AS realized_return_20d
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

    df["realized_return_20d"] = df["realized_return_20d"].clip(-REALIZED_RET_CAP, REALIZED_RET_CAP)

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
    ]
    X = df.drop(columns=drop_cols, errors="ignore")
    X = X.reindex(columns=model.booster_.feature_name())
    df["score"] = model.predict(X)

    unique_dates = sorted(pd.to_datetime(df["as_of_date"]).unique())
    date_to_idx = {d: i for i, d in enumerate(unique_dates)}
    sleeves = []

    for date, g in df.groupby("as_of_date"):
        if len(g) < 50:
            continue

        g = g.copy().sort_values("score")
        k = max(int(len(g) * BUCKET_PCT), 1)

        long = g.tail(k).copy()
        long["weight"] = 1.0 / len(long)
        long["weight"] = long["weight"].clip(upper=MAX_WEIGHT)

        wsum = float(long["weight"].sum())
        if wsum <= 0:
            continue
        long["weight"] = long["weight"] / wsum

        gross_20d = float((long["weight"] * long["realized_return_20d"]).sum())
        net_20d = gross_20d - (COST_BPS / 10000.0)

        sleeves.append({
            "date": pd.to_datetime(date),
            "gross_20d": gross_20d,
            "net_20d": net_20d,
            "names": int(len(long)),
        })

    sleeve_df = pd.DataFrame(sleeves)
    if sleeve_df.empty:
        raise RuntimeError("No sleeves produced in PIT whitelist backtest.")

    pnl_by_day = defaultdict(float)

    for _, row in sleeve_df.iterrows():
        start_idx = date_to_idx[row["date"]]

        # ---------------------------------------------------------------------
        # Each sleeve uses only 1/HOLD_DAYS of portfolio capital.
        # And its 20d return is amortized over HOLD_DAYS days.
        # This avoids implicit ~20x leverage from overlapping sleeves.
        # ---------------------------------------------------------------------
        daily_net = float(row["net_20d"]) / (HOLD_DAYS * HOLD_DAYS)

        for offset in range(HOLD_DAYS):
            idx = start_idx + offset
            if idx >= len(unique_dates):
                break
            pnl_by_day[unique_dates[idx]] += daily_net

    capital = AUM
    rows = []
    for d in unique_dates:
        net = float(pnl_by_day.get(d, 0.0))
        if net <= -0.999:
            net = -0.999
        capital *= (1.0 + net)
        rows.append((d, capital, net))

    eq = pd.DataFrame(rows, columns=["date", "capital", "return"])
    mean_daily = float(eq["return"].mean())
    std_daily = float(eq["return"].std())
    sharpe = mean_daily / (std_daily + 1e-9)

    print("===== PORTFOLIO V3 20D QUALITY =====")
    print(json.dumps({
        "mode": "20d_aligned_pit_whitelist",
        "model_path": MODEL_PATH,
        "universe_table": "research_universe_whitelist_20d_pit",
        "final_capital": float(eq["capital"].iloc[-1]),
        "total_return": float(eq["capital"].iloc[-1] / AUM - 1.0),
        "mean_daily": mean_daily,
        "median_daily": float(eq["return"].median()),
        "sharpe": sharpe,
        "positive_days": float((eq["return"] > 0).mean()),
        "days": int(len(eq)),
        "avg_sleeve_gross_20d": float(sleeve_df["gross_20d"].mean()),
        "median_sleeve_gross_20d": float(sleeve_df["gross_20d"].median()),
        "avg_sleeve_net_20d": float(sleeve_df["net_20d"].mean()),
        "median_sleeve_net_20d": float(sleeve_df["net_20d"].median()),
        "avg_names": float(sleeve_df["names"].mean()),
    }, indent=2))
    print("\n===== DAILY RETURN DISTRIBUTION =====")
    print(eq["return"].describe(percentiles=[0.01,0.05,0.25,0.5,0.75,0.95,0.99]).to_string())


if __name__ == "__main__":
    main()
