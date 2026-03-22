#!/usr/bin/env python3
from __future__ import annotations

import json
import duckdb
import joblib
import pandas as pd

DB_PATH = "~/stock-quant-oop-runtime/db/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker.pkl"

AUM = 1_000_000.0
BUCKET_PCT = 0.05
COST_BPS = 20.0
MAX_WEIGHT = 0.02
REALIZED_RET_CAP = 0.50

# Known pathological placeholder / auction-like symbol seen in probes.
EXCLUDED_SYMBOLS = {"ZVZZT"}


def main() -> None:
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
    WITH next_day_prices AS (
        SELECT
            symbol,
            bar_date,
            adj_close,
            volume,
            LEAD(adj_close, 1) OVER (
                PARTITION BY symbol
                ORDER BY bar_date
            ) AS adj_close_next
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
            WHEN p.adj_close_next IS NULL OR p.adj_close <= 0 OR p.adj_close_next <= 0 THEN NULL
            ELSE (p.adj_close_next / p.adj_close) - 1
        END AS realized_return_1d
    FROM research_split_dataset r
    LEFT JOIN next_day_prices p
        ON r.symbol = p.symbol
       AND r.as_of_date = p.bar_date
    WHERE r.dataset_split = 'test'
      AND r.target_return IS NOT NULL
    ORDER BY r.as_of_date
    """).fetchdf()

    df = df[
        (df["exec_close"] >= 5.0) &
        (df["exec_dollar_volume"] >= 1_000_000.0) &
        (df["realized_return_1d"].notna()) &
        (~df["symbol"].isin(EXCLUDED_SYMBOLS))
    ].copy()

    df["realized_return_1d_raw"] = df["realized_return_1d"]
    df["realized_return_1d"] = df["realized_return_1d"].clip(
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
        "realized_return_1d",
        "realized_return_1d_raw",
    ]
    X = df.drop(columns=drop_cols, errors="ignore")
    model_features = model.booster_.feature_name()
    X = X.reindex(columns=model_features)
    df["score"] = model.predict(X)

    capital = AUM
    prev_weights: dict[str, float] = {}
    equity_rows: list[tuple] = []

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

        long["pnl"] = long["weight"] * long["realized_return_1d"]
        gross = float(long["pnl"].sum())

        current_weights = dict(zip(long["symbol"], long["weight"]))
        all_symbols = set(current_weights) | set(prev_weights)
        turnover = float(
            sum(abs(current_weights.get(s, 0.0) - prev_weights.get(s, 0.0)) for s in all_symbols)
        )

        cost = float(turnover * (COST_BPS / 10000.0))
        net = float(gross - cost)

        if net <= -0.999:
            net = -0.999

        capital *= (1.0 + net)

        equity_rows.append(
            (date, capital, gross, cost, net, turnover, len(long))
        )
        prev_weights = current_weights

    df_eq = pd.DataFrame(
        equity_rows,
        columns=["date", "capital", "gross", "cost", "return", "turnover", "names"],
    )

    if df_eq.empty:
        raise RuntimeError("No backtest rows produced after execution filters.")

    mean_daily = float(df_eq["return"].mean())
    std_daily = float(df_eq["return"].std())
    sharpe = mean_daily / (std_daily + 1e-9)

    result = {
        "mode": "long_only_realized_next_day_capped_ex_zvzzt",
        "excluded_symbols": sorted(EXCLUDED_SYMBOLS),
        "realized_ret_cap": REALIZED_RET_CAP,
        "final_capital": float(df_eq["capital"].iloc[-1]),
        "total_return": float(df_eq["capital"].iloc[-1] / AUM - 1.0),
        "mean_daily": mean_daily,
        "median_daily": float(df_eq["return"].median()),
        "sharpe": sharpe,
        "positive_days": float((df_eq["return"] > 0).mean()),
        "avg_turnover": float(df_eq["turnover"].mean()),
        "avg_gross_before_cost": float(df_eq["gross"].mean()),
        "avg_cost": float(df_eq["cost"].mean()),
        "avg_names": float(df_eq["names"].mean()),
        "days": int(len(df_eq)),
    }

    print("===== PORTFOLIO V3 (EX ZVZZT, CAPPED) =====")
    print(json.dumps(result, indent=2))
    print("\n===== DAILY RETURN DISTRIBUTION =====")
    print(df_eq["return"].describe(percentiles=[0.01,0.05,0.25,0.5,0.75,0.95,0.99]).to_string())


if __name__ == "__main__":
    main()
