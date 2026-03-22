#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import joblib
import pandas as pd
from tqdm import tqdm
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker.pkl"

AUM = 1_000_000
BUCKET_PCT = 0.05
COST_BPS = 20
MAX_WEIGHT = 0.02


def zscore(s):
    std = s.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def main():
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT
            r.*,
            p.volume,
            p.adj_close AS close,
            p.adj_close * p.volume AS dollar_volume
        FROM research_split_dataset r
        LEFT JOIN price_bars_adjusted p
            ON r.symbol = p.symbol
           AND r.as_of_date = p.bar_date
        WHERE r.dataset_split = 'test'
          AND r.target_return IS NOT NULL
        ORDER BY r.as_of_date
    """).fetchdf()

    # ===== FILTER =====
    df = df[
        (df["close"] >= 5) &
        (df["dollar_volume"] >= 1_000_000)
    ]

    # ===== DROP NON-FEATURES =====
    drop_cols = [
        "symbol","instrument_id","company_id",
        "as_of_date","target_return","target_class","dataset_split",
        "volume","dollar_volume"
    ]

    X = df.drop(columns=drop_cols, errors="ignore")

    # ===== CRITICAL FIX =====
    # Align EXACT features used in training
    model_features = model.booster_.feature_name()

    # Keep only those columns
    X = X[model_features]

    # Ensure order is correct
    X = X.reindex(columns=model_features)

    df["score"] = model.predict(X)

    capital = AUM
    prev_weights = {}
    equity_curve = []

    for date, g in tqdm(df.groupby("as_of_date"), desc="portfolio_v3"):

        if len(g) < 50:
            continue

        g = g.copy()
        g["score_z"] = zscore(g["score"])

        market_ret = g["target_return"].mean()

        g = g.sort_values("score_z")

        n = len(g)
        k = max(int(n * BUCKET_PCT), 1)

        long = g.tail(k).copy()
        short = g.head(k).copy()

        long_w = long["score_z"].clip(lower=0)
        long_w = long_w / (long_w.abs().sum() + 1e-9)

        short_w = short["score_z"].clip(upper=0)
        short_w = -short_w.abs() / (short_w.abs().sum() + 1e-9)

        long["weight"] = long_w
        short["weight"] = short_w

        book = pd.concat([long, short])

        book["weight"] = book["weight"].clip(-MAX_WEIGHT, MAX_WEIGHT)
        book["weight"] /= book["weight"].abs().sum()

        book["ret"] = book["target_return"] - market_ret
        book["pnl"] = book["weight"] * book["ret"]

        gross = book["pnl"].sum()

        current_weights = dict(zip(book["symbol"], book["weight"]))
        symbols = set(current_weights) | set(prev_weights)

        turnover = sum(abs(current_weights.get(s,0)-prev_weights.get(s,0)) for s in symbols)
        cost = turnover * (COST_BPS / 10000)

        net = gross - cost
        capital *= (1 + net)

        equity_curve.append((date, capital, net, turnover))

        prev_weights = current_weights

    df_eq = pd.DataFrame(equity_curve, columns=[
        "date","capital","return","turnover"
    ])

    print("===== PORTFOLIO V3 =====")
    print(json.dumps({
        "final_capital": float(df_eq["capital"].iloc[-1]),
        "total_return": float(df_eq["capital"].iloc[-1] / AUM - 1),
        "mean_daily": float(df_eq["return"].mean()),
        "sharpe": float(df_eq["return"].mean() / df_eq["return"].std()),
        "avg_turnover": float(df_eq["turnover"].mean()),
        "days": int(len(df_eq))
    }, indent=2))

    con.close()


if __name__ == "__main__":
    main()
