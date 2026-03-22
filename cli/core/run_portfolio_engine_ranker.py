#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import joblib
import pandas as pd
from tqdm import tqdm
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker.pkl"

BUCKET_PCT = 0.05
TRANSACTION_COST_BPS = 10


def zscore(s):
    std = s.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def main():
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    # ===== LOAD DATA =====
    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split = 'test'
          AND target_return IS NOT NULL
        ORDER BY as_of_date
    """).fetchdf()

    drop_cols = [
        "symbol","instrument_id","company_id",
        "as_of_date","target_return","target_class","dataset_split"
    ]

    X = df.drop(columns=drop_cols)
    df["score"] = model.predict(X)

    daily_results = []
    positions = []

    prev_weights = {}

    # ===== BACKTEST =====
    for date, g in tqdm(df.groupby("as_of_date"), desc="portfolio_engine"):

        if len(g) < 50:
            continue

        g = g.copy()
        g["score_z"] = zscore(g["score"])
        g = g.sort_values("score_z")

        n = len(g)
        k = max(int(n * BUCKET_PCT), 1)

        long = g.tail(k).copy()
        short = g.head(k).copy()

        # ===== WEIGHTS =====
        long_w = long["score_z"].clip(lower=0)
        long_w = long_w / (long_w.abs().sum() + 1e-9)

        short_w = short["score_z"].clip(upper=0)
        short_w = -short_w.abs() / (short_w.abs().sum() + 1e-9)

        long["weight"] = long_w
        short["weight"] = short_w

        book = pd.concat([long, short])

        # ===== RETURNS =====
        book["pnl"] = book["weight"] * book["target_return"]
        gross = book["pnl"].sum()

        # ===== TURNOVER =====
        current_weights = dict(zip(book["symbol"], book["weight"]))
        symbols = set(current_weights) | set(prev_weights)

        turnover = sum(abs(current_weights.get(s,0)-prev_weights.get(s,0)) for s in symbols)

        cost = turnover * (TRANSACTION_COST_BPS / 10000)
        net = gross - cost

        hit = (book["pnl"] > 0).mean()

        daily_results.append((date, len(book), net, hit))

        # ===== STORE POSITIONS =====
        for r in book.itertuples():
            positions.append((
                "ranker_portfolio_v2",
                r.symbol,
                r.as_of_date,
                float(r.score_z),
                float(r.target_return),
                float(r.weight)
            ))

        prev_weights = current_weights

    # ===== FAST INSERT =====

    df_daily = pd.DataFrame(daily_results, columns=[
        "as_of_date","selected_count","return","hit_rate"
    ])

    df_pos = pd.DataFrame(positions, columns=[
        "backtest_name","symbol","as_of_date",
        "signal","target_return","weight"
    ])

    con.execute("DELETE FROM backtest_daily WHERE backtest_name='ranker_portfolio_v2'")
    con.execute("DELETE FROM backtest_positions WHERE backtest_name='ranker_portfolio_v2'")

    # 🚀 BULK INSERT (ULTRA FAST)
    con.register("df_daily", df_daily)
    con.register("df_pos", df_pos)

    con.execute("""
        INSERT INTO backtest_daily
        SELECT
            'ranker_portfolio_v2',
            'research',
            'v2',
            as_of_date,
            'score_z',
            'target_return',
            selected_count,
            return,
            hit_rate,
            CURRENT_TIMESTAMP
        FROM df_daily
    """)

    con.execute("""
        INSERT INTO backtest_positions
        SELECT
            backtest_name,
            'research',
            'v2',
            NULL,
            NULL,
            symbol,
            as_of_date,
            'score_z',
            'target_return',
            signal,
            target_return,
            NULL,
            weight,
            CURRENT_TIMESTAMP
        FROM df_pos
    """)

    # ===== SUMMARY =====
    res = df_daily["return"]

    print("===== PORTFOLIO ENGINE V2 =====")
    print(json.dumps({
        "mean_daily": float(res.mean()),
        "sharpe": float(res.mean() / res.std()),
        "days": int(len(res)),
        "avg_positions": int(df_daily["selected_count"].mean())
    }, indent=2))

    con.close()


if __name__ == "__main__":
    main()
