#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
import joblib
from tqdm import tqdm
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"


def run():
    model = joblib.load("models/lgbm_ranker.pkl")

    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split = 'test'
          AND target_return IS NOT NULL
    """).fetchdf()

    con.close()

    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split",
    ]

    X = df.drop(columns=drop_cols)

    df["score"] = model.predict(X)

    results = []

    for date, g in tqdm(df.groupby("as_of_date"), desc="Backtest ranker v2"):

        if len(g) < 50:
            continue

        # Z-score normalization (cross-section)
        g["score_z"] = (g["score"] - g["score"].mean()) / (g["score"].std() + 1e-9)

        # Select top/bottom 5%
        g = g.sort_values("score_z")

        n = len(g)
        bucket = max(int(n * 0.05), 1)

        long = g.tail(bucket)
        short = g.head(bucket)

        # Weight by score strength
        long_w = long["score_z"] / long["score_z"].abs().sum()
        short_w = short["score_z"] / short["score_z"].abs().sum()

        long_ret = (long["target_return"] * long_w).sum()
        short_ret = (short["target_return"] * short_w).sum()

        results.append({
            "date": date,
            "long": long_ret,
            "short": short_ret,
            "ls": long_ret - short_ret,
        })

    res = pd.DataFrame(results)

    summary = {
        "mean_long": float(res["long"].mean()),
        "mean_short": float(res["short"].mean()),
        "mean_ls": float(res["ls"].mean()),
        "sharpe_ls": float(res["ls"].mean() / res["ls"].std()),
        "days": int(len(res)),
    }

    print("===== RANKER BACKTEST V2 =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
