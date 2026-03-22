#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
import joblib
from tqdm import tqdm
import json

DB_PATH = "~/stock-quant-oop-runtime/db/market.duckdb"


def run():
    # load model
    model = joblib.load("models/lgbm_model.pkl")

    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split = 'test'
    """).fetchdf()

    con.close()

    # features
    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split"
    ]

    X = df.drop(columns=drop_cols)

    # prediction
    df["score"] = model.predict_proba(X)[:, 1]

    results = []

    grouped = df.groupby("as_of_date")

    for date, g in tqdm(grouped, desc="Backtest ML"):
        if len(g) < 50:
            continue

        g = g.sort_values("score")

        n = len(g)
        long = g.tail(int(n * 0.1))
        short = g.head(int(n * 0.1))

        long_ret = long["target_return"].mean()
        short_ret = short["target_return"].mean()

        results.append({
            "date": date,
            "long": long_ret,
            "short": short_ret,
            "ls": long_ret - short_ret
        })

    res = pd.DataFrame(results)

    summary = {
        "mean_long": float(res["long"].mean()),
        "mean_short": float(res["short"].mean()),
        "mean_ls": float(res["ls"].mean()),
        "sharpe_ls": float(res["ls"].mean() / res["ls"].std())
    }

    print("===== ML BACKTEST =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
