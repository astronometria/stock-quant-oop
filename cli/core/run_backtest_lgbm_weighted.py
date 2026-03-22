#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
import joblib
from tqdm import tqdm
import json

DB_PATH = "~/stock-quant-oop-runtime/db/market.duckdb"


def run():
    model = joblib.load("models/lgbm_model.pkl")

    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split = 'test'
    """).fetchdf()

    con.close()

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

    df["score"] = model.predict_proba(X)[:, 1]

    results = []

    for date, g in tqdm(df.groupby("as_of_date"), desc="Backtest weighted"):
        if len(g) < 50:
            continue

        # normalize score (centered)
        g["alpha"] = g["score"] - g["score"].mean()

        # weights
        g["w"] = g["alpha"] / g["alpha"].abs().sum()

        pnl = (g["w"] * g["target_return"]).sum()

        results.append(pnl)

    res = pd.Series(results)

    summary = {
        "mean_ls": float(res.mean()),
        "sharpe_ls": float(res.mean() / res.std())
    }

    print("===== ML WEIGHTED BACKTEST =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
