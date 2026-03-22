#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
from tqdm import tqdm
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"


def run():
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT
            symbol,
            as_of_date,
            returns_20d,
            target_return
        FROM research_split_dataset
        WHERE dataset_split = 'test'
          AND returns_20d IS NOT NULL
          AND target_return IS NOT NULL
    """).fetchdf()

    con.close()

    results = []

    grouped = df.groupby("as_of_date")

    for date, g in tqdm(grouped, desc="Backtest zscore"):
        if len(g) < 50:
            continue

        # Z-score cross-section
        g["z"] = (g["returns_20d"] - g["returns_20d"].mean()) / g["returns_20d"].std()

        g = g.sort_values("z")

        n = len(g)
        long = g.head(int(n * 0.1))   # mean-reversion → low z
        short = g.tail(int(n * 0.1))  # high z

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
        "mean_ls": float(res["ls"].mean()),
        "sharpe_ls": float(res["ls"].mean() / res["ls"].std())
    }

    print("===== ZSCORE BACKTEST =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
