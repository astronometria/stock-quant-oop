#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
from tqdm import tqdm
import json

DB_PATH = "~/stock-quant-oop-runtime/db/market.duckdb"


def run():
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT
            symbol,
            as_of_date,
            returns_20d AS signal,
            target_return
        FROM research_split_dataset
        WHERE dataset_split = 'test'
          AND signal IS NOT NULL
          AND target_return IS NOT NULL
    """).fetchdf()

    con.close()

    results = []

    grouped = df.groupby("as_of_date")

    for date, g in tqdm(grouped, desc="Backtest reversed"):
        g = g.sort_values("signal")

        n = len(g)
        if n < 50:
            continue

        # INVERSION
        long = g.head(int(n * 0.1))
        short = g.tail(int(n * 0.1))

        long_ret = long["target_return"].mean()
        short_ret = short["target_return"].mean()

        results.append({
            "date": date,
            "long": long_ret,
            "short": short_ret,
            "long_short": long_ret - short_ret
        })

    res = pd.DataFrame(results)

    summary = {
        "mean_long": float(res["long"].mean()),
        "mean_short": float(res["short"].mean()),
        "mean_ls": float(res["long_short"].mean()),
        "sharpe_ls": float(res["long_short"].mean() / res["long_short"].std())
    }

    print("===== BACKTEST REVERSED =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
