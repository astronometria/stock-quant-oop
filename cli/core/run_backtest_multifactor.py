#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
from tqdm import tqdm
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"


def zscore(s):
    return (s - s.mean()) / s.std()


def run():
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT
            symbol,
            as_of_date,
            returns_20d,
            volatility_20,
            short_volume_ratio,
            target_return
        FROM research_split_dataset
        WHERE dataset_split = 'test'
          AND returns_20d IS NOT NULL
          AND volatility_20 IS NOT NULL
          AND short_volume_ratio IS NOT NULL
          AND target_return IS NOT NULL
    """).fetchdf()

    con.close()

    results = []

    grouped = df.groupby("as_of_date")

    for date, g in tqdm(grouped, desc="Backtest multifactor"):
        if len(g) < 50:
            continue

        # Z-scores
        g["z_mom"] = zscore(g["returns_20d"])
        g["z_vol"] = zscore(g["volatility_20"])
        g["z_short"] = zscore(g["short_volume_ratio"])

        # alpha composite
        g["alpha"] = (
            - g["z_mom"]        # mean reversion
            - g["z_vol"]        # low vol
            + g["z_short"]      # short squeeze
        )

        g = g.sort_values("alpha")

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
        "mean_ls": float(res["ls"].mean()),
        "sharpe_ls": float(res["ls"].mean() / res["ls"].std())
    }

    print("===== MULTIFACTOR RESULT =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    run()
