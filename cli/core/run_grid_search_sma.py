#!/usr/bin/env python3

"""
Grid search SMA cross

Objectif:
- Tester plusieurs combinaisons SMA
- Alimenter research_backtest + research_signal_performance

"""

import subprocess
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
DATASET_ID = "research_snapshot_short_equities_20260318T155506Z_split_20260318T204144Z_dataset_20260321T003602Z"
SPLIT_ID = "split_20260318T204144Z"

SMA_PAIRS = [
    ("sma_20", "sma_50"),
    ("sma_20", "sma_200"),
    ("sma_50", "sma_200"),
]


def run_backtest(fast, slow):
    params = {
        "fast_feature_name": fast,
        "slow_feature_name": slow,
    }

    cmd = [
        "python3",
        "cli/core/build_research_backtest.py",
        "--db-path", DB_PATH,
        "--dataset-id", DATASET_ID,
        "--split-id", SPLIT_ID,
        "--signal-name", "sma_cross",
        "--signal-params-json", json.dumps(params),
        "--execution-lag-bars", "1",
        "--transaction-cost-bps", "10",
        "--memory-limit", "24GB",
        "--threads", "4",
        "--temp-dir", "/home/marty/stock-quant-oop/tmp",
    ]

    print(f"[GRID] running {fast} vs {slow}")

    subprocess.run(cmd, check=True)


def main():
    for fast, slow in SMA_PAIRS:
        run_backtest(fast, slow)

    print("[GRID] rebuilding performance table")

    subprocess.run([
        "python3",
        "cli/core/build_research_signal_performance.py",
        "--db-path", DB_PATH,
    ], check=True)


if __name__ == "__main__":
    main()
