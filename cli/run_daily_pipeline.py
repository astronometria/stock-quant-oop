#!/usr/bin/env python3
"""
Daily pipeline orchestrator

Runs:
- prices (yfinance)
- FINRA short volume + short interest
- SEC filings

Design goals:
- sequential (avoid DB contention)
- visible tqdm output
- logs written to logs/
- fail-fast but logged
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path("/home/marty/stock-quant-oop")
DB_PATH = PROJECT_ROOT / "market.duckdb"
LOG_DIR = PROJECT_ROOT / "logs"


def run_step(name: str, cmd: list[str]) -> None:
    """
    Run a subprocess step with logging and clear output.

    - Streams output live (keeps tqdm visible)
    - Writes to dedicated log file
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_file = LOG_DIR / f"{name}_{timestamp}.log"

    print(f"\n===== RUN STEP: {name} =====")
    print("cmd =", " ".join(cmd))
    print("log =", log_file)

    with open(log_file, "w") as f:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=PROJECT_ROOT,
        )

        # Stream output live AND write to file
        for line in process.stdout:
            print(line, end="")
            f.write(line)

        process.wait()

        if process.returncode != 0:
            print(f"\n❌ STEP FAILED: {name}")
            sys.exit(process.returncode)

    print(f"✅ STEP OK: {name}")


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("===== DAILY PIPELINE START =====")
    print("db_path =", DB_PATH)

    # =========================
    # 1. PRICES (yfinance)
    # =========================
    run_step(
        "build_prices",
        [
            "python3",
            "cli/core/build_prices.py",
            "--db-path", str(DB_PATH),
            "--mode", "daily",
        ],
    )

    # =========================
    # 2. FINRA SHORT VOLUME
    # =========================
    run_step(
        "build_finra_short_volume",
        [
            "python3",
            "cli/core/build_finra_short_volume.py",
            "--db-path", str(DB_PATH),
        ],
    )

    # =========================
    # 3. FINRA SHORT INTEREST
    # =========================
    run_step(
        "build_finra_short_interest",
        [
            "python3",
            "cli/core/build_finra_short_interest.py",
            "--db-path", str(DB_PATH),
        ],
    )

    # =========================
    # 4. SHORT FEATURES
    # =========================
    run_step(
        "build_short_features",
        [
            "python3",
            "cli/core/build_short_features.py",
            "--db-path", str(DB_PATH),
            "--duckdb-threads", "2",
            "--duckdb-memory-limit", "36GB",
        ],
    )

    # =========================
    # 5. SEC FILINGS
    # =========================
    run_step(
        "build_sec_filings",
        [
            "python3",
            "cli/core/build_sec_filings.py",
            "--db-path", str(DB_PATH),
        ],
    )

    print("\n===== DAILY PIPELINE SUCCESS =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
