#!/usr/bin/env python3

"""
Rebuild FULL database from scratch.

Assumptions:
- stooq data already present locally
- no existing DB required
- runs ALL pipelines in correct dependency order

This script is SAFE to run on a fresh machine.

Design:
- SQL-first pipelines reused
- Python only orchestrates
- Each step logs clearly
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


# =========================
# Helpers
# =========================

def run_step(name: str, cmd: list[str]) -> None:
    """
    Execute a subprocess step with logging.

    - prints step header
    - streams stdout live
    - fails fast if error
    """
    print("\n" + "=" * 60)
    print(f"===== STEP START: {name} =====")
    print("cmd =", " ".join(cmd))
    print("=" * 60)

    start = time.time()

    result = subprocess.run(cmd)

    elapsed = time.time() - start

    print("-----")
    print(f"===== STEP END: {name} =====")
    print(f"return_code={result.returncode}")
    print(f"elapsed_seconds={round(elapsed, 2)}")
    print("=" * 60)

    if result.returncode != 0:
        print(f"❌ Step failed: {name}")
        sys.exit(result.returncode)


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--db-path", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--python-bin", default="python3")

    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    python_bin = args.python_bin
    db_path = args.db_path

    print("===== REBUILD DB FROM SCRATCH =====")
    print("repo_root =", repo_root)
    print("db_path   =", db_path)
    print("python    =", python_bin)

    # =========================
    # 1. INIT DB
    # =========================
    run_step(
        "init_db",
        [
            python_bin,
            "cli/init_market_db.py",
            "--db-path",
            db_path,
        ],
    )

    # =========================
    # 2. RESEARCH UNIVERSE
    # =========================
    run_step(
        "build_research_universe",
        [
            python_bin,
            "cli/build_research_universe.py",
            "--db-path",
            db_path,
        ],
    )

    # =========================
    # 3. PRICES (STOOQ FULL)
    # =========================
    run_step(
        "build_prices_full_stooq",
        [
            python_bin,
            "cli/core/build_prices.py",
            "--db-path",
            db_path,
            "--mode",
            "full",
        ],
    )

    # =========================
    # 4. FINRA
    # =========================
    run_step(
        "build_finra_pipeline",
        [
            python_bin,
            "cli/run_short_interest_pipeline.py",
            "--db-path",
            db_path,
        ],
    )

    # =========================
    # 5. SEC
    # =========================
    run_step(
        "build_sec_pipeline",
        [
            python_bin,
            "cli/run_sec_pipeline.py",
            "--db-path",
            db_path,
        ],
    )

    # =========================
    # 6. RESEARCH FEATURES
    # =========================
    run_step(
        "build_research_features_daily",
        [
            python_bin,
            "cli/build_market_features_daily.py",
            "--db-path",
            db_path,
        ],
    )

    # =========================
    # DONE
    # =========================
    print("\n" + "=" * 60)
    print("✅ REBUILD COMPLETE")
    print("finished_at =", datetime.utcnow().isoformat())
    print("=" * 60)


if __name__ == "__main__":
    main()

