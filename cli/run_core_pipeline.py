#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the core stock-quant-oop pipeline end-to-end.")
    parser.add_argument("--db-path", default="~/stock-quant-oop/market.duckdb", help="Path to DuckDB database file.")
    parser.add_argument("--source-market", default="regular", choices=["regular", "otc", "both"], help="FINRA source market filter.")
    parser.add_argument("--drop-existing", action="store_true", help="Recreate schema before running.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def run_step(name: str, cmd: list[str], env: dict[str, str] | None = None) -> dict[str, object]:
    print(f"===== STEP: {name} =====")
    print("COMMAND:", " ".join(cmd))
    completed = subprocess.run(cmd, env=env, check=False)
    result = {
        "name": name,
        "returncode": completed.returncode,
        "ok": completed.returncode == 0,
    }
    print(f"RETURN_CODE: {completed.returncode}")
    print()
    return result


def main() -> int:
    args = parse_args()
    project_root = Path("~/stock-quant-oop").expanduser().resolve()
    db_path = str(Path(args.db_path).expanduser().resolve())

    py = sys.executable
    env = dict(**__import__("os").environ)
    env["PYTHONPATH"] = str(project_root)

    steps: list[tuple[str, list[str]]] = []

    init_cmd = [
        py,
        str(project_root / "cli" / "init_market_db.py"),
        "--db-path",
        db_path,
    ]
    if args.drop_existing:
        init_cmd.append("--drop-existing")
    if args.verbose:
        init_cmd.append("--verbose")
    steps.append(("init_market_db", init_cmd))

    load_universe_cmd = [
        py,
        str(project_root / "cli" / "load_symbol_reference_source_raw.py"),
        "--db-path",
        db_path,
        "--truncate",
    ]
    if args.verbose:
        load_universe_cmd.append("--verbose")
    steps.append(("load_symbol_reference_source_raw", load_universe_cmd))

    build_universe_cmd = [
        py,
        str(project_root / "cli" / "build_market_universe.py"),
        "--db-path",
        db_path,
    ]
    if args.verbose:
        build_universe_cmd.append("--verbose")
    steps.append(("build_market_universe", build_universe_cmd))

    build_symbol_reference_cmd = [
        py,
        str(project_root / "cli" / "build_symbol_reference.py"),
        "--db-path",
        db_path,
    ]
    if args.verbose:
        build_symbol_reference_cmd.append("--verbose")
    steps.append(("build_symbol_reference", build_symbol_reference_cmd))

    load_prices_cmd = [
        py,
        str(project_root / "cli" / "load_price_source_daily_raw.py"),
        "--db-path",
        db_path,
        "--truncate",
    ]
    if args.verbose:
        load_prices_cmd.append("--verbose")
    steps.append(("load_price_source_daily_raw", load_prices_cmd))

    build_prices_cmd = [
        py,
        str(project_root / "cli" / "build_prices.py"),
        "--db-path",
        db_path,
    ]
    if args.verbose:
        build_prices_cmd.append("--verbose")
    steps.append(("build_prices", build_prices_cmd))

    load_finra_cmd = [
        py,
        str(project_root / "cli" / "load_finra_short_interest_source_raw.py"),
        "--db-path",
        db_path,
        "--truncate",
    ]
    if args.verbose:
        load_finra_cmd.append("--verbose")
    steps.append(("load_finra_short_interest_source_raw", load_finra_cmd))

    build_finra_cmd = [
        py,
        str(project_root / "cli" / "build_finra_short_interest.py"),
        "--db-path",
        db_path,
        "--source-market",
        args.source_market,
    ]
    if args.verbose:
        build_finra_cmd.append("--verbose")
    steps.append(("build_finra_short_interest", build_finra_cmd))

    load_news_cmd = [
        py,
        str(project_root / "cli" / "load_news_source_raw.py"),
        "--db-path",
        db_path,
        "--truncate",
    ]
    if args.verbose:
        load_news_cmd.append("--verbose")
    steps.append(("load_news_source_raw", load_news_cmd))

    build_news_raw_cmd = [
        py,
        str(project_root / "cli" / "build_news_raw.py"),
        "--db-path",
        db_path,
    ]
    if args.verbose:
        build_news_raw_cmd.append("--verbose")
    steps.append(("build_news_raw", build_news_raw_cmd))

    build_news_candidates_cmd = [
        py,
        str(project_root / "cli" / "build_news_symbol_candidates.py"),
        "--db-path",
        db_path,
    ]
    if args.verbose:
        build_news_candidates_cmd.append("--verbose")
    steps.append(("build_news_symbol_candidates", build_news_candidates_cmd))

    summary: list[dict[str, object]] = []
    for name, cmd in steps:
        result = run_step(name, cmd, env=env)
        summary.append(result)
        if not result["ok"]:
            print("===== CORE PIPELINE SUMMARY =====")
            print(json.dumps(summary, indent=2, sort_keys=True))
            return 1

    print("===== CORE PIPELINE SUMMARY =====")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
