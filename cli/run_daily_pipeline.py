#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run unified daily market refresh for prices, FINRA, SEC fundamentals, then status."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop/market.duckdb",
        help="DuckDB database path.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter for price refresh. Repeat this flag.",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Optional price target date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--price-start-date",
        default=None,
        help="Optional price start date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--price-end-date",
        default=None,
        help="Optional price end date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--finra-start-date",
        default=None,
        help="Optional FINRA start date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--finra-end-date",
        default=None,
        help="Optional FINRA end date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--finra-output-dir",
        default="~/stock-quant-oop/data/raw/finra_short_interest",
        help="Directory for FINRA downloaded files.",
    )
    parser.add_argument(
        "--finra-source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="FINRA source market used during normalized build.",
    )
    parser.add_argument(
        "--sec-source",
        action="append",
        dest="sec_sources",
        default=[],
        help="Optional SEC raw index CSV source. Repeat this flag.",
    )
    parser.add_argument("--skip-prices", action="store_true")
    parser.add_argument("--skip-finra", action="store_true")
    parser.add_argument("--skip-sec", action="store_true")
    parser.add_argument(
        "--skip-sec-load",
        action="store_true",
        help="Skip SEC raw index load and rebuild from already-loaded SEC raw data.",
    )
    parser.add_argument("--overwrite-finra-downloads", action="store_true")
    parser.add_argument("--truncate-finra-raw", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def run_step(name: str, cmd: list[str], project_root: Path) -> None:
    completed = subprocess.run(cmd, cwd=project_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {name} (exit={completed.returncode})")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()
    db_path = str(Path(args.db_path).expanduser().resolve())

    steps: list[tuple[str, list[str]]] = []

    if not args.skip_prices:
        price_cmd = [
            sys.executable,
            str(project_root / "cli" / "ops" / "run_price_daily_refresh.py"),
            "--project-root", str(project_root),
            "--db-path", db_path,
        ]
        for symbol in args.symbols:
            price_cmd.extend(["--symbol", symbol])
        if args.as_of:
            price_cmd.extend(["--as-of", args.as_of])
        else:
            if args.price_start_date:
                price_cmd.extend(["--start-date", args.price_start_date])
            if args.price_end_date:
                price_cmd.extend(["--end-date", args.price_end_date])
        if args.verbose:
            price_cmd.append("--verbose")
        steps.append(("run_price_daily_refresh", price_cmd))

    if not args.skip_finra:
        finra_cmd = [
            sys.executable,
            str(project_root / "cli" / "ops" / "run_finra_daily_refresh.py"),
            "--project-root", str(project_root),
            "--db-path", db_path,
            "--output-dir", str(Path(args.finra_output_dir).expanduser().resolve()),
            "--source-market", args.finra_source_market,
        ]
        if args.finra_start_date:
            finra_cmd.extend(["--start-date", args.finra_start_date])
        if args.finra_end_date:
            finra_cmd.extend(["--end-date", args.finra_end_date])
        if args.overwrite_finra_downloads:
            finra_cmd.append("--overwrite-downloads")
        if args.truncate_finra_raw:
            finra_cmd.append("--truncate-raw")
        if args.verbose:
            finra_cmd.append("--verbose")
        steps.append(("run_finra_daily_refresh", finra_cmd))

    if not args.skip_sec:
        sec_cmd = [
            sys.executable,
            str(project_root / "cli" / "ops" / "run_sec_fundamentals_daily.py"),
            "--project-root", str(project_root),
            "--db-path", db_path,
        ]
        for src in args.sec_sources:
            sec_cmd.extend(["--sec-source", str(Path(src).expanduser().resolve())])
        if args.skip_sec_load:
            sec_cmd.append("--skip-load")
        if args.verbose:
            sec_cmd.append("--verbose")
        steps.append(("run_sec_fundamentals_daily", sec_cmd))

    status_cmd = [
        sys.executable,
        str(project_root / "cli" / "pipeline_status.py"),
        "--db-path", db_path,
    ]
    steps.append(("pipeline_status", status_cmd))

    print("===== DAILY PIPELINE START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {db_path}", flush=True)

    for name, cmd in tqdm(steps, desc="daily pipeline", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== DAILY PIPELINE COMPLETE =====", flush=True)


if __name__ == "__main__":
    main()
