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
        description="Run the core stock-quant-oop pipeline in sequence."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to each CLI step.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate database objects at init step.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market selection passed to supported steps.",
    )
    parser.add_argument(
        "--skip-symbol-load",
        action="store_true",
        help="Skip loading symbol reference raw staging data.",
    )
    parser.add_argument(
        "--skip-universe",
        action="store_true",
        help="Skip market universe build step.",
    )
    parser.add_argument(
        "--skip-symbol-reference",
        action="store_true",
        help="Skip symbol reference build step.",
    )
    parser.add_argument(
        "--skip-price-load",
        action="store_true",
        help="Skip loading price raw staging data.",
    )
    parser.add_argument(
        "--skip-prices",
        action="store_true",
        help="Skip price build step.",
    )
    parser.add_argument(
        "--skip-finra-load",
        action="store_true",
        help="Skip loading FINRA raw staging data.",
    )
    parser.add_argument(
        "--skip-finra",
        action="store_true",
        help="Skip FINRA short interest build step.",
    )
    parser.add_argument(
        "--skip-news-load",
        action="store_true",
        help="Skip loading news raw staging data.",
    )
    parser.add_argument(
        "--skip-news-raw",
        action="store_true",
        help="Skip normalized news build step.",
    )
    parser.add_argument(
        "--skip-news-candidates",
        action="store_true",
        help="Skip news symbol candidates step.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Pass --truncate to raw loader steps when supported.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def build_command(
    project_root: Path,
    relative_script: str,
    db_path: str | None,
    verbose: bool,
    extra_args: list[str] | None = None,
) -> list[str]:
    cmd = [sys.executable, str(project_root / relative_script)]
    if db_path:
        cmd.extend(["--db-path", db_path])
    if verbose:
        cmd.append("--verbose")
    if extra_args:
        cmd.extend(extra_args)
    return cmd


def script_exists(project_root: Path, relative_script: str) -> bool:
    return (project_root / relative_script).is_file()


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()

    steps: list[tuple[str, str, list[str]]] = []

    steps.append(
        (
            "init_market_db",
            "cli/init_market_db.py",
            ["--drop-existing"] if args.drop_existing else [],
        )
    )

    if not args.skip_symbol_load and script_exists(project_root, "cli/load_symbol_reference_source_raw.py"):
        symbol_load_args: list[str] = []
        if args.truncate_raw:
            symbol_load_args.append("--truncate")
        steps.append(
            (
                "load_symbol_reference_source_raw",
                "cli/load_symbol_reference_source_raw.py",
                symbol_load_args,
            )
        )

    if not args.skip_universe:
        steps.append(("build_market_universe", "cli/build_market_universe.py", []))

    if not args.skip_symbol_reference:
        steps.append(("build_symbol_reference", "cli/build_symbol_reference.py", []))

    if not args.skip_price_load:
        if script_exists(project_root, "cli/load_price_source_daily_raw_all_from_stooq_zip.py"):
            raw_all_args: list[str] = []
            if args.truncate_raw:
                raw_all_args.append("--truncate")
            steps.append(
                (
                    "load_price_source_daily_raw_all_from_stooq_zip",
                    "cli/load_price_source_daily_raw_all_from_stooq_zip.py",
                    raw_all_args,
                )
            )

        if script_exists(project_root, "cli/fetch_price_source_daily_raw_yahoo.py"):
            yahoo_args: list[str] = []
            if args.truncate_raw:
                yahoo_args.append("--truncate")
            steps.append(
                (
                    "fetch_price_source_daily_raw_yahoo",
                    "cli/fetch_price_source_daily_raw_yahoo.py",
                    yahoo_args,
                )
            )

        if script_exists(project_root, "cli/filter_price_source_daily_raw_from_all.py"):
            filter_args: list[str] = []
            if args.truncate_raw:
                filter_args.append("--truncate")
            steps.append(
                (
                    "filter_price_source_daily_raw_from_all",
                    "cli/filter_price_source_daily_raw_from_all.py",
                    filter_args,
                )
            )

    if not args.skip_prices:
        steps.append(("build_prices", "cli/build_prices.py", []))

    if not args.skip_finra_load and script_exists(project_root, "cli/load_finra_short_interest_source_raw.py"):
        finra_load_args: list[str] = []
        if args.truncate_raw:
            finra_load_args.append("--truncate")
        steps.append(
            (
                "load_finra_short_interest_source_raw",
                "cli/load_finra_short_interest_source_raw.py",
                finra_load_args,
            )
        )

    if not args.skip_finra:
        steps.append(
            (
                "build_finra_short_interest",
                "cli/build_finra_short_interest.py",
                ["--source-market", args.source_market],
            )
        )

    if not args.skip_news_load and script_exists(project_root, "cli/load_news_source_raw.py"):
        news_load_args: list[str] = []
        if args.truncate_raw:
            news_load_args.append("--truncate")
        steps.append(("load_news_source_raw", "cli/load_news_source_raw.py", news_load_args))

    if not args.skip_news_raw:
        steps.append(("build_news_raw", "cli/build_news_raw.py", []))

    if not args.skip_news_candidates:
        steps.append(("build_news_symbol_candidates", "cli/build_news_symbol_candidates.py", []))

    if not steps:
        print("No pipeline steps selected.")
        return 0

    for step_name, relative_script, extra_args in tqdm(steps, desc="core-pipeline", unit="step"):
        cmd = build_command(
            project_root=project_root,
            relative_script=relative_script,
            db_path=args.db_path,
            verbose=args.verbose,
            extra_args=extra_args,
        )
        print(f"\n===== RUN {step_name} =====")
        print(" ".join(cmd))
        completed = subprocess.run(cmd, cwd=project_root)
        if completed.returncode != 0:
            print(f"\nFAILED at step: {step_name} (exit={completed.returncode})")
            return completed.returncode

    print("\n===== CORE PIPELINE DONE =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
