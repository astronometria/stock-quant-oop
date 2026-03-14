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
        description="Seed the development database with fixture raw data and derived tables."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
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
        help="Drop and recreate database objects before seeding.",
    )
    parser.add_argument(
        "--include-prices",
        action="store_true",
        help="Load fixture price raw data and build price tables.",
    )
    parser.add_argument(
        "--include-finra",
        action="store_true",
        help="Load fixture FINRA raw data and build FINRA tables.",
    )
    parser.add_argument(
        "--include-news",
        action="store_true",
        help="Load fixture news raw data and build news tables.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market selection for FINRA build step.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def build_base_command(
    project_root: Path,
    script_name: str,
    db_path: str | None,
    verbose: bool,
) -> list[str]:
    cmd = [sys.executable, str(project_root / "cli" / script_name)]
    if db_path:
        cmd.extend(["--db-path", db_path])
    if verbose:
        cmd.append("--verbose")
    return cmd


def run_step(name: str, cmd: list[str], project_root: Path) -> None:
    completed = subprocess.run(cmd, cwd=project_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {name} (exit={completed.returncode})")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()

    steps: list[tuple[str, list[str]]] = []

    init_cmd = build_base_command(project_root, "init_market_db.py", args.db_path, args.verbose)
    if args.drop_existing:
        init_cmd.append("--drop-existing")
    steps.append(("init_market_db", init_cmd))

    symbol_load_cmd = build_base_command(
        project_root,
        "load_symbol_reference_source_raw.py",
        args.db_path,
        args.verbose,
    )
    symbol_load_cmd.append("--truncate")
    steps.append(("load_symbol_reference_source_raw", symbol_load_cmd))

    steps.append(
        (
            "build_market_universe",
            build_base_command(project_root, "build_market_universe.py", args.db_path, args.verbose),
        )
    )
    steps.append(
        (
            "build_symbol_reference",
            build_base_command(project_root, "build_symbol_reference.py", args.db_path, args.verbose),
        )
    )

    if args.include_prices:
        price_load_cmd = build_base_command(
            project_root,
            "load_price_source_daily_raw.py",
            args.db_path,
            args.verbose,
        )
        price_load_cmd.append("--truncate")
        steps.append(("load_price_source_daily_raw", price_load_cmd))
        steps.append(
            (
                "build_prices",
                build_base_command(project_root, "build_prices.py", args.db_path, args.verbose),
            )
        )

    if args.include_finra:
        finra_load_cmd = build_base_command(
            project_root,
            "load_finra_short_interest_source_raw.py",
            args.db_path,
            args.verbose,
        )
        finra_load_cmd.append("--truncate")
        steps.append(("load_finra_short_interest_source_raw", finra_load_cmd))

        finra_build_cmd = build_base_command(
            project_root,
            "build_finra_short_interest.py",
            args.db_path,
            args.verbose,
        )
        finra_build_cmd.extend(["--source-market", args.source_market])
        steps.append(("build_finra_short_interest", finra_build_cmd))

    if args.include_news:
        news_load_cmd = build_base_command(
            project_root,
            "load_news_source_raw.py",
            args.db_path,
            args.verbose,
        )
        news_load_cmd.append("--truncate")
        steps.append(("load_news_source_raw", news_load_cmd))
        steps.append(
            (
                "build_news_raw",
                build_base_command(project_root, "build_news_raw.py", args.db_path, args.verbose),
            )
        )
        steps.append(
            (
                "build_news_symbol_candidates",
                build_base_command(
                    project_root,
                    "build_news_symbol_candidates.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    print("===== SEED INITIAL DB START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(
        f"INCLUDE: prices={args.include_prices} finra={args.include_finra} news={args.include_news}",
        flush=True,
    )

    for name, cmd in tqdm(steps, desc="seed steps", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== SEED INITIAL DB DONE =====", flush=True)


if __name__ == "__main__":
    main()
