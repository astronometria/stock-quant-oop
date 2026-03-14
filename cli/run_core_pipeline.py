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
        help="Truncate staging raw tables before loading.",
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


def maybe_add_truncate(cmd: list[str], truncate_raw: bool) -> list[str]:
    if truncate_raw:
        cmd.append("--truncate")
    return cmd


def run_step(name: str, cmd: list[str], project_root: Path) -> None:
    completed = subprocess.run(cmd, cwd=project_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {name} (exit={completed.returncode})")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()

    steps: list[tuple[str, list[str]]] = []

    init_cmd = build_base_command(
        project_root,
        "init_market_db.py",
        args.db_path,
        args.verbose,
    )
    if args.drop_existing:
        init_cmd.append("--drop-existing")
    steps.append(("init_market_db", init_cmd))

    if not args.skip_symbol_load:
        symbol_load_cmd = build_base_command(
            project_root,
            "load_symbol_reference_source_raw.py",
            args.db_path,
            args.verbose,
        )
        steps.append(
            (
                "load_symbol_reference_source_raw",
                maybe_add_truncate(symbol_load_cmd, args.truncate_raw),
            )
        )

    if not args.skip_universe:
        steps.append(
            (
                "build_market_universe",
                build_base_command(
                    project_root,
                    "build_market_universe.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    if not args.skip_symbol_reference:
        steps.append(
            (
                "build_symbol_reference",
                build_base_command(
                    project_root,
                    "build_symbol_reference.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    if not args.skip_price_load:
        price_load_cmd = build_base_command(
            project_root,
            "load_price_source_daily_raw.py",
            args.db_path,
            args.verbose,
        )
        steps.append(
            (
                "load_price_source_daily_raw",
                maybe_add_truncate(price_load_cmd, args.truncate_raw),
            )
        )

    if not args.skip_prices:
        steps.append(
            (
                "build_prices",
                build_base_command(
                    project_root,
                    "build_prices.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    if not args.skip_finra_load:
        finra_load_cmd = build_base_command(
            project_root,
            "load_finra_short_interest_source_raw.py",
            args.db_path,
            args.verbose,
        )
        steps.append(
            (
                "load_finra_short_interest_source_raw",
                maybe_add_truncate(finra_load_cmd, args.truncate_raw),
            )
        )

    if not args.skip_finra:
        finra_build_cmd = build_base_command(
            project_root,
            "build_finra_short_interest.py",
            args.db_path,
            args.verbose,
        )
        finra_build_cmd.extend(["--source-market", args.source_market])
        steps.append(("build_finra_short_interest", finra_build_cmd))

    if not args.skip_news_load:
        news_load_cmd = build_base_command(
            project_root,
            "load_news_source_raw.py",
            args.db_path,
            args.verbose,
        )
        steps.append(
            (
                "load_news_source_raw",
                maybe_add_truncate(news_load_cmd, args.truncate_raw),
            )
        )

    if not args.skip_news_raw:
        steps.append(
            (
                "build_news_raw",
                build_base_command(
                    project_root,
                    "build_news_raw.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    if not args.skip_news_candidates:
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

    print("===== CORE PIPELINE START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"SOURCE MARKET: {args.source_market}", flush=True)
    print(f"TRUNCATE RAW: {args.truncate_raw}", flush=True)

    for name, cmd in tqdm(steps, desc="core pipeline", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== CORE PIPELINE DONE =====", flush=True)


if __name__ == "__main__":
    main()
