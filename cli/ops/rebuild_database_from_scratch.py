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


# ---------------------------------------------------------------------
# ARGUMENT PARSER
# ---------------------------------------------------------------------
def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Rebuild the entire DuckDB database from scratch."
    )

    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )

    parser.add_argument(
        "--db-path",
        default="market.duckdb",
        help="DuckDB database path.",
    )

    parser.add_argument(
        "--skip-sec-fetch",
        action="store_true",
        help="Skip downloading SEC symbol sources.",
    )

    parser.add_argument(
        "--skip-nasdaq-fetch",
        action="store_true",
        help="Skip downloading NASDAQ symbol directory.",
    )

    parser.add_argument(
        "--skip-fundamentals",
        action="store_true",
        help="Skip fundamentals build.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------
def build_cmd(project_root: Path, script: str, db_path: str, verbose: bool) -> list[str]:

    cmd = [sys.executable, str(project_root / script)]

    if db_path:
        cmd.extend(["--db-path", db_path])

    if verbose:
        cmd.append("--verbose")

    return cmd


def run_step(name: str, cmd: list[str], project_root: Path):

    print(f"\n===== STEP: {name} =====", flush=True)
    print("COMMAND:", " ".join(cmd), flush=True)

    completed = subprocess.run(cmd, cwd=project_root)

    if completed.returncode != 0:
        raise SystemExit(f"step failed: {name} (exit_code={completed.returncode})")

    print(f"===== STEP OK: {name} =====", flush=True)


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():

    args = parse_args()

    project_root = Path(args.project_root).resolve()

    print("===== REBUILD DATABASE FROM SCRATCH =====")
    print(f"project_root={project_root}")
    print(f"db_path={args.db_path}")

    steps: list[tuple[str, list[str]]] = []

    # -----------------------------------------------------------------
    # INIT DB
    # -----------------------------------------------------------------
    steps.append(
        (
            "INIT MARKET DB",
            build_cmd(project_root, "cli/core/init_market_db.py", args.db_path, args.verbose),
        )
    )

    # -----------------------------------------------------------------
    # SYMBOL SOURCES
    # -----------------------------------------------------------------
    if not args.skip_sec_fetch:
        steps.append(
            (
                "FETCH SEC COMPANY TICKERS RAW",
                build_cmd(
                    project_root,
                    "cli/raw/fetch_sec_company_tickers_raw.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    if not args.skip_nasdaq_fetch:
        steps.append(
            (
                "FETCH NASDAQ SYMBOL DIRECTORY RAW",
                build_cmd(
                    project_root,
                    "cli/raw/fetch_nasdaq_symbol_directory_raw.py",
                    args.db_path,
                    args.verbose,
                ),
            )
        )

    # -----------------------------------------------------------------
    # LOAD SYMBOL SOURCES
    # -----------------------------------------------------------------
    steps.append(
        (
            "LOAD SYMBOL_REFERENCE_SOURCE_RAW",
            build_cmd(
                project_root,
                "cli/raw/load_symbol_reference_source_raw.py",
                args.db_path,
                args.verbose,
            ),
        )
    )

    # -----------------------------------------------------------------
    # BUILD MARKET UNIVERSE
    # -----------------------------------------------------------------
    steps.append(
        (
            "BUILD MARKET UNIVERSE",
            build_cmd(project_root, "cli/core/build_market_universe.py", args.db_path, args.verbose),
        )
    )

    # -----------------------------------------------------------------
    # BUILD SYMBOL REFERENCE
    # -----------------------------------------------------------------
    steps.append(
        (
            "BUILD SYMBOL REFERENCE",
            build_cmd(project_root, "cli/core/build_symbol_reference.py", args.db_path, args.verbose),
        )
    )

    # -----------------------------------------------------------------
    # SEC FILINGS
    # -----------------------------------------------------------------
    steps.append(
        (
            "BUILD SEC FILINGS",
            build_cmd(project_root, "cli/core/build_sec_filings.py", args.db_path, args.verbose),
        )
    )

    # -----------------------------------------------------------------
    # SEC FACT NORMALIZATION
    # -----------------------------------------------------------------
    steps.append(
        (
            "BUILD SEC FACT NORMALIZED",
            build_cmd(project_root, "cli/core/build_sec_fact_normalized.py", args.db_path, args.verbose),
        )
    )

    # -----------------------------------------------------------------
    # FUNDAMENTALS
    # -----------------------------------------------------------------
    if not args.skip_fundamentals:
        steps.append(
            (
                "BUILD FUNDAMENTALS",
                build_cmd(project_root, "cli/core/build_fundamentals.py", args.db_path, args.verbose),
            )
        )

    # -----------------------------------------------------------------
    # EXECUTION
    # -----------------------------------------------------------------
    for name, cmd in tqdm(steps, desc="rebuild", unit="step", dynamic_ncols=True):
        run_step(name, cmd, project_root)

    print("\n===== REBUILD DATABASE COMPLETE =====")
    print(f"db_path={args.db_path}")


if __name__ == "__main__":
    main()
