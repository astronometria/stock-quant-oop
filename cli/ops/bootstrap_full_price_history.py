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
        description="Bootstrap full historical price pipeline from extracted Stooq directory."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to child steps.",
    )
    parser.add_argument(
        "--stooq-root-dir",
        default="~/stock-quant/data/extracted/stooq/data/daily/us",
        help="Extracted Stooq root directory.",
    )
    parser.add_argument(
        "--normalized-csv",
        default="~/stock-quant/data/normalized/stooq_us_daily_normalized_full.csv",
        help="Normalized CSV output path.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate database objects before bootstrap.",
    )
    parser.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip CSV export step and reuse existing normalized CSV.",
    )
    parser.add_argument(
        "--skip-bronze-load",
        action="store_true",
        help="Skip bronze CSV load step.",
    )
    parser.add_argument(
        "--skip-filter",
        action="store_true",
        help="Skip bronze to staging filter step.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Skip build_prices step.",
    )
    parser.add_argument(
        "--exclude-etfs",
        action="store_true",
        help="Exclude ETF files during export.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Optional max number of txt files to export. 0 means no limit.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=0,
        help="Optional max number of rows to export. 0 means no limit.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def build_base_command(project_root: Path, script_name: str, db_path: str | None, verbose: bool) -> list[str]:
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

    seed_cmd = build_base_command(project_root, "seed_initial_db.py", args.db_path, args.verbose)
    if args.drop_existing:
        seed_cmd.append("--drop-existing")
    steps.append(("seed_initial_db", seed_cmd))

    if not args.skip_export:
        export_cmd = [sys.executable, str(project_root / "cli" / "export_stooq_dir_to_normalized_csv.py")]
        export_cmd.extend(["--root-dir", str(Path(args.stooq_root_dir).expanduser().resolve())])
        export_cmd.extend(["--output-csv", str(Path(args.normalized_csv).expanduser().resolve())])
        if args.exclude_etfs:
            export_cmd.append("--exclude-etfs")
        if args.limit_files > 0:
            export_cmd.extend(["--limit-files", str(args.limit_files)])
        if args.limit_rows > 0:
            export_cmd.extend(["--limit-rows", str(args.limit_rows)])
        if args.verbose:
            export_cmd.append("--verbose")
        steps.append(("export_stooq_dir_to_normalized_csv", export_cmd))

    if not args.skip_bronze_load:
        bronze_cmd = build_base_command(project_root, "load_price_source_daily_raw_all_from_csv.py", args.db_path, args.verbose)
        bronze_cmd.extend(["--csv-path", str(Path(args.normalized_csv).expanduser().resolve())])
        bronze_cmd.append("--truncate")
        steps.append(("load_price_source_daily_raw_all_from_csv", bronze_cmd))

    if not args.skip_filter:
        filter_cmd = build_base_command(project_root, "filter_price_source_daily_raw_from_all.py", args.db_path, args.verbose)
        filter_cmd.append("--truncate")
        steps.append(("filter_price_source_daily_raw_from_all", filter_cmd))

    if not args.skip_build:
        build_cmd = build_base_command(project_root, "build_prices.py", args.db_path, args.verbose)
        steps.append(("build_prices", build_cmd))

    status_cmd = build_base_command(project_root, "pipeline_status.py", args.db_path, False)
    steps.append(("pipeline_status", status_cmd))

    print("===== BOOTSTRAP FULL PRICE HISTORY START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"STOOQ ROOT DIR: {Path(args.stooq_root_dir).expanduser().resolve()}", flush=True)
    print(f"NORMALIZED CSV: {Path(args.normalized_csv).expanduser().resolve()}", flush=True)

    for name, cmd in tqdm(steps, desc="bootstrap price history", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== BOOTSTRAP FULL PRICE HISTORY DONE =====", flush=True)


if __name__ == "__main__":
    main()
