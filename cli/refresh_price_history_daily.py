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
        description="Refresh effective price history using Yahoo daily raw input."
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
        "--yahoo-csv",
        required=True,
        help="Yahoo daily CSV input path.",
    )
    parser.add_argument(
        "--truncate-yahoo",
        action="store_true",
        help="Delete existing Yahoo raw rows before loading.",
    )
    parser.add_argument(
        "--truncate-effective",
        action="store_true",
        help="Delete existing effective staging rows before rebuilding.",
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

    load_cmd = build_base_command(
        project_root,
        "load_price_source_daily_raw_yahoo_csv.py",
        args.db_path,
        args.verbose,
    )
    load_cmd.extend(["--csv-path", str(Path(args.yahoo_csv).expanduser().resolve())])
    if args.truncate_yahoo:
        load_cmd.append("--truncate")
    steps.append(("load_price_source_daily_raw_yahoo_csv", load_cmd))

    rebuild_cmd = build_base_command(
        project_root,
        "rebuild_price_source_daily_raw_effective.py",
        args.db_path,
        args.verbose,
    )
    if args.truncate_effective:
        rebuild_cmd.append("--truncate")
    steps.append(("rebuild_price_source_daily_raw_effective", rebuild_cmd))

    build_cmd = build_base_command(
        project_root,
        "build_prices.py",
        args.db_path,
        args.verbose,
    )
    steps.append(("build_prices", build_cmd))

    status_cmd = build_base_command(
        project_root,
        "pipeline_status.py",
        args.db_path,
        False,
    )
    steps.append(("pipeline_status", status_cmd))

    print("===== REFRESH PRICE HISTORY DAILY START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"YAHOO CSV: {Path(args.yahoo_csv).expanduser().resolve()}", flush=True)

    for name, cmd in tqdm(steps, desc="refresh price daily", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== REFRESH PRICE HISTORY DAILY DONE =====", flush=True)


if __name__ == "__main__":
    main()
