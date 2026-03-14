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
        description="Run the daily price refresh pipeline: Yahoo fetch -> effective rebuild -> incremental build -> status."
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
        "--max-symbols",
        type=int,
        default=None,
        help="Optional limit for Yahoo fetch, useful for testing.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="Sleep between Yahoo requests.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Yahoo fetch retries per symbol.",
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

    fetch_cmd = build_base_command(
        project_root,
        "fetch_price_source_daily_raw_yahoo.py",
        args.db_path,
        args.verbose,
    )
    if args.max_symbols is not None:
        fetch_cmd.extend(["--max-symbols", str(args.max_symbols)])
    fetch_cmd.extend(["--sleep", str(args.sleep), "--retries", str(args.retries)])
    steps.append(("fetch_price_source_daily_raw_yahoo", fetch_cmd))

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

    print("===== RUN PRICE DAILY REFRESH START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)

    for name, cmd in tqdm(steps, desc="price daily refresh", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN PRICE DAILY REFRESH DONE =====", flush=True)


if __name__ == "__main__":
    main()
