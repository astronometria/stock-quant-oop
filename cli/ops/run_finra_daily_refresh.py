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
        description="Run FINRA daily refresh: download recent files -> load raw -> build normalized -> status."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to child steps.",
    )
    parser.add_argument(
        "--output-dir",
        default="~/stock-quant-oop/data/raw/finra_short_interest",
        help="Directory where FINRA files are stored/downloaded.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive start date YYYY-MM-DD for FINRA download/load window.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date YYYY-MM-DD for FINRA download/load window.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market passed to build_finra_short_interest.",
    )
    parser.add_argument(
        "--overwrite-downloads",
        action="store_true",
        help="Overwrite existing local FINRA download files.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Delete existing finra_short_interest_source_raw before load.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def build_base_command(project_root: Path, script_rel: str, db_path: str | None, verbose: bool) -> list[str]:
    cmd = [sys.executable, str(project_root / script_rel)]
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
    output_dir = str(Path(args.output_dir).expanduser().resolve())

    steps: list[tuple[str, list[str]]] = []

    download_cmd = [
        sys.executable,
        str(project_root / "cli" / "core" / "download_finra_short_interest.py"),
        "--output-dir", output_dir,
    ]
    if args.start_date:
        download_cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        download_cmd.extend(["--end-date", args.end_date])
    if args.overwrite_downloads:
        download_cmd.append("--overwrite")
    if args.verbose:
        download_cmd.append("--verbose")
    steps.append(("download_finra_short_interest", download_cmd))

    load_cmd = build_base_command(
        project_root,
        "cli/raw/load_finra_short_interest_source_raw.py",
        args.db_path,
        args.verbose,
    )
    load_cmd.extend(["--source", output_dir])
    if args.start_date:
        load_cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        load_cmd.extend(["--end-date", args.end_date])
    if args.truncate_raw:
        load_cmd.append("--truncate")
    steps.append(("load_finra_short_interest_source_raw", load_cmd))

    build_cmd = build_base_command(
        project_root,
        "cli/core/build_finra_short_interest.py",
        args.db_path,
        args.verbose,
    )
    build_cmd.extend(["--source-market", args.source_market])
    steps.append(("build_finra_short_interest", build_cmd))

    status_cmd = build_base_command(
        project_root,
        "cli/pipeline_status.py",
        args.db_path,
        False,
    )
    steps.append(("pipeline_status", status_cmd))

    print("===== RUN FINRA DAILY REFRESH START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"OUTPUT DIR: {output_dir}", flush=True)

    for name, cmd in tqdm(steps, desc="finra daily refresh", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN FINRA DAILY REFRESH DONE =====", flush=True)


if __name__ == "__main__":
    main()
