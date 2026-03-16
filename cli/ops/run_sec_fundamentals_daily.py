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
        description="Run SEC + fundamentals daily flow using staged SEC raw filing index files."
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
        "--sec-source",
        action="append",
        dest="sec_sources",
        default=[],
        help="SEC raw index CSV source path. Repeat this flag for multiple files.",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip raw SEC index load step and rebuild from already-loaded SEC raw tables.",
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

    steps: list[tuple[str, list[str]]] = []

    if not args.skip_load:
        load_cmd = build_base_command(
            project_root,
            "cli/raw/load_sec_filing_raw_index.py",
            args.db_path,
            args.verbose,
        )
        for src in args.sec_sources:
            load_cmd.extend(["--source", str(Path(src).expanduser().resolve())])
        steps.append(("load_sec_filing_raw_index", load_cmd))

    build_sec_cmd = build_base_command(
        project_root,
        "cli/core/build_sec_filings.py",
        args.db_path,
        args.verbose,
    )
    steps.append(("build_sec_filings", build_sec_cmd))

    build_fund_cmd = build_base_command(
        project_root,
        "cli/core/build_fundamentals.py",
        args.db_path,
        args.verbose,
    )
    steps.append(("build_fundamentals", build_fund_cmd))

    status_cmd = build_base_command(
        project_root,
        "cli/pipeline_status.py",
        args.db_path,
        False,
    )
    steps.append(("pipeline_status", status_cmd))

    print("===== RUN SEC + FUNDAMENTALS DAILY START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"SEC SOURCES: {len(args.sec_sources)}", flush=True)
    print(f"SKIP LOAD: {args.skip_load}", flush=True)

    for name, cmd in tqdm(steps, desc="sec fundamentals daily", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN SEC + FUNDAMENTALS DAILY DONE =====", flush=True)


if __name__ == "__main__":
    main()
