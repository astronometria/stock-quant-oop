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
        description="Run manual news refresh: load raw CSV -> build news_articles_raw -> build news_symbol_candidates -> status."
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
        "--news-csv",
        required=True,
        help="Path to manual news CSV input.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Delete existing news_source_raw rows before load.",
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
        "load_news_source_raw_csv.py",
        args.db_path,
        args.verbose,
    )
    load_cmd.extend(["--csv-path", str(Path(args.news_csv).expanduser().resolve())])
    if args.truncate_raw:
        load_cmd.append("--truncate")
    steps.append(("load_news_source_raw_csv", load_cmd))

    build_raw_cmd = build_base_command(
        project_root,
        "build_news_raw.py",
        args.db_path,
        args.verbose,
    )
    steps.append(("build_news_raw", build_raw_cmd))

    build_candidates_cmd = build_base_command(
        project_root,
        "build_news_symbol_candidates.py",
        args.db_path,
        args.verbose,
    )
    steps.append(("build_news_symbol_candidates", build_candidates_cmd))

    status_cmd = build_base_command(
        project_root,
        "pipeline_status.py",
        args.db_path,
        False,
    )
    steps.append(("pipeline_status", status_cmd))

    print("===== RUN NEWS DAILY REFRESH START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"NEWS CSV: {Path(args.news_csv).expanduser().resolve()}", flush=True)

    for name, cmd in tqdm(steps, desc="news daily refresh", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN NEWS DAILY REFRESH DONE =====", flush=True)


if __name__ == "__main__":
    main()
