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
        description="Run daily price refresh using the canonical build_prices CLI."
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
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter. Repeat for multiple symbols.",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="Optional single as-of date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def _build_base_command(
    project_root: Path,
    script_rel: str,
    db_path: str | None,
    verbose: bool,
) -> list[str]:
    cmd = [sys.executable, str(project_root / script_rel)]
    if db_path:
        cmd.extend(["--db-path", db_path])
    if verbose:
        cmd.append("--verbose")
    return cmd


def _run_step(name: str, cmd: list[str], project_root: Path) -> None:
    completed = subprocess.run(cmd, cwd=project_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {name} (exit={completed.returncode})")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()

    build_cmd = _build_base_command(
        project_root,
        "cli/core/build_prices.py",
        args.db_path,
        args.verbose,
    )
    build_cmd.extend(["--mode", "daily"])

    for symbol in args.symbols:
        build_cmd.extend(["--symbol", str(symbol)])

    if args.as_of:
        build_cmd.extend(["--as-of", str(args.as_of)])

    status_cmd = _build_base_command(
        project_root,
        "cli/pipeline_status.py",
        args.db_path,
        False,
    )

    steps: list[tuple[str, list[str]]] = [
        ("build_prices_daily", build_cmd),
        ("pipeline_status", status_cmd),
    ]

    print("===== RUN PRICE DAILY REFRESH START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"SYMBOLS: {len(args.symbols)}", flush=True)
    print("NORMALIZED TABLE: price_history", flush=True)
    print("DERIVED TABLE: price_latest (serving only)", flush=True)

    for name, cmd in tqdm(steps, desc="price daily refresh", unit="step", dynamic_ncols=True):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        _run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN PRICE DAILY REFRESH DONE =====", flush=True)


if __name__ == "__main__":
    main()
