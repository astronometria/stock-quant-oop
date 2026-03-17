#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FINRA daily refresh stack.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--short-interest-source",
        action="append",
        dest="short_interest_sources",
        default=[],
        help="Optional short-interest source path. Repeat as needed.",
    )
    parser.add_argument(
        "--daily-short-volume-source",
        action="append",
        dest="daily_short_volume_sources",
        default=[],
        help="Optional daily short volume source path. Repeat as needed.",
    )
    return parser.parse_args()


def _run(cmd: list[str]) -> int:
    print(" ".join(cmd), flush=True)
    return subprocess.run(cmd, check=False).returncode


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[2]
    python_bin = "python3"
    db_path = str(Path(args.db_path).expanduser().resolve())

    rc = 0

    if args.daily_short_volume_sources:
        cmd = [python_bin, str(repo_root / "cli/raw/load_finra_daily_short_volume_raw.py"), "--db-path", db_path]
        for item in args.daily_short_volume_sources:
            cmd.extend(["--source", item])
        rc = _run(cmd)
        if rc != 0:
            return rc

    rc = _run([python_bin, str(repo_root / "cli/core/build_finra_daily_short_volume.py"), "--db-path", db_path])
    if rc != 0:
        return rc

    rc = _run([python_bin, str(repo_root / "cli/core/build_finra_short_interest.py"), "--db-path", db_path])
    if rc != 0:
        return rc

    rc = _run([python_bin, str(repo_root / "cli/core/build_short_features.py"), "--db-path", db_path])
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
