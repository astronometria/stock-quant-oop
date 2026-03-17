#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize and probe the research pipeline end-to-end."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose child output.")
    return parser.parse_args()


def run_step(name: str, cmd: list[str], cwd: Path) -> dict:
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "step_name": name,
        "command": cmd,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()

    steps: list[tuple[str, list[str]]] = []

    python_bin = sys.executable

    for script_rel in [
        "cli/core/init_feature_engine_foundation.py",
        "cli/core/init_label_engine_foundation.py",
        "cli/core/init_dataset_builder_foundation.py",
        "cli/core/init_backtest_foundation.py",
    ]:
        cmd = [python_bin, str(project_root / script_rel), "--db-path", args.db_path]
        if args.verbose:
            cmd.append("--verbose")
        steps.append((Path(script_rel).name, cmd))

    for script_rel in [
        "cli/core/build_feature_engine.py",
        "cli/core/build_label_engine.py",
        "cli/core/build_dataset_builder.py",
    ]:
        cmd = [python_bin, str(project_root / script_rel), "--db-path", args.db_path]
        if args.verbose:
            cmd.append("--verbose")
        steps.append((Path(script_rel).name, cmd))

    report: dict[str, object] = {
        "db_path": args.db_path,
        "project_root": str(project_root),
        "steps": [],
    }

    for name, cmd in steps:
        result = run_step(name=name, cmd=cmd, cwd=project_root)
        report["steps"].append(result)

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
