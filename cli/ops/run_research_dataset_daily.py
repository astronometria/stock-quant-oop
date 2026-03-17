#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **k):
        return x


def parse_args():

    parser = argparse.ArgumentParser(
        description="Run full research dataset build."
    )

    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
    )

    parser.add_argument(
        "--db-path",
        default=None,
    )

    parser.add_argument(
        "--dataset-version",
        default="daily",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
    )

    return parser.parse_args()


def build_cmd(project_root, script, db_path, verbose):

    cmd = [sys.executable, str(project_root / script)]

    if db_path:
        cmd += ["--db-path", db_path]

    if verbose:
        cmd.append("--verbose")

    return cmd


def run_step(name, cmd, cwd):

    proc = subprocess.run(cmd, cwd=cwd)

    if proc.returncode != 0:
        raise SystemExit(f"step failed: {name}")


def main():

    args = parse_args()

    project_root = Path(args.project_root).resolve()

    steps = []

    steps.append((
        "build_prices",
        build_cmd(project_root, "cli/core/build_prices.py", args.db_path, args.verbose),
    ))

    steps.append((
        "build_short_interest",
        build_cmd(project_root, "cli/core/build_finra_short_interest.py", args.db_path, args.verbose),
    ))

    steps.append((
        "build_fundamentals",
        build_cmd(project_root, "cli/core/build_fundamentals.py", args.db_path, args.verbose),
    ))

    steps.append((
        "build_training_dataset",
        build_cmd(project_root, "cli/core/build_training_dataset.py", args.db_path, args.verbose),
    ))

    dataset_cmd = build_cmd(project_root, "cli/core/build_dataset_version.py", args.db_path, args.verbose)

    dataset_cmd += [
        "--dataset-name",
        "training_dataset_daily",
        "--dataset-version",
        args.dataset_version,
    ]

    steps.append(("build_dataset_version", dataset_cmd))

    print("===== RUN RESEARCH DATASET DAILY =====")
    print("PROJECT ROOT:", project_root)
    print("DB PATH:", args.db_path)

    for name, cmd in tqdm(steps, desc="research pipeline", unit="step"):

        print("\n===== RUN STEP:", name, "=====")
        print("COMMAND:", " ".join(cmd))

        run_step(name, cmd, project_root)

        print("===== STEP OK:", name, "=====")

    print("\n===== RESEARCH DATASET COMPLETE =====")


if __name__ == "__main__":
    main()
