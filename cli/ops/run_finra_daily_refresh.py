#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--project-root", default=".")

    parser.add_argument("--db-path", default=None)

    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args()


def main():

    args = parse_args()

    root = Path(args.project_root).resolve()

    cmd = [
        sys.executable,
        str(root / "cli/core/build_finra_short_interest.py"),
    ]

    if args.db_path:
        cmd += ["--db-path", args.db_path]

    if args.verbose:
        cmd += ["--verbose"]

    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
