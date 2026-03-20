from __future__ import annotations

"""
Legacy shim for the removed cross-sectional backtest entrypoint.

Why this file still exists
--------------------------
The old backtest path was removed because it depended on the legacy
BuildBacktestPipeline / SignalBacktester flow.

This shim is intentionally kept for a short transition window so that:
- old shell scripts fail with a clear explanation
- developers are redirected to the research-grade backtest CLI
- we avoid a confusing ImportError caused by a deleted module

Research-grade note
-------------------
The active research path in this repository is now the explicit
research snapshot / split / training dataset / labels / research backtest flow.
This legacy entrypoint must not be used for new work.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Legacy backtest shim. Redirects users to the research-grade backtest CLI."
    )
    parser.add_argument("--db-path", required=False, help="Path to DuckDB database.")
    parser.add_argument("--snapshot-id", required=False, help="Research snapshot identifier.")
    parser.add_argument("--split-id", required=False, help="Research split identifier.")
    parser.add_argument("--experiment-name", required=False, help="Experiment name.")
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print the redirected command before execution.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args, unknown = parser.parse_known_args()

    repo_root = Path(__file__).resolve().parents[2]
    target = repo_root / "cli" / "core" / "build_research_backtest.py"

    if not target.exists():
        print(
            "[build_backtest] ERROR: legacy backtest entrypoint was removed, "
            "and cli/core/build_research_backtest.py was not found.",
            file=sys.stderr,
            flush=True,
        )
        return 2

    redirected_cmd = [sys.executable, str(target)]

    # Forward only arguments that make sense for the modern research flow.
    if args.db_path:
        redirected_cmd.extend(["--db-path", args.db_path])
    if args.snapshot_id:
        redirected_cmd.extend(["--snapshot-id", args.snapshot_id])
    if args.split_id:
        redirected_cmd.extend(["--split-id", args.split_id])
    if args.experiment_name:
        redirected_cmd.extend(["--experiment-name", args.experiment_name])

    # Preserve unknown flags so the modern CLI can validate them.
    redirected_cmd.extend(unknown)

    print(
        "[build_backtest] INFO: legacy cross-sectional backtest path was removed. "
        "Redirecting to cli/core/build_research_backtest.py",
        flush=True,
    )

    if args.verbose:
        print(f"[build_backtest] REDIRECT CMD: {' '.join(redirected_cmd)}", flush=True)

    completed = subprocess.run(redirected_cmd, check=False)
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
