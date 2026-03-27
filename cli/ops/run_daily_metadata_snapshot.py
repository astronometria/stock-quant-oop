#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_daily_metadata_snapshot.py
#
# Objectif:
# - accumuler chaque jour les snapshots metadata réellement observés
# - ne jamais écraser l'historique de snapshots
# - reconstruire ensuite la couche history/PIT à partir de ces snapshots
#
# Ce script:
# 1) fetch les sources symboles courantes
# 2) charge symbol_reference_source_raw avec une vraie as_of_date
# 3) rebuild market_universe / symbol_reference
# 4) rebuild listing_status_history / market_universe_history /
#    symbol_reference_history / universe_membership_history
# 5) tente la whitelist PIT
#
# Important:
# - ce script améliore la profondeur PIT avec le temps
# - il ne prétend pas recréer gratuitement 20 ans d'univers historique instantanément
# - il prépare la base pour une vraie reconstruction scientifique future
# =============================================================================

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from tqdm import tqdm


@dataclass
class StepResult:
    step_name: str
    command: list[str]
    started_at: str
    finished_at: str
    duration_seconds: float
    returncode: int


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_cmd(step_name: str, cmd: list[str]) -> StepResult:
    started = datetime.now(timezone.utc)
    print(f"===== STEP: {step_name} =====", flush=True)
    print("COMMAND:", " ".join(cmd), flush=True)

    completed = subprocess.run(cmd)

    finished = datetime.now(timezone.utc)
    result = StepResult(
        step_name=step_name,
        command=cmd,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_seconds=(finished - started).total_seconds(),
        returncode=completed.returncode,
    )

    if completed.returncode != 0:
        raise SystemExit(completed.returncode)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Accumulate daily metadata snapshots and rebuild history/PIT layers."
    )
    parser.add_argument("--project-root", default="~/stock-quant-oop")
    parser.add_argument("--db-path", default="~/stock-quant-oop-runtime/db/market.duckdb")
    parser.add_argument("--snapshot-date", default=None, help="YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--allow-adr", action="store_true")
    parser.add_argument("--build-pit-whitelist", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def discover_symbol_source_files(project_root: Path) -> list[Path]:
    """
    Recherche les fichiers de sources symboles générés par les fetchers.
    """
    symbol_root = project_root / "data" / "symbol_sources"
    if not symbol_root.exists():
        return []

    files: list[Path] = []
    for pattern in ("**/*.csv", "**/*.tsv"):
        files.extend(symbol_root.glob(pattern))
    return sorted([p.resolve() for p in files if p.is_file()])


def main() -> int:
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    python_bin = str(Path(args.python_bin).expanduser().resolve())

    snapshot_date = args.snapshot_date or date.today().isoformat()

    steps: list[StepResult] = []
    progress = tqdm(total=10, desc="daily_metadata_snapshot", unit="step")

    # -------------------------------------------------------------------------
    # 1) Fetch des sources symboles du jour
    # -------------------------------------------------------------------------
    steps.append(
        run_cmd(
            "FETCH SEC COMPANY TICKERS RAW",
            [
                python_bin,
                str(project_root / "cli/raw/fetch_sec_company_tickers_raw.py"),
            ],
        )
    )
    progress.update(1)

    steps.append(
        run_cmd(
            "FETCH NASDAQ SYMBOL DIRECTORY RAW",
            [
                python_bin,
                str(project_root / "cli/raw/fetch_nasdaq_symbol_directory_raw.py"),
            ],
        )
    )
    progress.update(1)

    # -------------------------------------------------------------------------
    # 2) Load strict des snapshots du jour
    # -------------------------------------------------------------------------
    source_files = discover_symbol_source_files(project_root)
    if not source_files:
        raise SystemExit(f"No symbol source files found under {project_root / 'data' / 'symbol_sources'}")

    for idx, source_file in enumerate(source_files, start=1):
        steps.append(
            run_cmd(
                f"LOAD SYMBOL SOURCE SNAPSHOT [{idx}/{len(source_files)}]",
                [
                    python_bin,
                    str(project_root / "cli/raw/load_symbol_reference_source_raw.py"),
                    "--db-path",
                    str(db_path),
                    "--source",
                    str(source_file),
                    "--as-of-date",
                    snapshot_date,
                    "--verbose",
                ],
            )
        )
    progress.update(1)

    # -------------------------------------------------------------------------
    # 3) Rebuild snapshot courant de l'univers
    # -------------------------------------------------------------------------
    build_market_universe_cmd = [
        python_bin,
        str(project_root / "cli/core/build_market_universe.py"),
        "--db-path",
        str(db_path),
    ]
    if args.allow_adr:
        build_market_universe_cmd.append("--allow-adr")

    steps.append(run_cmd("BUILD MARKET UNIVERSE", build_market_universe_cmd))
    progress.update(1)

    steps.append(
        run_cmd(
            "BUILD SYMBOL REFERENCE",
            [
                python_bin,
                str(project_root / "cli/core/build_symbol_reference.py"),
                "--db-path",
                str(db_path),
            ],
        )
    )
    progress.update(1)

    # -------------------------------------------------------------------------
    # 4) Rebuild history réelle à partir des snapshots accumulés
    # -------------------------------------------------------------------------
    steps.append(
        run_cmd(
            "BUILD LISTING HISTORY",
            [
                python_bin,
                str(project_root / "cli/core/build_listing_history.py"),
                "--db-path",
                str(db_path),
            ],
        )
    )
    progress.update(1)

    steps.append(
        run_cmd(
            "BUILD MARKET UNIVERSE HISTORY",
            [
                python_bin,
                str(project_root / "cli/core/build_market_universe_history.py"),
                "--db-path",
                str(db_path),
            ],
        )
    )
    progress.update(1)

    steps.append(
        run_cmd(
            "BUILD SYMBOL REFERENCE HISTORY",
            [
                python_bin,
                str(project_root / "cli/core/build_symbol_reference_history.py"),
                "--db-path",
                str(db_path),
            ],
        )
    )
    progress.update(1)

    steps.append(
        run_cmd(
            "BUILD UNIVERSE MEMBERSHIP HISTORY",
            [
                python_bin,
                str(project_root / "cli/core/build_universe_membership_history_from_market_universe_history.py"),
                "--db-path",
                str(db_path),
            ],
        )
    )
    progress.update(1)

    # -------------------------------------------------------------------------
    # 5) Whitelist PIT optionnelle
    # -------------------------------------------------------------------------
    if args.build_pit_whitelist:
        steps.append(
            run_cmd(
                "BUILD RESEARCH UNIVERSE WHITELIST 20D PIT",
                [
                    python_bin,
                    str(project_root / "cli/core/build_research_universe_whitelist_20d_pit.py"),
                    "--db-path",
                    str(db_path),
                ],
            )
        )
    progress.update(1)

    progress.close()

    summary = {
        "status": "SUCCESS",
        "snapshot_date": snapshot_date,
        "db_path": str(db_path),
        "steps": [asdict(step) for step in steps],
        "notes": [
            "This script accumulates metadata snapshots over time.",
            "Scientific PIT depth improves only as distinct snapshot dates accumulate.",
            "No fake PIT history is created here.",
        ],
    }
    print("===== DAILY METADATA SNAPSHOT COMPLETE =====", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
