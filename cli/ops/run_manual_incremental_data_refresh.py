#!/usr/bin/env python3
from __future__ import annotations

"""
Manual orchestrator for incremental market-data refresh.

Objectif
--------
Lancer manuellement, dans un ordre stable et visible:
- price incremental
- finra incremental
- sec incremental
- final research_features_daily refresh

Principes
---------
- Python mince pour l'orchestration subprocess
- chaque sous-script garde sa propre logique SQL-first
- sortie visible en temps réel + log fichier
- arrêt immédiat si une étape échoue
- très commenté volontairement pour aider les autres développeurs

Notes d'exploitation
--------------------
- ce script n'essaie pas d'inventer la logique métier des sous-jobs
- il appelle simplement les entrypoints locaux validés dans le repo
- les steps FINRA raw sont activés par défaut mais peuvent être désactivés
- le step SEC raw n'est pas imposé ici, car tes tables raw SEC sont déjà présentes
"""

import argparse
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


# -----------------------------------------------------------------------------
# Modèle simple d'une étape.
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class Step:
    name: str
    cmd: list[str]


# -----------------------------------------------------------------------------
# Helpers CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run manual incremental refresh for price, FINRA, SEC, and research features."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root. Defaults to the detected repository root.",
    )
    parser.add_argument(
        "--log-dir",
        default=None,
        help="Directory for orchestrator logs. Defaults to <repo_root>/logs.",
    )
    parser.add_argument(
        "--python-bin",
        default=sys.executable,
        help="Python executable used to run child scripts.",
    )

    # ------------------------------------------------------------------
    # FINRA raw controls
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-finra-download",
        action="store_true",
        help="Skip persistent FINRA raw download step.",
    )
    parser.add_argument(
        "--skip-finra-raw-load",
        action="store_true",
        help="Skip FINRA raw load into DuckDB.",
    )
    parser.add_argument(
        "--finra-timeout-seconds",
        type=int,
        default=60,
        help="Timeout used by download_finra_raw_persistent.py.",
    )
    parser.add_argument(
        "--finra-sleep-seconds",
        type=float,
        default=0.20,
        help="Sleep between FINRA page/file requests.",
    )
    parser.add_argument(
        "--finra-source-name",
        default="finra_disk_raw_v3",
        help="source_name written by the FINRA raw loader.",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default="24GB",
        help="DuckDB memory limit for heavy loaders/builders.",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=6,
        help="DuckDB threads for heavy loaders/builders.",
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="DuckDB temp directory. Defaults to <repo_root>/tmp.",
    )

    # ------------------------------------------------------------------
    # SEC controls
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-sec",
        action="store_true",
        help="Skip SEC incremental steps.",
    )
    parser.add_argument(
        "--sec-reprocess-recent-months",
        type=int,
        default=3,
        help="Overlap window for incremental SEC jobs.",
    )

    # ------------------------------------------------------------------
    # Price controls
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-price",
        action="store_true",
        help="Skip price and price-feature steps.",
    )
    parser.add_argument(
        "--price-lookback-days",
        type=int,
        default=5,
        help="Lookback days passed to build_prices.py.",
    )
    parser.add_argument(
        "--price-catchup-max-days",
        type=int,
        default=30,
        help="Catch-up cap passed to build_prices.py.",
    )

    # ------------------------------------------------------------------
    # Final refresh controls
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-research-refresh",
        action="store_true",
        help="Skip the final research_features_daily rebuild.",
    )

    # ------------------------------------------------------------------
    # Optional pre-hook for SEC raw ingestion if the user stabilizes one later.
    # ------------------------------------------------------------------
    parser.add_argument(
        "--sec-raw-pre-cmd",
        default="",
        help=(
            "Optional shell command run before SEC incremental builders. "
            "Example: \"python3 cli/ops/run_sec_raw_incremental.py --db-path ...\""
        ),
    )

    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_cmd(cmd: Iterable[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


# -----------------------------------------------------------------------------
# Subprocess runner with live terminal + log file.
# -----------------------------------------------------------------------------
def run_step(
    *,
    step: Step,
    repo_root: Path,
    log_handle,
    env: dict[str, str],
) -> None:
    started = time.time()
    header = f"\n===== STEP START: {step.name} =====\n"
    header += f"cwd={repo_root}\n"
    header += f"cmd={format_cmd(step.cmd)}\n"
    header += "===================================\n"

    print(header, end="", flush=True)
    log_handle.write(header)
    log_handle.flush()

    process = subprocess.Popen(
        step.cmd,
        cwd=str(repo_root),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert process.stdout is not None

    for line in process.stdout:
        # ------------------------------------------------------------------
        # On relaie tel quel vers terminal + log.
        # Le but est de préserver les tqdm / progress déjà fournis par les
        # scripts enfants, sans pipeline shell qui pourrait les casser.
        # ------------------------------------------------------------------
        print(line, end="", flush=True)
        log_handle.write(line)
        log_handle.flush()

    return_code = process.wait()
    elapsed = time.time() - started

    footer = (
        f"\n===== STEP END: {step.name} =====\n"
        f"return_code={return_code}\n"
        f"elapsed_seconds={elapsed:.2f}\n"
        "=================================\n"
    )
    print(footer, end="", flush=True)
    log_handle.write(footer)
    log_handle.flush()

    if return_code != 0:
        raise SystemExit(f"step failed: {step.name}")


# -----------------------------------------------------------------------------
# Build step list
# -----------------------------------------------------------------------------
def build_steps(args: argparse.Namespace, repo_root: Path) -> list[Step]:
    python_bin = args.python_bin
    db_path = str(Path(args.db_path).expanduser().resolve())
    temp_dir = str((Path(args.temp_dir).expanduser().resolve() if args.temp_dir else repo_root / "tmp"))

    steps: list[Step] = []

    # ------------------------------------------------------------------
    # PRICE
    # ------------------------------------------------------------------
    if not args.skip_price:
        steps.append(
            Step(
                name="price_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_prices.py",
                    "--db-path",
                    db_path,
                    "--lookback-days",
                    str(args.price_lookback_days),
                    "--catchup-max-days",
                    str(args.price_catchup_max_days),
                ],
            )
        )
        steps.append(
            Step(
                name="feature_price_momentum_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_feature_price_momentum.py",
                    "--db-path",
                    db_path,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--verbose",
                ],
            )
        )
        steps.append(
            Step(
                name="feature_price_trend_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_feature_price_trend.py",
                    "--db-path",
                    db_path,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--verbose",
                ],
            )
        )
        steps.append(
            Step(
                name="feature_price_volatility_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_feature_price_volatility.py",
                    "--db-path",
                    db_path,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--verbose",
                ],
            )
        )

    # ------------------------------------------------------------------
    # FINRA
    # ------------------------------------------------------------------
    if not args.skip_finra_download:
        steps.append(
            Step(
                name="finra_download_raw_persistent",
                cmd=[
                    python_bin,
                    "cli/core/download_finra_raw_persistent.py",
                    "--repo-root",
                    str(repo_root),
                    "--dataset",
                    "all",
                    "--timeout-seconds",
                    str(args.finra_timeout_seconds),
                    "--sleep-seconds",
                    str(args.finra_sleep_seconds),
                ],
            )
        )

    if not args.skip_finra_raw_load:
        steps.append(
            Step(
                name="finra_load_daily_short_volume_raw",
                cmd=[
                    python_bin,
                    "cli/raw/load_finra_daily_short_volume_raw.py",
                    "--db-path",
                    db_path,
                    "--default-root",
                    str(repo_root / "data" / "raw" / "finra" / "daily_short_sale_volume"),
                    "--source-name",
                    args.finra_source_name,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--verbose",
                ],
            )
        )

    steps.append(
        Step(
            name="finra_build_daily_short_volume",
            cmd=[
                python_bin,
                "cli/core/build_finra_daily_short_volume.py",
                "--db-path",
                db_path,
            ],
        )
    )
    steps.append(
        Step(
            name="finra_build_short_features",
            cmd=[
                python_bin,
                "cli/core/build_short_features.py",
                "--db-path",
                db_path,
                "--duckdb-threads",
                str(args.duckdb_threads),
                "--duckdb-memory-limit",
                args.duckdb_memory_limit,
            ],
        )
    )

    # ------------------------------------------------------------------
    # SEC
    # ------------------------------------------------------------------
    if not args.skip_sec and args.sec_raw_pre_cmd.strip():
        steps.append(
            Step(
                name="sec_raw_pre_cmd",
                cmd=["bash", "-lc", args.sec_raw_pre_cmd],
            )
        )

    if not args.skip_sec:
        steps.append(
            Step(
                name="sec_build_filings_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_sec_filings.py",
                    "--db-path",
                    db_path,
                    "--reprocess-recent-months",
                    str(args.sec_reprocess_recent_months),
                    "--verbose",
                ],
            )
        )
        steps.append(
            Step(
                name="sec_build_fact_normalized_incremental",
                cmd=[
                    python_bin,
                    "cli/core/build_sec_fact_normalized.py",
                    "--db-path",
                    db_path,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--reprocess-recent-months",
                    str(args.sec_reprocess_recent_months),
                    "--verbose",
                ],
            )
        )

    # ------------------------------------------------------------------
    # FINAL RESEARCH FEATURES
    # ------------------------------------------------------------------
    if not args.skip_research_refresh:
        steps.append(
            Step(
                name="research_features_daily_refresh",
                cmd=[
                    python_bin,
                    "cli/core/build_research_features_daily.py",
                    "--db-path",
                    db_path,
                    "--memory-limit",
                    args.duckdb_memory_limit,
                    "--threads",
                    str(args.duckdb_threads),
                    "--temp-dir",
                    temp_dir,
                    "--verbose",
                ],
            )
        )

    return steps


# -----------------------------------------------------------------------------
# Final probe
# -----------------------------------------------------------------------------
def run_final_probe(db_path: Path) -> str:
    import duckdb

    lines: list[str] = []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        checks = [
            ("price_history", "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_history"),
            ("daily_short_volume_history", "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM daily_short_volume_history"),
            ("short_features_daily", "SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM short_features_daily"),
            ("sec_filing", "SELECT COUNT(*), MIN(filing_date), MAX(filing_date) FROM sec_filing"),
            ("sec_fact_normalized", "SELECT COUNT(*), MIN(period_end_date), MAX(period_end_date) FROM sec_fact_normalized"),
            ("research_features_daily", "SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM research_features_daily"),
        ]
        lines.append("===== FINAL PROBE =====")
        for label, sql in checks:
            try:
                row = con.execute(sql).fetchone()
                lines.append(f"{label} = {row}")
            except Exception as exc:
                lines.append(f"{label} = ERROR: {exc}")
    finally:
        con.close()

    return "\n".join(lines) + "\n"


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    repo_root = Path(args.repo_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    log_dir = Path(args.log_dir).expanduser().resolve() if args.log_dir else (repo_root / "logs")
    temp_dir = Path(args.temp_dir).expanduser().resolve() if args.temp_dir else (repo_root / "tmp")

    ensure_dir(log_dir)
    ensure_dir(temp_dir)

    run_log_path = log_dir / f"manual_incremental_data_refresh_{now_ts()}.log"

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    steps = build_steps(args, repo_root=repo_root)

    with run_log_path.open("a", encoding="utf-8") as log_handle:
        header = (
            "===== MANUAL INCREMENTAL DATA REFRESH =====\n"
            f"started_at={datetime.now().isoformat(timespec='seconds')}\n"
            f"repo_root={repo_root}\n"
            f"db_path={db_path}\n"
            f"log_path={run_log_path}\n"
            f"steps={len(steps)}\n"
            "===========================================\n"
        )
        print(header, end="", flush=True)
        log_handle.write(header)
        log_handle.flush()

        for step in steps:
            run_step(
                step=step,
                repo_root=repo_root,
                log_handle=log_handle,
                env=env,
            )

        probe = run_final_probe(db_path=db_path)
        print(probe, end="", flush=True)
        log_handle.write(probe)
        log_handle.flush()

        footer = (
            "===== MANUAL INCREMENTAL DATA REFRESH DONE =====\n"
            f"finished_at={datetime.now().isoformat(timespec='seconds')}\n"
            f"log_path={run_log_path}\n"
            "================================================\n"
        )
        print(footer, end="", flush=True)
        log_handle.write(footer)
        log_handle.flush()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
