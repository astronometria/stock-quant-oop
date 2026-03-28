#!/usr/bin/env python3
from __future__ import annotations

"""
rebuild_database_from_scratch.py

Orchestrateur complet de rebuild avec séparation explicite:

1) chaîne raw/canonical/research
2) chaîne history enrichie
3) garde-fous PIT strict

Principes:
- SQL-first
- pas de faux PIT implicite
- pas de whitelist PIT si la profondeur history est insuffisante
- résumé JSON final clair et auditable
"""

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tqdm import tqdm


@dataclass
class StepResult:
    step_name: str
    command: list[str] | None
    started_at: str
    finished_at: str
    duration_seconds: float
    returncode: int
    skipped: bool
    reason: str | None


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild stock-quant DB with strict separation between research rebuild and PIT/history rebuild."
    )

    parser.add_argument("--project-root", default="~/stock-quant-oop")
    parser.add_argument("--db-path", default="~/stock-quant-oop-runtime/db/market.duckdb")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--verbose", action="store_true")

    # Raw inputs
    parser.add_argument(
        "--stooq-source",
        action="append",
        default=[],
        help="Repeatable path to a stooq source directory.",
    )
    parser.add_argument("--skip-price-backfill", action="store_true")
    parser.add_argument("--skip-finra", action="store_true")
    parser.add_argument("--skip-sec", action="store_true")

    # Metadata snapshots
    parser.add_argument("--skip-symbol-source-fetch", action="store_true")
    parser.add_argument("--skip-symbol-reference-load", action="store_true")
    parser.add_argument("--skip-market-universe", action="store_true")
    parser.add_argument("--skip-symbol-reference", action="store_true")

    # History
    parser.add_argument("--build-history", action="store_true")
    parser.add_argument("--build-pit-whitelist", action="store_true")
    parser.add_argument("--require-pit-ready", action="store_true")
    parser.add_argument("--min-history-dates", type=int, default=30)
    parser.add_argument(
        "--daily-list-root",
        default="~/stock-quant-oop/data/nasdaq_daily_list",
    )

    # Research
    parser.add_argument("--skip-research-rebuild", action="store_true")

    return parser.parse_args()


def build_child_path(project_root: Path, relative_script: str) -> Path:
    return (project_root / relative_script).resolve()


def discover_symbol_source_files(project_root: Path) -> list[Path]:
    """
    Cherche tous les snapshots metadata locaux déjà présents.

    Convention repo actuelle:
    - data/symbol_sources/nasdaq/**/*.csv
    - data/symbol_sources/sec/**/*.csv
    """
    symbol_root = project_root / "data" / "symbol_sources"
    patterns = [
        "nasdaq/**/*.csv",
        "sec/**/*.csv",
    ]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(symbol_root.glob(pattern))
    return sorted([p.resolve() for p in files if p.is_file()])


def run_step(step_name: str, cmd: list[str]) -> StepResult:
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
        skipped=False,
        reason=None,
    )
    if completed.returncode != 0:
        print(f"STEP FAILED: {step_name} (exit_code={completed.returncode})", flush=True)
        raise SystemExit(completed.returncode)
    return result


def skip_step(step_name: str, reason: str) -> StepResult:
    now = utcnow_iso()
    print(f"===== STEP: {step_name} =====", flush=True)
    print(f"SKIPPED: {reason}", flush=True)
    return StepResult(
        step_name=step_name,
        command=None,
        started_at=now,
        finished_at=now,
        duration_seconds=0.0,
        returncode=0,
        skipped=True,
        reason=reason,
    )


def python_duckdb_metrics(db_path: Path) -> dict[str, Any]:
    """
    Probe fiable via Python + DuckDB.
    """
    code = f"""
from pathlib import Path
import json
import duckdb

db_path = Path(r"{str(db_path)}")
con = duckdb.connect(str(db_path))
try:
    def safe_scalar(sql):
        try:
            row = con.execute(sql).fetchone()
            return None if row is None else row[0]
        except Exception:
            return None

    metrics = {{
        "table_counts": {{
            "listing_status_history": safe_scalar("SELECT COUNT(*) FROM listing_status_history"),
            "market_universe_history": safe_scalar("SELECT COUNT(*) FROM market_universe_history"),
            "symbol_reference_history": safe_scalar("SELECT COUNT(*) FROM symbol_reference_history"),
            "universe_membership_history": safe_scalar("SELECT COUNT(*) FROM universe_membership_history"),
            "listing_event_history": safe_scalar("SELECT COUNT(*) FROM listing_event_history"),
            "history_reconstruction_audit": safe_scalar("SELECT COUNT(*) FROM history_reconstruction_audit"),
            "research_universe_quality_20d": safe_scalar("SELECT COUNT(*) FROM research_universe_quality_20d"),
            "research_universe_whitelist_20d_pit": safe_scalar("SELECT COUNT(*) FROM research_universe_whitelist_20d_pit"),
            "research_split_dataset": safe_scalar("SELECT COUNT(*) FROM research_split_dataset"),
            "sec_filing_raw_index": safe_scalar("SELECT COUNT(*) FROM sec_filing_raw_index"),
        }},
        "history_source_depth": {{
            "market_universe_distinct_dates": safe_scalar("SELECT COUNT(DISTINCT as_of_date) FROM market_universe"),
            "symbol_reference_source_raw_distinct_dates": safe_scalar("SELECT COUNT(DISTINCT as_of_date) FROM symbol_reference_source_raw"),
            "market_universe_min_date": safe_scalar("SELECT MIN(as_of_date) FROM market_universe"),
            "market_universe_max_date": safe_scalar("SELECT MAX(as_of_date) FROM market_universe"),
            "symbol_reference_source_raw_min_date": safe_scalar("SELECT MIN(as_of_date) FROM symbol_reference_source_raw"),
            "symbol_reference_source_raw_max_date": safe_scalar("SELECT MAX(as_of_date) FROM symbol_reference_source_raw"),
        }},
        "pit_overlap": {{
            "quality_symbols": safe_scalar("SELECT COUNT(DISTINCT symbol) FROM research_universe_quality_20d"),
            "membership_symbols": safe_scalar("SELECT COUNT(DISTINCT symbol) FROM universe_membership_history"),
            "overlap_symbols": safe_scalar(\\"\\"
                SELECT COUNT(DISTINCT q.symbol)
                FROM research_universe_quality_20d q
                INNER JOIN universe_membership_history u
                    ON UPPER(TRIM(q.symbol)) = UPPER(TRIM(u.symbol))
            \\"\\"),
        }},
    }}
    print(json.dumps(metrics))
finally:
    con.close()
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return {
            "table_counts": {},
            "history_source_depth": {},
            "pit_overlap": {},
            "probe_error": completed.stderr.strip(),
        }
    return json.loads(completed.stdout.strip())


def compute_pit_level(metrics: dict[str, Any], min_history_dates: int) -> dict[str, Any]:
    depth = metrics.get("history_source_depth", {})
    overlap = metrics.get("pit_overlap", {})

    source_dates = max(
        int(depth.get("market_universe_distinct_dates") or 0),
        int(depth.get("symbol_reference_source_raw_distinct_dates") or 0),
    )
    overlap_symbols = int(overlap.get("overlap_symbols") or 0)

    if source_dates < 3:
        return {
            "pit_level": "PIT_LEVEL_0",
            "status": "insufficient_history_depth",
            "reason": f"only {source_dates} observed snapshot dates",
        }

    if source_dates < min_history_dates:
        return {
            "pit_level": "PIT_LEVEL_1",
            "status": "event_enriched_partial_history_only",
            "reason": f"observed snapshot depth={source_dates} < required={min_history_dates}",
        }

    if overlap_symbols < 1000:
        return {
            "pit_level": "PIT_LEVEL_1",
            "status": "history_depth_ok_but_low_overlap",
            "reason": f"overlap_symbols={overlap_symbols} < 1000",
        }

    return {
        "pit_level": "PIT_LEVEL_2",
        "status": "pit_strict_ready",
        "reason": "history depth and overlap thresholds satisfied",
    }


def main() -> int:
    args = parse_args()

    project_root = resolve_path(args.project_root)
    db_path = resolve_path(args.db_path)
    python_bin = resolve_path(args.python_bin)

    steps: list[StepResult] = []
    progress = tqdm(total=24, desc="rebuild_database_from_scratch", unit="step")

    def child(relative_script: str) -> Path:
        return build_child_path(project_root, relative_script)

    try:
        # --------------------------------------------------------------
        # 1) Metadata snapshot accumulation
        # --------------------------------------------------------------
        if args.skip_symbol_source_fetch:
            steps.append(skip_step("FETCH SYMBOL SOURCES", "requested by --skip-symbol-source-fetch"))
        else:
            # SEC tickers raw
            steps.append(
                run_step(
                    "FETCH SEC COMPANY TICKERS RAW",
                    [str(python_bin), str(child("cli/raw/fetch_sec_company_tickers_raw.py"))],
                )
            )
        progress.update(1)

        if args.skip_symbol_source_fetch:
            steps.append(skip_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", "requested by --skip-symbol-source-fetch"))
        else:
            steps.append(
                run_step(
                    "FETCH NASDAQ SYMBOL DIRECTORY RAW",
                    [str(python_bin), str(child("cli/raw/fetch_nasdaq_symbol_directory_raw.py"))],
                )
            )
        progress.update(1)

        symbol_files = discover_symbol_source_files(project_root)

        if args.skip_symbol_reference_load:
            steps.append(skip_step("LOAD SYMBOL SOURCE SNAPSHOTS", "requested by --skip-symbol-reference-load"))
        elif not symbol_files:
            steps.append(skip_step("LOAD SYMBOL SOURCE SNAPSHOTS", "no local symbol source files found"))
        else:
            for file_path in symbol_files:
                steps.append(
                    run_step(
                        f"LOAD SYMBOL SNAPSHOT [{file_path.name}]",
                        [
                            str(python_bin),
                            str(child("cli/raw/load_symbol_reference_source_raw.py")),
                            "--db-path",
                            str(db_path),
                            "--source",
                            str(file_path),
                            "--verbose",
                        ],
                    )
                )
        progress.update(1)

        # --------------------------------------------------------------
        # 2) Current metadata snapshots
        # --------------------------------------------------------------
        if args.skip_market_universe:
            steps.append(skip_step("BUILD MARKET UNIVERSE", "requested by --skip-market-universe"))
        else:
            cmd = [
                str(python_bin),
                str(child("cli/core/build_market_universe.py")),
                "--db-path",
                str(db_path),
            ]
            if args.verbose:
                cmd.append("--verbose")
            steps.append(run_step("BUILD MARKET UNIVERSE", cmd))
        progress.update(1)

        if args.skip_symbol_reference:
            steps.append(skip_step("BUILD SYMBOL REFERENCE", "requested by --skip-symbol-reference"))
        else:
            steps.append(
                run_step(
                    "BUILD SYMBOL REFERENCE",
                    [
                        str(python_bin),
                        str(child("cli/core/build_symbol_reference.py")),
                        "--db-path",
                        str(db_path),
                    ],
                )
            )
        progress.update(1)

        # --------------------------------------------------------------
        # 3) Price path
        # --------------------------------------------------------------
        if args.skip_price_backfill:
            steps.append(skip_step("LOAD STOOQ RAW", "requested by --skip-price-backfill"))
        else:
            if not args.stooq_source:
                steps.append(skip_step("LOAD STOOQ RAW", "no --stooq-source provided"))
            else:
                for source_dir in args.stooq_source:
                    steps.append(
                        run_step(
                            f"LOAD STOOQ RAW [{source_dir}]",
                            [
                                str(python_bin),
                                str(child("cli/raw/load_price_source_daily_raw_all_from_csv.py")),
                                "--db-path",
                                str(db_path),
                                "--source-dir",
                                str(resolve_path(source_dir)),
                            ],
                        )
                    )
        progress.update(1)

        steps.append(
            run_step(
                "INIT PRICES RESEARCH FOUNDATION",
                [
                    str(python_bin),
                    str(child("cli/core/init_prices_research_foundation.py")),
                    "--db-path",
                    str(db_path),
                ],
            )
        )
        progress.update(1)

        steps.append(
            run_step(
                "BUILD PRICES RESEARCH",
                [
                    str(python_bin),
                    str(child("cli/core/build_prices_research.py")),
                    "--db-path",
                    str(db_path),
                ],
            )
        )
        progress.update(1)

        # --------------------------------------------------------------
        # 4) SEC / FINRA optional
        # --------------------------------------------------------------
        if args.skip_sec:
            steps.append(skip_step("SEC ENRICHMENT INPUTS", "requested by --skip-sec"))
        else:
            # Placeholder: la reconstruction principale peut déjà utiliser les tables si remplies
            steps.append(skip_step("SEC ENRICHMENT INPUTS", "using existing SEC tables already present in DB"))
        progress.update(1)

        if args.skip_finra:
            steps.append(skip_step("FINRA LOAD / BUILD", "requested by --skip-finra"))
        else:
            steps.append(skip_step("FINRA LOAD / BUILD", "not forced by this orchestrator version"))
        progress.update(1)

        # --------------------------------------------------------------
        # 5) Research rebuild
        # --------------------------------------------------------------
        if args.skip_research_rebuild:
            for name in [
                "BUILD FEATURE PRICE MOMENTUM",
                "BUILD FEATURE PRICE TREND",
                "BUILD FEATURE PRICE VOLATILITY",
                "BUILD RESEARCH FEATURES DAILY",
                "BUILD RESEARCH LABELS",
                "BUILD RESEARCH UNIVERSE QUALITY 20D",
                "BUILD RESEARCH TRAINING DATASET",
                "BUILD RESEARCH SPLITS",
            ]:
                steps.append(skip_step(name, "requested by --skip-research-rebuild"))
        else:
            for relative_script, label in [
                ("cli/core/build_feature_price_momentum.py", "BUILD FEATURE PRICE MOMENTUM"),
                ("cli/core/build_feature_price_trend.py", "BUILD FEATURE PRICE TREND"),
                ("cli/core/build_feature_price_volatility.py", "BUILD FEATURE PRICE VOLATILITY"),
                ("cli/core/build_research_features_daily.py", "BUILD RESEARCH FEATURES DAILY"),
                ("cli/core/build_research_labels.py", "BUILD RESEARCH LABELS"),
                ("cli/core/build_research_universe_quality_20d.py", "BUILD RESEARCH UNIVERSE QUALITY 20D"),
                ("cli/core/build_research_training_dataset.py", "BUILD RESEARCH TRAINING DATASET"),
                ("cli/core/build_research_splits.py", "BUILD RESEARCH SPLITS"),
            ]:
                steps.append(
                    run_step(
                        label,
                        [str(python_bin), str(child(relative_script)), "--db-path", str(db_path)],
                    )
                )
        progress.update(8)

        # --------------------------------------------------------------
        # 6) History rebuild
        # --------------------------------------------------------------
        if not args.build_history:
            for name in [
                "BUILD LISTING HISTORY",
                "ENRICH LISTING HISTORY FROM SEC",
                "APPLY LISTING EVENTS FROM SNAPSHOT DIFFS",
                "APPLY LISTING EVENTS FROM NASDAQ DAILY LIST",
                "BUILD MARKET UNIVERSE HISTORY",
                "BUILD SYMBOL REFERENCE HISTORY",
                "BUILD UNIVERSE MEMBERSHIP HISTORY",
            ]:
                steps.append(skip_step(name, "requested by absence of --build-history"))
        else:
            steps.append(
                run_step(
                    "BUILD LISTING HISTORY",
                    [str(python_bin), str(child("cli/core/build_listing_history.py")), "--db-path", str(db_path)],
                )
            )
            steps.append(
                run_step(
                    "ENRICH LISTING HISTORY FROM SEC",
                    [str(python_bin), str(child("cli/core/enrich_listing_history_from_sec_submissions.py")), "--db-path", str(db_path)],
                )
            )
            steps.append(
                run_step(
                    "APPLY LISTING EVENTS FROM SNAPSHOT DIFFS",
                    [str(python_bin), str(child("cli/core/apply_listing_events_from_snapshots.py")), "--db-path", str(db_path)],
                )
            )
            steps.append(
                run_step(
                    "APPLY LISTING EVENTS FROM NASDAQ DAILY LIST",
                    [
                        str(python_bin),
                        str(child("cli/core/apply_listing_events_from_nasdaq_daily_list.py")),
                        "--db-path",
                        str(db_path),
                        "--daily-list-root",
                        str(resolve_path(args.daily_list_root)),
                    ],
                )
            )
            steps.append(
                run_step(
                    "BUILD MARKET UNIVERSE HISTORY",
                    [str(python_bin), str(child("cli/core/build_market_universe_history.py")), "--db-path", str(db_path)],
                )
            )
            steps.append(
                run_step(
                    "BUILD SYMBOL REFERENCE HISTORY",
                    [str(python_bin), str(child("cli/core/build_symbol_reference_history.py")), "--db-path", str(db_path)],
                )
            )
            steps.append(
                run_step(
                    "BUILD UNIVERSE MEMBERSHIP HISTORY",
                    [
                        str(python_bin),
                        str(child("cli/core/build_universe_membership_history_from_market_universe_history.py")),
                        "--db-path",
                        str(db_path),
                    ],
                )
            )
        progress.update(1)

        # --------------------------------------------------------------
        # 7) Probe PIT readiness
        # --------------------------------------------------------------
        metrics = python_duckdb_metrics(db_path)
        pit_verdict = compute_pit_level(metrics, args.min_history_dates)

        if not args.build_pit_whitelist:
            steps.append(skip_step("BUILD PIT WHITELIST", "requested by absence of --build-pit-whitelist"))
        elif pit_verdict["pit_level"] != "PIT_LEVEL_2":
            steps.append(skip_step("BUILD PIT WHITELIST", pit_verdict["reason"]))
        else:
            steps.append(
                run_step(
                    "BUILD PIT WHITELIST",
                    [
                        str(python_bin),
                        str(child("cli/core/build_research_universe_whitelist_20d_pit.py")),
                        "--db-path",
                        str(db_path),
                    ],
                )
            )
        progress.update(1)

        if args.require_pit_ready and pit_verdict["pit_level"] != "PIT_LEVEL_2":
            print(
                json.dumps(
                    {
                        "status": "FAILED",
                        "reason": "PIT strict required but not ready",
                        "pit_verdict": pit_verdict,
                        "metrics": metrics,
                        "steps": [asdict(step) for step in steps],
                    },
                    indent=2,
                )
            )
            return 2

        print(
            json.dumps(
                {
                    "status": "SUCCESS",
                    "project_root": str(project_root),
                    "db_path": str(db_path),
                    "pit_verdict": pit_verdict,
                    "metrics": metrics,
                    "steps": [asdict(step) for step in steps],
                },
                indent=2,
            )
        )
        return 0

    finally:
        progress.close()


if __name__ == "__main__":
    raise SystemExit(main())
