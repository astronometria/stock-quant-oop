#!/usr/bin/env python3
from __future__ import annotations

"""
rebuild_database_from_scratch.py

Orchestrateur complet de rebuild, réécrit avec les constats d'audit récents.

Objectifs de cette version:
- conserver le chemin raw -> canonical -> research qui fonctionne réellement
- intégrer la couche history/PIT réelle quand elle est disponible
- NE JAMAIS fabriquer un faux PIT par bootstrap implicite
- rendre explicite la différence entre:
    * rebuild recherche non-PIT / PIT-light exploitable
    * rebuild PIT scientifique strict
- produire un résumé JSON final facile à auditer

Philosophie:
- les prix historiques sont reconstruits via:
    1) chargement Stooq dans price_source_daily_raw_all
    2) run_core_pipeline -> price_source_daily_raw + price_history
    3) init_prices_research_foundation + build_prices_research -> price_bars_adjusted
- la chaîne research valide:
    price_bars_adjusted
    -> feature_price_*_daily
    -> research_features_daily
    -> research_labels
    -> research_universe_quality_20d
    -> research_training_dataset
    -> research_split_dataset
- la chaîne PIT stricte:
    listing_status_history / market_universe_history / symbol_reference_history
    -> instrument_master / ticker_history / universe_membership_history
    -> research_universe_whitelist_20d_pit

Important:
- si la profondeur history source est insuffisante, on le dit clairement
- si --require-pit-ready est activé, on échoue explicitement
- aucune whitelist PIT ne doit être créée à partir d'un faux historique inventé
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
        description="Rebuild stock-quant DB using the valid raw/canonical/research path and strict PIT guards."
    )

    parser.add_argument("--project-root", default="~/stock-quant-oop")
    parser.add_argument("--db-path", default="~/stock-quant-oop-runtime/db/market.duckdb")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--temp-dir", default="~/stock-quant-oop-runtime/tmp")
    parser.add_argument("--memory-limit", default="24GB")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--verbose", action="store_true")

    # Stooq / prix
    parser.add_argument("--stooq-source", action="append", default=[])
    parser.add_argument("--price-symbol", action="append", default=[])
    parser.add_argument("--price-start-date")
    parser.add_argument("--price-end-date")

    # FINRA
    parser.add_argument("--finra-output-dir", default="~/stock-quant-oop-raw/data/raw/finra")
    parser.add_argument("--finra-source-market", default="us")
    parser.add_argument("--finra-start-date")
    parser.add_argument("--finra-end-date")
    parser.add_argument("--finra-overwrite-downloads", action="store_true")
    parser.add_argument("--finra-truncate-raw", action="store_true")

    # Universe rules
    parser.add_argument("--allow-adr", action="store_true")

    # History / PIT controls
    parser.add_argument(
        "--build-history",
        action="store_true",
        help="Try to build listing_status_history / market_universe_history / symbol_reference_history.",
    )
    parser.add_argument(
        "--build-pit-whitelist",
        action="store_true",
        help="Try to build research_universe_whitelist_20d_pit, but only if history depth is sufficient.",
    )
    parser.add_argument(
        "--require-pit-ready",
        action="store_true",
        help="Fail the run if PIT strict readiness is not achieved.",
    )
    parser.add_argument(
        "--min-history-dates",
        type=int,
        default=30,
        help="Minimum distinct snapshot dates required before PIT strict can be considered usable.",
    )

    # Skip flags
    parser.add_argument("--skip-init-db", action="store_true")
    parser.add_argument("--skip-symbol-source-fetch", action="store_true")
    parser.add_argument("--skip-symbol-reference-load", action="store_true")
    parser.add_argument("--skip-market-universe", action="store_true")
    parser.add_argument("--skip-symbol-reference", action="store_true")
    parser.add_argument("--skip-sec-filings", action="store_true")
    parser.add_argument("--skip-sec-facts", action="store_true")
    parser.add_argument("--skip-fundamentals", action="store_true")
    parser.add_argument("--skip-price-backfill", action="store_true")
    parser.add_argument("--skip-price-daily", action="store_true")
    parser.add_argument("--skip-finra", action="store_true")
    parser.add_argument("--skip-research-rebuild", action="store_true")

    return parser.parse_args()


def build_child_path(project_root: Path, relative_script: str) -> Path:
    return (project_root / relative_script).resolve()


def discover_symbol_source_files(project_root: Path) -> list[Path]:
    """
    Les fetchers raw actuels écrivent sous:
      project_root / data / symbol_sources / ...
    """
    symbol_root = project_root / "data" / "symbol_sources"
    patterns = [
        "sec/**/*.csv",
        "nasdaq/**/*.csv",
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


def duckdb_scalar(db_path: Path, sql: str) -> Any:
    cmd = [
        "duckdb",
        str(db_path),
        "-c",
        sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        return None

    # DuckDB shell table output is not ideal for parsing.
    # We therefore prefer a plain SELECT with COPY-compatible formatting using Python only
    # when possible. Here, keep a permissive fallback.
    return "\n".join(lines)


def python_duckdb_metrics(db_path: Path) -> dict[str, Any]:
    """
    Petit probe Python pour récupérer des métriques clés de façon fiable.
    """
    code = f"""
from pathlib import Path
import json
import duckdb

db_path = Path(r"{str(db_path)}")
con = duckdb.connect(str(db_path))
try:
    tables = {{r[0] for r in con.execute("SHOW TABLES").fetchall()}}

    def safe_scalar(sql):
        try:
            row = con.execute(sql).fetchone()
            return None if row is None else row[0]
        except Exception:
            return None

    metrics = {{
        "table_counts": {{
            "instrument_master": safe_scalar("SELECT COUNT(*) FROM instrument_master"),
            "ticker_history": safe_scalar("SELECT COUNT(*) FROM ticker_history"),
            "universe_membership_history": safe_scalar("SELECT COUNT(*) FROM universe_membership_history"),
            "listing_status_history": safe_scalar("SELECT COUNT(*) FROM listing_status_history"),
            "market_universe_history": safe_scalar("SELECT COUNT(*) FROM market_universe_history"),
            "symbol_reference_history": safe_scalar("SELECT COUNT(*) FROM symbol_reference_history"),
            "research_universe_quality_20d": safe_scalar("SELECT COUNT(*) FROM research_universe_quality_20d"),
            "research_universe_whitelist_20d_pit": safe_scalar("SELECT COUNT(*) FROM research_universe_whitelist_20d_pit"),
            "research_split_dataset": safe_scalar("SELECT COUNT(*) FROM research_split_dataset"),
        }},
        "history_source_depth": {{
            "market_universe_distinct_dates": safe_scalar("SELECT COUNT(DISTINCT as_of_date) FROM market_universe"),
            "symbol_reference_source_raw_distinct_dates": safe_scalar("SELECT COUNT(DISTINCT as_of_date) FROM symbol_reference_source_raw"),
            "market_universe_min_date": safe_scalar("SELECT MIN(as_of_date) FROM market_universe"),
            "market_universe_max_date": safe_scalar("SELECT MAX(as_of_date) FROM market_universe"),
            "symbol_reference_source_raw_min_date": safe_scalar("SELECT MIN(as_of_date) FROM symbol_reference_source_raw"),
            "symbol_reference_source_raw_max_date": safe_scalar("SELECT MAX(as_of_date) FROM symbol_reference_source_raw"),
        }},
    }}
    print(json.dumps(metrics))
finally:
    con.close()
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    if result.returncode != 0:
        return {"error": result.stderr.strip()}
    try:
        return json.loads(result.stdout.strip())
    except Exception:
        return {"error": "failed to decode python_duckdb_metrics output", "raw": result.stdout}


def history_depth_is_sufficient(metrics: dict[str, Any], min_history_dates: int) -> tuple[bool, dict[str, Any]]:
    depth = metrics.get("history_source_depth", {})
    market_dates = depth.get("market_universe_distinct_dates") or 0
    symbol_dates = depth.get("symbol_reference_source_raw_distinct_dates") or 0

    sufficient = (market_dates >= min_history_dates) and (symbol_dates >= min_history_dates)
    return sufficient, {
        "market_universe_distinct_dates": market_dates,
        "symbol_reference_source_raw_distinct_dates": symbol_dates,
        "min_history_dates_required": min_history_dates,
    }


def main() -> int:
    args = parse_args()

    project_root = resolve_path(args.project_root)
    db_path = resolve_path(args.db_path)
    temp_dir = resolve_path(args.temp_dir)
    python_bin = str(resolve_path(args.python_bin))
    finra_output_dir = resolve_path(args.finra_output_dir)
    stooq_sources = [resolve_path(item) for item in args.stooq_source]

    db_path.parent.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(parents=True, exist_ok=True)

    script_presence = {
        "init_market_db": build_child_path(project_root, "cli/core/init_market_db.py").exists(),
        "fetch_sec_company_tickers_raw": build_child_path(project_root, "cli/raw/fetch_sec_company_tickers_raw.py").exists(),
        "fetch_nasdaq_symbol_directory_raw": build_child_path(project_root, "cli/raw/fetch_nasdaq_symbol_directory_raw.py").exists(),
        "load_symbol_reference_source_raw": build_child_path(project_root, "cli/raw/load_symbol_reference_source_raw.py").exists(),
        "build_market_universe": build_child_path(project_root, "cli/core/build_market_universe.py").exists(),
        "build_symbol_reference": build_child_path(project_root, "cli/core/build_symbol_reference.py").exists(),
        "build_sec_filings": build_child_path(project_root, "cli/core/build_sec_filings.py").exists(),
        "build_sec_fact_normalized": build_child_path(project_root, "cli/core/build_sec_fact_normalized.py").exists(),
        "build_fundamentals": build_child_path(project_root, "cli/core/build_fundamentals.py").exists(),
        "load_price_source_daily_raw_all_from_stooq_dir": build_child_path(project_root, "cli/raw/load_price_source_daily_raw_all_from_stooq_dir.py").exists(),
        "run_core_pipeline": build_child_path(project_root, "cli/core/run_core_pipeline.py").exists(),
        "run_price_daily_refresh": build_child_path(project_root, "cli/ops/run_price_daily_refresh.py").exists(),
        "run_finra_daily_refresh": build_child_path(project_root, "cli/ops/run_finra_daily_refresh.py").exists(),
        "init_prices_research_foundation": build_child_path(project_root, "cli/core/init_prices_research_foundation.py").exists(),
        "build_prices_research": build_child_path(project_root, "cli/core/build_prices_research.py").exists(),
        "build_research_universe": build_child_path(project_root, "cli/core/build_research_universe.py").exists(),
        "build_feature_price_momentum": build_child_path(project_root, "cli/core/build_feature_price_momentum.py").exists(),
        "build_feature_price_trend": build_child_path(project_root, "cli/core/build_feature_price_trend.py").exists(),
        "build_feature_price_volatility": build_child_path(project_root, "cli/core/build_feature_price_volatility.py").exists(),
        "build_research_features_daily": build_child_path(project_root, "cli/core/build_research_features_daily.py").exists(),
        "build_research_labels": build_child_path(project_root, "cli/core/build_research_labels.py").exists(),
        "build_research_universe_quality_20d": build_child_path(project_root, "cli/core/build_research_universe_quality_20d.py").exists(),
        "build_research_universe_whitelist_20d_pit": build_child_path(project_root, "cli/core/build_research_universe_whitelist_20d_pit.py").exists(),
        "build_research_training_dataset": build_child_path(project_root, "cli/core/build_research_training_dataset.py").exists(),
        "build_research_splits": build_child_path(project_root, "cli/core/build_research_splits.py").exists(),
        "build_listing_history": build_child_path(project_root, "cli/core/build_listing_history.py").exists(),
        "build_market_universe_history": build_child_path(project_root, "cli/core/build_market_universe_history.py").exists(),
        "build_symbol_reference_history": build_child_path(project_root, "cli/core/build_symbol_reference_history.py").exists(),
    }

    preflight = {
        "db_path": str(db_path),
        "project_root": str(project_root),
        "python_bin": python_bin,
        "temp_dir": str(temp_dir),
        "script_presence": script_presence,
        "stooq_sources": [
            {
                "path": str(source),
                "exists": source.exists(),
                "is_dir": source.is_dir(),
                "is_file": source.is_file(),
            }
            for source in stooq_sources
        ],
    }

    print("===== PREFLIGHT =====", flush=True)
    print(json.dumps(preflight, indent=2), flush=True)

    if not args.skip_price_backfill and not stooq_sources:
        raise SystemExit("missing --stooq-source for price backfill")

    steps: list[StepResult] = []
    started_at = datetime.now(timezone.utc)

    progress = tqdm(total=22, desc="rebuild_db", unit="step")

    # -------------------------------------------------------------------------
    # 1) INIT DB
    # -------------------------------------------------------------------------
    if args.skip_init_db:
        steps.append(skip_step("INIT MARKET DB", "requested by --skip-init-db"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/init_market_db.py")),
            "--db-path",
            str(db_path),
        ]
        steps.append(run_step("INIT MARKET DB", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 2) FETCH SYMBOL SOURCES
    # -------------------------------------------------------------------------
    if args.skip_symbol_source_fetch:
        steps.append(skip_step("FETCH SEC COMPANY TICKERS RAW", "requested by --skip-symbol-source-fetch"))
        steps.append(skip_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", "requested by --skip-symbol-source-fetch"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/raw/fetch_sec_company_tickers_raw.py")),
        ]
        steps.append(run_step("FETCH SEC COMPANY TICKERS RAW", cmd))

        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/raw/fetch_nasdaq_symbol_directory_raw.py")),
        ]
        steps.append(run_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", cmd))
    progress.update(2)

    # -------------------------------------------------------------------------
    # 3) LOAD SYMBOL SOURCE RAW
    # -------------------------------------------------------------------------
    if args.skip_symbol_reference_load:
        steps.append(skip_step("LOAD SYMBOL REFERENCE SOURCE RAW", "requested by --skip-symbol-reference-load"))
    else:
        symbol_sources = discover_symbol_source_files(project_root)
        if not symbol_sources:
            raise SystemExit(f"no symbol source files found under {project_root / 'data' / 'symbol_sources'}")

        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/raw/load_symbol_reference_source_raw.py")),
            "--db-path",
            str(db_path),
        ]
        for source in symbol_sources:
            cmd.extend(["--source", str(source)])
        steps.append(run_step("LOAD SYMBOL REFERENCE SOURCE RAW", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 4) BUILD MARKET UNIVERSE
    # -------------------------------------------------------------------------
    if args.skip_market_universe:
        steps.append(skip_step("BUILD MARKET UNIVERSE", "requested by --skip-market-universe"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/build_market_universe.py")),
            "--db-path",
            str(db_path),
        ]
        if args.allow_adr:
            cmd.append("--allow-adr")
        steps.append(run_step("BUILD MARKET UNIVERSE", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 5) BUILD SYMBOL REFERENCE
    # -------------------------------------------------------------------------
    if args.skip_symbol_reference:
        steps.append(skip_step("BUILD SYMBOL REFERENCE", "requested by --skip-symbol-reference"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/build_symbol_reference.py")),
            "--db-path",
            str(db_path),
        ]
        steps.append(run_step("BUILD SYMBOL REFERENCE", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 6) SEC FILINGS
    # -------------------------------------------------------------------------
    if args.skip_sec_filings:
        steps.append(skip_step("BUILD SEC FILINGS", "requested by --skip-sec-filings"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/build_sec_filings.py")),
            "--db-path",
            str(db_path),
        ]
        steps.append(run_step("BUILD SEC FILINGS", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 7) SEC FACTS
    # -------------------------------------------------------------------------
    if args.skip_sec_facts:
        steps.append(skip_step("BUILD SEC FACT NORMALIZED", "requested by --skip-sec-facts"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/build_sec_fact_normalized.py")),
            "--memory-limit",
            str(args.memory_limit),
            "--threads",
            str(args.threads),
            "--temp-dir",
            str(temp_dir),
            "--db-path",
            str(db_path),
        ]
        steps.append(run_step("BUILD SEC FACT NORMALIZED", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 8) FUNDAMENTALS
    # -------------------------------------------------------------------------
    if args.skip_fundamentals:
        steps.append(skip_step("BUILD FUNDAMENTALS", "requested by --skip-fundamentals"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/build_fundamentals.py")),
            "--memory-limit",
            str(args.memory_limit),
            "--threads",
            str(args.threads),
            "--temp-dir",
            str(temp_dir),
            "--db-path",
            str(db_path),
        ]
        steps.append(run_step("BUILD FUNDAMENTALS", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 9) PRICE BACKFILL
    # -------------------------------------------------------------------------
    if args.skip_price_backfill:
        steps.append(skip_step("LOAD STOOQ RAW ALL", "requested by --skip-price-backfill"))
        steps.append(skip_step("RUN CORE PIPELINE", "requested by --skip-price-backfill"))
    else:
        for idx, source in enumerate(stooq_sources, start=1):
            cmd = [
                python_bin,
                str(build_child_path(project_root, "cli/raw/load_price_source_daily_raw_all_from_stooq_dir.py")),
                "--db-path",
                str(db_path),
                "--root-dir",
                str(source),
            ]
            if idx == 1:
                cmd.append("--truncate")
            steps.append(run_step(f"LOAD STOOQ RAW ALL [{idx}/{len(stooq_sources)}]", cmd))

        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/core/run_core_pipeline.py")),
            "--db-path",
            str(db_path),
        ]
        if args.verbose:
            cmd.append("--verbose")
        steps.append(run_step("RUN CORE PIPELINE", cmd))
    progress.update(2)

    # -------------------------------------------------------------------------
    # 10) PRICE DAILY REFRESH
    # -------------------------------------------------------------------------
    if args.skip_price_daily:
        steps.append(skip_step("RUN PRICE DAILY REFRESH", "requested by --skip-price-daily"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/ops/run_price_daily_refresh.py")),
            "--project-root",
            str(project_root),
            "--db-path",
            str(db_path),
        ]
        if args.price_symbol:
            cmd.append("--symbols")
            cmd.extend(args.price_symbol)
        if args.price_start_date:
            cmd.extend(["--start-date", args.price_start_date])
        if args.price_end_date:
            cmd.extend(["--end-date", args.price_end_date])
        steps.append(run_step("RUN PRICE DAILY REFRESH", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 11) FINRA
    # -------------------------------------------------------------------------
    if args.skip_finra:
        steps.append(skip_step("RUN FINRA DAILY REFRESH", "requested by --skip-finra"))
    else:
        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/ops/run_finra_daily_refresh.py")),
            "--project-root",
            str(project_root),
            "--db-path",
            str(db_path),
            "--output-dir",
            str(finra_output_dir),
            "--source-market",
            str(args.finra_source_market),
        ]
        if args.finra_start_date:
            cmd.extend(["--start-date", args.finra_start_date])
        if args.finra_end_date:
            cmd.extend(["--end-date", args.finra_end_date])
        if args.finra_overwrite_downloads:
            cmd.append("--overwrite-downloads")
        if args.finra_truncate_raw:
            cmd.append("--truncate-raw")
        steps.append(run_step("RUN FINRA DAILY REFRESH", cmd))
    progress.update(1)

    # -------------------------------------------------------------------------
    # 12) RESEARCH CANONICAL TABLES
    # -------------------------------------------------------------------------
    if args.skip_research_rebuild:
        steps.append(skip_step("INIT PRICES RESEARCH FOUNDATION", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD PRICES RESEARCH", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH UNIVERSE", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD FEATURE PRICE MOMENTUM", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD FEATURE PRICE TREND", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD FEATURE PRICE VOLATILITY", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH FEATURES DAILY", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH LABELS", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH UNIVERSE QUALITY 20D", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH TRAINING DATASET", "requested by --skip-research-rebuild"))
        steps.append(skip_step("BUILD RESEARCH SPLITS", "requested by --skip-research-rebuild"))
    else:
        research_steps = [
            ("INIT PRICES RESEARCH FOUNDATION", "cli/core/init_prices_research_foundation.py"),
            ("BUILD PRICES RESEARCH", "cli/core/build_prices_research.py"),
            ("BUILD RESEARCH UNIVERSE", "cli/core/build_research_universe.py"),
            ("BUILD FEATURE PRICE MOMENTUM", "cli/core/build_feature_price_momentum.py"),
            ("BUILD FEATURE PRICE TREND", "cli/core/build_feature_price_trend.py"),
            ("BUILD FEATURE PRICE VOLATILITY", "cli/core/build_feature_price_volatility.py"),
            ("BUILD RESEARCH FEATURES DAILY", "cli/core/build_research_features_daily.py"),
            ("BUILD RESEARCH LABELS", "cli/core/build_research_labels.py"),
            ("BUILD RESEARCH UNIVERSE QUALITY 20D", "cli/core/build_research_universe_quality_20d.py"),
            ("BUILD RESEARCH TRAINING DATASET", "cli/core/build_research_training_dataset.py"),
            ("BUILD RESEARCH SPLITS", "cli/core/build_research_splits.py"),
        ]
        for step_name, rel in research_steps:
            cmd = [
                python_bin,
                str(build_child_path(project_root, rel)),
                "--db-path",
                str(db_path),
            ]
            steps.append(run_step(step_name, cmd))
    progress.update(11)

    # -------------------------------------------------------------------------
    # 13) HISTORY BUILDERS
    # -------------------------------------------------------------------------
    history_metrics_before = python_duckdb_metrics(db_path)
    depth_ok_before, depth_detail_before = history_depth_is_sufficient(
        history_metrics_before,
        args.min_history_dates,
    )

    if args.build_history:
        history_steps = [
            ("BUILD LISTING HISTORY", "cli/core/build_listing_history.py"),
            ("BUILD MARKET UNIVERSE HISTORY", "cli/core/build_market_universe_history.py"),
            ("BUILD SYMBOL REFERENCE HISTORY", "cli/core/build_symbol_reference_history.py"),
        ]
        for step_name, rel in history_steps:
            cmd = [
                python_bin,
                str(build_child_path(project_root, rel)),
                "--db-path",
                str(db_path),
            ]
            steps.append(run_step(step_name, cmd))
    else:
        steps.append(skip_step("BUILD LISTING HISTORY", "requested by absence of --build-history"))
        steps.append(skip_step("BUILD MARKET UNIVERSE HISTORY", "requested by absence of --build-history"))
        steps.append(skip_step("BUILD SYMBOL REFERENCE HISTORY", "requested by absence of --build-history"))
    progress.update(3)

    history_metrics_after = python_duckdb_metrics(db_path)
    depth_ok_after, depth_detail_after = history_depth_is_sufficient(
        history_metrics_after,
        args.min_history_dates,
    )

    # -------------------------------------------------------------------------
    # 14) PIT WHITELIST
    # -------------------------------------------------------------------------
    pit_strict_ready = False
    pit_reason = None

    table_counts_after = history_metrics_after.get("table_counts", {})
    universe_membership_rows = table_counts_after.get("universe_membership_history") or 0
    ticker_history_rows = table_counts_after.get("ticker_history") or 0

    if args.build_pit_whitelist:
        if not depth_ok_after:
            pit_reason = (
                "history source depth is insufficient for scientific PIT strict mode "
                f"(details={depth_detail_after})"
            )
            steps.append(skip_step("BUILD RESEARCH UNIVERSE WHITELIST 20D PIT", pit_reason))
        elif universe_membership_rows <= 0:
            pit_reason = "universe_membership_history is empty after history build"
            steps.append(skip_step("BUILD RESEARCH UNIVERSE WHITELIST 20D PIT", pit_reason))
        elif ticker_history_rows <= 0:
            pit_reason = "ticker_history is empty after history build"
            steps.append(skip_step("BUILD RESEARCH UNIVERSE WHITELIST 20D PIT", pit_reason))
        else:
            cmd = [
                python_bin,
                str(build_child_path(project_root, "cli/core/build_research_universe_whitelist_20d_pit.py")),
                "--db-path",
                str(db_path),
            ]
            steps.append(run_step("BUILD RESEARCH UNIVERSE WHITELIST 20D PIT", cmd))
            pit_strict_ready = True
    else:
        pit_reason = "requested by absence of --build-pit-whitelist"
        steps.append(skip_step("BUILD RESEARCH UNIVERSE WHITELIST 20D PIT", pit_reason))
    progress.update(1)

    progress.close()

    if args.require_pit_ready and not pit_strict_ready:
        finished_at = datetime.now(timezone.utc)
        failure_summary = {
            "status": "failed",
            "reason": "PIT strict readiness was required but not achieved",
            "pit_reason": pit_reason,
            "history_depth_before": depth_detail_before,
            "history_depth_after": depth_detail_after,
            "table_counts_after": table_counts_after,
            "steps": [asdict(step) for step in steps],
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": (finished_at - started_at).total_seconds(),
        }
        print("===== REBUILD FAILED (PIT REQUIRED) =====", flush=True)
        print(json.dumps(failure_summary, indent=2), flush=True)
        raise SystemExit(2)

    finished_at = datetime.now(timezone.utc)

    summary = {
        "preflight": preflight,
        "summary": {
            "status": "success",
            "project_root": str(project_root),
            "db_path": str(db_path),
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_seconds": (finished_at - started_at).total_seconds(),
            "runtime": {
                "python_bin": python_bin,
                "temp_dir": str(temp_dir),
                "memory_limit": str(args.memory_limit),
                "threads": int(args.threads),
                "verbose": bool(args.verbose),
            },
            "inputs": {
                "allow_adr": bool(args.allow_adr),
                "stooq_source_count": len(stooq_sources),
                "stooq_sources": [str(item) for item in stooq_sources],
                "price_symbol_count": len(args.price_symbol),
                "price_symbols": list(args.price_symbol),
                "price_start_date": args.price_start_date,
                "price_end_date": args.price_end_date,
                "finra_output_dir": str(finra_output_dir),
                "finra_source_market": str(args.finra_source_market),
                "finra_start_date": args.finra_start_date,
                "finra_end_date": args.finra_end_date,
                "finra_overwrite_downloads": bool(args.finra_overwrite_downloads),
                "finra_truncate_raw": bool(args.finra_truncate_raw),
                "build_history": bool(args.build_history),
                "build_pit_whitelist": bool(args.build_pit_whitelist),
                "require_pit_ready": bool(args.require_pit_ready),
                "min_history_dates": int(args.min_history_dates),
            },
            "research_ready": {
                "research_split_dataset_rows": table_counts_after.get("research_split_dataset"),
                "research_universe_quality_20d_rows": table_counts_after.get("research_universe_quality_20d"),
            },
            "pit_readiness": {
                "strict_ready": pit_strict_ready,
                "reason_if_not_ready": pit_reason,
                "history_depth_before": depth_detail_before,
                "history_depth_after": depth_detail_after,
                "table_counts_after": table_counts_after,
            },
            "notes": [
                "This rebuild never fabricates PIT history from current-state snapshots.",
                "Scientific PIT strict mode requires enough historical source depth and non-empty ticker_history + universe_membership_history.",
                "If history depth is insufficient, research rebuild can still succeed while PIT strict remains explicitly not ready.",
            ],
            "steps": [asdict(step) for step in steps],
        },
    }

    print("===== REBUILD COMPLETE =====", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
