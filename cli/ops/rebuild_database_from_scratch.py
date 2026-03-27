#!/usr/bin/env python3
from __future__ import annotations

"""
rebuild_database_from_scratch.py

Réécriture complète de l'orchestrateur de rebuild.

Objectifs:
- garder la même intention produit que le script original
- supprimer les chemins cassés découverts pendant l'audit
- utiliser le chemin prix réellement valide:
    1) Stooq -> price_source_daily_raw_all
    2) run_core_pipeline.py -> price_source_daily_raw + price_history
- conserver les autres étapes utiles du rebuild
- produire un résumé JSON clair
- ne pas injecter de fixtures
- rester lisible et abondamment commenté
"""

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild stock-quant DB using real raw data and canonical rebuild steps."
    )

    parser.add_argument("--project-root", default="~/stock-quant-oop")
    parser.add_argument("--db-path", default="~/stock-quant-oop-runtime/db/market.duckdb")
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--memory-limit", default="24GB")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--temp-dir", default="~/stock-quant-oop-runtime/tmp")
    parser.add_argument("--verbose", action="store_true")

    # Prix
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

    # Universe
    parser.add_argument("--allow-adr", action="store_true")

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

    return parser.parse_args()


def resolve_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()


def discover_symbol_source_files(project_root: Path) -> list[Path]:
    """
    Découvre les fichiers réellement téléchargés par:
    - fetch_sec_company_tickers_raw.py
    - fetch_nasdaq_symbol_directory_raw.py

    Après audit, on sait que ces scripts écrivent dans:
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

    files = sorted([path.resolve() for path in files if path.is_file()])
    return files


def build_child_path(project_root: Path, relative_script: str) -> Path:
    return (project_root / relative_script).resolve()


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
        print(
            f"step failed: {step_name} (exit_code={completed.returncode})",
            flush=True,
        )
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


def main() -> int:
    args = parse_args()

    project_root = resolve_path(args.project_root)
    db_path = resolve_path(args.db_path)
    temp_dir = resolve_path(args.temp_dir)
    python_bin = str(resolve_path(args.python_bin))
    finra_output_dir = resolve_path(args.finra_output_dir)
    stooq_sources = [resolve_path(item) for item in args.stooq_source]

    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

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
        raise SystemExit(
            "missing --stooq-source for price backfill"
        )

    steps: list[StepResult] = []
    started_at = datetime.now(timezone.utc)

    # 1) INIT DB
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

    # 2) FETCH SYMBOL SOURCES
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

    # 3) LOAD SYMBOL REFERENCE SOURCE RAW
    if args.skip_symbol_reference_load:
        steps.append(skip_step("LOAD SYMBOL REFERENCE SOURCE RAW", "requested by --skip-symbol-reference-load"))
    else:
        symbol_sources = discover_symbol_source_files(project_root)
        if not symbol_sources:
            raise SystemExit(
                f"no symbol source files found under {project_root / 'data' / 'symbol_sources'}"
            )

        cmd = [
            python_bin,
            str(build_child_path(project_root, "cli/raw/load_symbol_reference_source_raw.py")),
            "--db-path",
            str(db_path),
        ]
        for source in symbol_sources:
            cmd.extend(["--source", str(source)])

        steps.append(run_step("LOAD SYMBOL REFERENCE SOURCE RAW", cmd))

    # 4) BUILD MARKET UNIVERSE
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

    # 5) BUILD SYMBOL REFERENCE
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

    # 6) SEC FILINGS
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

    # 7) SEC FACTS
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

    # 8) FUNDAMENTALS
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

    # 9) PRICE BACKFILL
    #
    # Correctif principal:
    # - on ne passe plus par build_prices.py pour l'historique Stooq
    # - on charge Stooq en bronze
    # - puis on reconstruit price_source_daily_raw + price_history
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

    # 10) PRICE DAILY REFRESH
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
        steps.append(run_step("RUN PRICE DAILY REFRESH", cmd))

    # 11) FINRA
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
        steps.append(run_step("RUN FINRA DAILY REFRESH", cmd))

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
                "skip_flags": {
                    "skip_init_db": bool(args.skip_init_db),
                    "skip_symbol_source_fetch": bool(args.skip_symbol_source_fetch),
                    "skip_symbol_reference_load": bool(args.skip_symbol_reference_load),
                    "skip_market_universe": bool(args.skip_market_universe),
                    "skip_symbol_reference": bool(args.skip_symbol_reference),
                    "skip_sec_filings": bool(args.skip_sec_filings),
                    "skip_sec_facts": bool(args.skip_sec_facts),
                    "skip_fundamentals": bool(args.skip_fundamentals),
                    "skip_price_backfill": bool(args.skip_price_backfill),
                    "skip_price_daily": bool(args.skip_price_daily),
                    "skip_finra": bool(args.skip_finra),
                },
            },
            "notes": [
                "Price backfill now uses Stooq bronze loader + canonical run_core_pipeline.",
                "No historical price rebuild through build_prices.py.",
                "Symbol source files are discovered from data/symbol_sources.",
            ],
            "steps": [asdict(step) for step in steps],
        },
    }

    print("===== REBUILD COMPLETE =====", flush=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
