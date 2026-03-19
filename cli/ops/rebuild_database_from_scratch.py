#!/usr/bin/env python3
from __future__ import annotations

"""
Production-grade rebuild orchestrator for stock-quant-oop.

Objectif
--------
Reconstruire la base DuckDB depuis zéro avec une orchestration robuste,
observable et cohérente avec les autres wrappers récents du projet.

Principes de design
-------------------
- Python mince pour l'orchestration
- SQL-first dans les scripts enfants
- preflight explicite avant de lancer les longues étapes
- propagation cohérente des paramètres runtime:
  - --verbose
  - --memory-limit
  - --threads
  - --temp-dir
- résumé JSON final exploitable dans les logs
- aucune activation implicite d'ADR
- beaucoup de commentaires pour aider les autres développeurs

Important
---------
Ce wrapper ne remplace pas la logique métier des scripts enfants.
Il les orchestre dans un ordre déterministe, avec validation de surface,
journaling lisible, et arrêt immédiat en cas d'échec.

Compatibilité
-------------
Le but est de rester compatible avec l'organisation actuelle du repo:
- cli/core/*
- cli/raw/*
- cli/ops/*

On évite d'imposer des flags à des scripts qui ne les supportent pas.
Pour cela, on inspecte dynamiquement l'aide `-h` de chaque script avant de
propager les arguments optionnels.
"""

import argparse
import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ============================================================================
# Utilitaires temps / JSON
# ============================================================================


def _now_utc() -> datetime:
    """Retourne un timestamp UTC timezone-aware."""
    return datetime.now(timezone.utc)


def _iso_now() -> str:
    """Timestamp ISO pratique pour les résumés JSON."""
    return _now_utc().isoformat()


def _json_default(value: Any) -> Any:
    """
    Serializer JSON minimaliste.

    Pourquoi:
    - Path n'est pas sérialisable nativement
    - datetime non plus
    """
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


# ============================================================================
# Structures de données simples pour garder le code lisible
# ============================================================================


@dataclass
class StepResult:
    """
    Résultat structuré d'une étape enfant.

    On garde aussi stdout/stderr pour le diagnostic en cas d'échec
    ou pour un futur enrichissement.
    """

    step_name: str
    command: list[str]
    started_at: str
    finished_at: str
    duration_seconds: float
    returncode: int
    stdout: str
    stderr: str
    skipped: bool = False
    reason: str | None = None


@dataclass
class RebuildSummary:
    """
    Résumé final du rebuild.

    Ce résumé est imprimé en JSON à la fin pour être facilement indexable
    dans des logs ou dans des probes automatisés.
    """

    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    project_root: Path
    db_path: Path
    steps: list[dict[str, Any]] = field(default_factory=list)
    runtime: dict[str, Any] = field(default_factory=dict)
    inputs: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


# ============================================================================
# Parsing CLI
# ============================================================================


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the market database from scratch with preflight checks and runtime passthrough."
    )

    # ----------------------------------------------------------------------
    # Contexte principal
    # ----------------------------------------------------------------------
    parser.add_argument(
        "--project-root",
        default="/home/marty/stock-quant-oop",
        help="Root of the stock-quant-oop project.",
    )
    parser.add_argument(
        "--db-path",
        default="/home/marty/stock-quant-oop/market.duckdb",
        help="DuckDB database path.",
    )
    parser.add_argument(
        "--python-bin",
        default=sys.executable,
        help="Python interpreter to use for child commands.",
    )

    # ----------------------------------------------------------------------
    # Runtime passthrough
    # ----------------------------------------------------------------------
    parser.add_argument(
        "--memory-limit",
        default="24GB",
        help="DuckDB memory limit propagated to child scripts when supported.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=6,
        help="DuckDB thread count propagated to child scripts when supported.",
    )
    parser.add_argument(
        "--temp-dir",
        default="/home/marty/stock-quant-oop/tmp",
        help="DuckDB temp spill directory propagated to child scripts when supported.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose child command output when supported.",
    )

    # ----------------------------------------------------------------------
    # Prix / Stooq
    # ----------------------------------------------------------------------
    parser.add_argument(
        "--stooq-source",
        action="append",
        default=[],
        help=(
            "Historical Stooq source directory or file. "
            "Can be repeated. Mandatory unless --skip-price-backfill is used."
        ),
    )
    parser.add_argument(
        "--price-symbol",
        action="append",
        default=[],
        help="Optional symbol filter forwarded to compatible price scripts. Repeatable.",
    )
    parser.add_argument(
        "--price-start-date",
        default=None,
        help="Optional start date forwarded to compatible price scripts.",
    )
    parser.add_argument(
        "--price-end-date",
        default=None,
        help="Optional end date forwarded to compatible price scripts.",
    )

    # ----------------------------------------------------------------------
    # FINRA
    # ----------------------------------------------------------------------
    parser.add_argument(
        "--finra-output-dir",
        default="/home/marty/stock-quant-oop/data/raw/finra",
        help="Directory used by FINRA refresh workflow.",
    )
    parser.add_argument(
        "--finra-source-market",
        default="us",
        help="Source market passed to FINRA normalized build.",
    )
    parser.add_argument(
        "--finra-start-date",
        default=None,
        help="Optional FINRA start date.",
    )
    parser.add_argument(
        "--finra-end-date",
        default=None,
        help="Optional FINRA end date.",
    )
    parser.add_argument(
        "--finra-overwrite-downloads",
        action="store_true",
        help="Overwrite existing local FINRA download files.",
    )
    parser.add_argument(
        "--finra-truncate-raw",
        action="store_true",
        help="Delete existing FINRA raw table before load when supported by child workflow.",
    )

    # ----------------------------------------------------------------------
    # Univers / ADR
    # ----------------------------------------------------------------------
    parser.add_argument(
        "--allow-adr",
        action="store_true",
        default=False,
        help="Allow ADR in market universe build. Default is ADR-free.",
    )

    # ----------------------------------------------------------------------
    # Skip flags
    # ----------------------------------------------------------------------
    parser.add_argument("--skip-init-db", action="store_true", help="Skip init_market_db.")
    parser.add_argument("--skip-symbol-source-fetch", action="store_true", help="Skip SEC/Nasdaq raw symbol fetch.")
    parser.add_argument("--skip-symbol-reference-load", action="store_true", help="Skip symbol raw load.")
    parser.add_argument("--skip-market-universe", action="store_true", help="Skip build_market_universe.")
    parser.add_argument("--skip-symbol-reference", action="store_true", help="Skip build_symbol_reference.")
    parser.add_argument("--skip-sec-filings", action="store_true", help="Skip build_sec_filings.")
    parser.add_argument("--skip-sec-facts", action="store_true", help="Skip build_sec_fact_normalized.")
    parser.add_argument("--skip-fundamentals", action="store_true", help="Skip build_fundamentals.")
    parser.add_argument("--skip-price-backfill", action="store_true", help="Skip historical price backfill.")
    parser.add_argument("--skip-price-daily", action="store_true", help="Skip daily price refresh.")
    parser.add_argument("--skip-finra", action="store_true", help="Skip FINRA refresh workflow.")

    return parser.parse_args()


# ============================================================================
# Introspection des scripts enfants
# ============================================================================


def _script_exists(script_path: Path) -> bool:
    """Petit helper lisible pour le preflight."""
    return script_path.exists() and script_path.is_file()


def _get_help_text(python_bin: str, script_path: Path) -> str:
    """
    Retourne l'aide d'un script enfant.

    Pourquoi:
    - on veut propager les flags seulement s'ils sont supportés
    - cela évite les erreurs du type 'unrecognized arguments: --verbose'
    """
    completed = subprocess.run(
        [python_bin, str(script_path), "-h"],
        text=True,
        capture_output=True,
    )
    return (completed.stdout or "") + "\n" + (completed.stderr or "")


def _supports_flag(help_text: str, flag_name: str) -> bool:
    """Détection simple et suffisante pour nos wrappers."""
    return flag_name in help_text


# ============================================================================
# Construction de commandes
# ============================================================================


def _base_python_cmd(python_bin: str, script_path: Path) -> list[str]:
    """Commande de base pour un script enfant."""
    return [python_bin, str(script_path)]


def _append_if_supported(
    cmd: list[str],
    help_text: str,
    flag_name: str,
    value: str | int | None = None,
    enable_bool: bool = True,
) -> list[str]:
    """
    Ajoute un flag seulement si le script enfant le supporte.

    Cas gérés:
    - booléen: --verbose
    - option avec valeur: --memory-limit 24GB
    """
    if not _supports_flag(help_text, flag_name):
        return cmd

    if value is None:
        if enable_bool:
            cmd.append(flag_name)
        return cmd

    cmd.extend([flag_name, str(value)])
    return cmd


def _build_runtime_passthrough(
    cmd: list[str],
    help_text: str,
    args: argparse.Namespace,
) -> list[str]:
    """
    Propagation cohérente des paramètres runtime communs.

    Très important:
    - ne jamais pousser un flag non supporté
    - cela rend le wrapper stable même si tous les scripts du repo
      ne sont pas encore alignés
    """
    cmd = _append_if_supported(cmd, help_text, "--memory-limit", args.memory_limit)
    cmd = _append_if_supported(cmd, help_text, "--threads", args.threads)
    cmd = _append_if_supported(cmd, help_text, "--temp-dir", args.temp_dir)
    cmd = _append_if_supported(cmd, help_text, "--verbose", enable_bool=bool(args.verbose))
    return cmd


def _extend_with_optional_price_filters(
    cmd: list[str],
    help_text: str,
    args: argparse.Namespace,
) -> list[str]:
    """
    Ajoute des filtres prix optionnels seulement si le script enfant les supporte.
    """
    if args.price_start_date and _supports_flag(help_text, "--start-date"):
        cmd.extend(["--start-date", str(args.price_start_date)])
    if args.price_end_date and _supports_flag(help_text, "--end-date"):
        cmd.extend(["--end-date", str(args.price_end_date)])

    for symbol in args.price_symbol:
        if _supports_flag(help_text, "--symbol"):
            cmd.extend(["--symbol", str(symbol)])

    for source in args.stooq_source:
        # Certains scripts pourraient supporter --stooq-source, d'autres --source.
        if _supports_flag(help_text, "--stooq-source"):
            cmd.extend(["--stooq-source", str(source)])
        elif _supports_flag(help_text, "--source"):
            cmd.extend(["--source", str(source)])

    return cmd


# ============================================================================
# Exécution
# ============================================================================


def _run_step(step_name: str, command: list[str]) -> StepResult:
    """
    Exécute une étape avec sortie streamée vers le terminal.

    Important:
    - pas de capture muette
    - l'utilisateur voit la progression des scripts enfants
    - on garde néanmoins stdout/stderr en mémoire pour le résumé
    """
    started_at = _iso_now()
    t0 = time.perf_counter()

    print(f"===== STEP: {step_name} =====", flush=True)
    print("COMMAND:", " ".join(shlex.quote(part) for part in command), flush=True)

    process = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )

    streamed_lines: list[str] = []

    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        streamed_lines.append(line)

    returncode = process.wait()

    finished_at = _iso_now()
    duration_seconds = round(time.perf_counter() - t0, 6)
    stdout = "".join(streamed_lines)

    if returncode != 0:
        raise SystemExit(
            f"step failed: {step_name} (exit_code={returncode})"
        )

    return StepResult(
        step_name=step_name,
        command=command,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        returncode=returncode,
        stdout=stdout,
        stderr="",
    )


def _skip_step(step_name: str, reason: str) -> StepResult:
    """Fabrique un résultat de skip homogène."""
    print(f"===== STEP: {step_name} =====", flush=True)
    print(f"SKIPPED: {reason}", flush=True)

    now = _iso_now()
    return StepResult(
        step_name=step_name,
        command=[],
        started_at=now,
        finished_at=now,
        duration_seconds=0.0,
        returncode=0,
        stdout="",
        stderr="",
        skipped=True,
        reason=reason,
    )


# ============================================================================
# Preflight
# ============================================================================


def _preflight(
    args: argparse.Namespace,
    project_root: Path,
    db_path: Path,
    python_bin: str,
) -> dict[str, Any]:
    """
    Vérifie les prérequis de surface avant de lancer le rebuild.

    On ne fait pas d'assumptions silencieuses.
    """
    cli_core = project_root / "cli" / "core"
    cli_raw = project_root / "cli" / "raw"
    cli_ops = project_root / "cli" / "ops"

    required_scripts = {
        "init_market_db": cli_core / "init_market_db.py",
        "fetch_sec_company_tickers_raw": cli_raw / "fetch_sec_company_tickers_raw.py",
        "fetch_nasdaq_symbol_directory_raw": cli_raw / "fetch_nasdaq_symbol_directory_raw.py",
        "load_symbol_reference_source_raw": cli_raw / "load_symbol_reference_source_raw.py",
        "build_market_universe": cli_core / "build_market_universe.py",
        "build_symbol_reference": cli_core / "build_symbol_reference.py",
        "build_sec_filings": cli_core / "build_sec_filings.py",
        "build_sec_fact_normalized": cli_core / "build_sec_fact_normalized.py",
        "build_fundamentals": cli_core / "build_fundamentals.py",
        "build_prices": cli_core / "build_prices.py",
        "run_price_daily_refresh": cli_ops / "run_price_daily_refresh.py",
        "run_finra_daily_refresh": cli_ops / "run_finra_daily_refresh.py",
    }

    script_presence = {
        name: _script_exists(path)
        for name, path in required_scripts.items()
    }

    missing_scripts = [
        str(required_scripts[name])
        for name, present in script_presence.items()
        if not present
    ]

    stooq_sources = [Path(p).expanduser().resolve() for p in args.stooq_source]
    stooq_probe = []
    for source in stooq_sources:
        stooq_probe.append(
            {
                "path": str(source),
                "exists": source.exists(),
                "is_dir": source.is_dir(),
                "is_file": source.is_file(),
            }
        )

    invalid_stooq_sources = [
        probe["path"]
        for probe in stooq_probe
        if not probe["exists"]
    ]

    if not args.skip_price_backfill and not stooq_sources:
        raise SystemExit(
            "historical Stooq price sources are required for rebuild; "
            "pass at least one --stooq-source or explicitly use --skip-price-backfill"
        )

    if invalid_stooq_sources:
        raise SystemExit(
            "missing Stooq sources: " + ", ".join(invalid_stooq_sources)
        )

    if missing_scripts:
        raise SystemExit(
            "missing required child scripts: " + ", ".join(missing_scripts)
        )

    temp_dir = Path(args.temp_dir).expanduser().resolve()
    temp_dir.mkdir(parents=True, exist_ok=True)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    preflight = {
        "python_bin": python_bin,
        "project_root": str(project_root),
        "db_path": str(db_path),
        "temp_dir": str(temp_dir),
        "script_presence": script_presence,
        "stooq_sources": stooq_probe,
    }

    print("===== PREFLIGHT =====", flush=True)
    print(json.dumps(preflight, indent=2, sort_keys=True), flush=True)

    return preflight


# ============================================================================
# Main orchestration
# ============================================================================


def main() -> int:
    started_at = _iso_now()
    t0 = time.perf_counter()

    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    python_bin = str(Path(args.python_bin).expanduser().resolve())

    preflight = _preflight(
        args=args,
        project_root=project_root,
        db_path=db_path,
        python_bin=python_bin,
    )

    steps: list[StepResult] = []

    # ------------------------------------------------------------------
    # Helpers locaux pour réduire la répétition
    # ------------------------------------------------------------------
    def script(rel_path: str) -> Path:
        return project_root / rel_path

    def build_child_cmd(rel_path: str) -> tuple[list[str], str]:
        script_path = script(rel_path)
        help_text = _get_help_text(python_bin, script_path)
        cmd = _base_python_cmd(python_bin, script_path)
        cmd = _build_runtime_passthrough(cmd, help_text, args)
        return cmd, help_text

    # ------------------------------------------------------------------
    # 1) INIT DB
    # ------------------------------------------------------------------
    if args.skip_init_db:
        steps.append(_skip_step("INIT MARKET DB", "requested by --skip-init-db"))
    else:
        cmd, _ = build_child_cmd("cli/core/init_market_db.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("INIT MARKET DB", cmd))

    # ------------------------------------------------------------------
    # 2) RAW SYMBOL SOURCE FETCH
    # ------------------------------------------------------------------
    if args.skip_symbol_source_fetch:
        steps.append(_skip_step("FETCH SEC COMPANY TICKERS RAW", "requested by --skip-symbol-source-fetch"))
        steps.append(_skip_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", "requested by --skip-symbol-source-fetch"))
    else:
        cmd, _ = build_child_cmd("cli/raw/fetch_sec_company_tickers_raw.py")
        steps.append(_run_step("FETCH SEC COMPANY TICKERS RAW", cmd))

        cmd, _ = build_child_cmd("cli/raw/fetch_nasdaq_symbol_directory_raw.py")
        steps.append(_run_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", cmd))

    # ------------------------------------------------------------------
    # 3) LOAD SYMBOL REFERENCE RAW
    # ------------------------------------------------------------------
    if args.skip_symbol_reference_load:
        steps.append(_skip_step("LOAD SYMBOL REFERENCE SOURCE RAW", "requested by --skip-symbol-reference-load"))
    else:
        cmd, _ = build_child_cmd("cli/raw/load_symbol_reference_source_raw.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("LOAD SYMBOL REFERENCE SOURCE RAW", cmd))

    # ------------------------------------------------------------------
    # 4) BUILD MARKET UNIVERSE
    # ------------------------------------------------------------------
    if args.skip_market_universe:
        steps.append(_skip_step("BUILD MARKET UNIVERSE", "requested by --skip-market-universe"))
    else:
        cmd, help_text = build_child_cmd("cli/core/build_market_universe.py")
        cmd.extend(["--db-path", str(db_path)])

        # Correction importante:
        # --allow-adr ne doit être propagé QUE si explicitement demandé.
        if args.allow_adr and _supports_flag(help_text, "--allow-adr"):
            cmd.append("--allow-adr")

        steps.append(_run_step("BUILD MARKET UNIVERSE", cmd))

    # ------------------------------------------------------------------
    # 5) BUILD SYMBOL REFERENCE
    # ------------------------------------------------------------------
    if args.skip_symbol_reference:
        steps.append(_skip_step("BUILD SYMBOL REFERENCE", "requested by --skip-symbol-reference"))
    else:
        cmd, _ = build_child_cmd("cli/core/build_symbol_reference.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("BUILD SYMBOL REFERENCE", cmd))

    # ------------------------------------------------------------------
    # 6) SEC FILINGS
    # ------------------------------------------------------------------
    if args.skip_sec_filings:
        steps.append(_skip_step("BUILD SEC FILINGS", "requested by --skip-sec-filings"))
    else:
        cmd, _ = build_child_cmd("cli/core/build_sec_filings.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("BUILD SEC FILINGS", cmd))

    # ------------------------------------------------------------------
    # 7) SEC FACTS NORMALIZED
    # ------------------------------------------------------------------
    if args.skip_sec_facts:
        steps.append(_skip_step("BUILD SEC FACT NORMALIZED", "requested by --skip-sec-facts"))
    else:
        cmd, _ = build_child_cmd("cli/core/build_sec_fact_normalized.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("BUILD SEC FACT NORMALIZED", cmd))

    # ------------------------------------------------------------------
    # 8) FUNDAMENTALS
    # ------------------------------------------------------------------
    if args.skip_fundamentals:
        steps.append(_skip_step("BUILD FUNDAMENTALS", "requested by --skip-fundamentals"))
    else:
        cmd, _ = build_child_cmd("cli/core/build_fundamentals.py")
        cmd.extend(["--db-path", str(db_path)])
        steps.append(_run_step("BUILD FUNDAMENTALS", cmd))

    # ------------------------------------------------------------------
    # 9) PRICE BACKFILL
    # ------------------------------------------------------------------
    if args.skip_price_backfill:
        steps.append(_skip_step("BUILD PRICES BACKFILL", "requested by --skip-price-backfill"))
    else:
        cmd, help_text = build_child_cmd("cli/core/build_prices.py")
        cmd.extend([
            "--db-path", str(db_path),
            "--mode", "backfill",
        ])
        cmd = _extend_with_optional_price_filters(cmd, help_text, args)
        steps.append(_run_step("BUILD PRICES BACKFILL", cmd))

    # ------------------------------------------------------------------
    # 10) DAILY PRICE REFRESH
    # ------------------------------------------------------------------
    if args.skip_price_daily:
        steps.append(_skip_step("RUN PRICE DAILY REFRESH", "requested by --skip-price-daily"))
    else:
        cmd, help_text = build_child_cmd("cli/ops/run_price_daily_refresh.py")
        cmd.extend([
            "--project-root", str(project_root),
            "--db-path", str(db_path),
        ])
        cmd = _extend_with_optional_price_filters(cmd, help_text, args)
        steps.append(_run_step("RUN PRICE DAILY REFRESH", cmd))

    # ------------------------------------------------------------------
    # 11) FINRA DAILY REFRESH
    # ------------------------------------------------------------------
    if args.skip_finra:
        steps.append(_skip_step("RUN FINRA DAILY REFRESH", "requested by --skip-finra"))
    else:
        cmd, help_text = build_child_cmd("cli/ops/run_finra_daily_refresh.py")
        cmd.extend([
            "--project-root", str(project_root),
            "--db-path", str(db_path),
            "--output-dir", str(Path(args.finra_output_dir).expanduser().resolve()),
            "--source-market", str(args.finra_source_market),
        ])

        if args.finra_start_date and _supports_flag(help_text, "--start-date"):
            cmd.extend(["--start-date", str(args.finra_start_date)])
        if args.finra_end_date and _supports_flag(help_text, "--end-date"):
            cmd.extend(["--end-date", str(args.finra_end_date)])
        if args.finra_overwrite_downloads and _supports_flag(help_text, "--overwrite-downloads"):
            cmd.append("--overwrite-downloads")
        if args.finra_truncate_raw and _supports_flag(help_text, "--truncate-raw"):
            cmd.append("--truncate-raw")

        steps.append(_run_step("RUN FINRA DAILY REFRESH", cmd))

    # ------------------------------------------------------------------
    # Résumé final
    # ------------------------------------------------------------------
    finished_at = _iso_now()
    duration_seconds = round(time.perf_counter() - t0, 6)

    summary = RebuildSummary(
        status="success",
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        project_root=project_root,
        db_path=db_path,
        runtime={
            "python_bin": python_bin,
            "memory_limit": args.memory_limit,
            "threads": args.threads,
            "temp_dir": str(Path(args.temp_dir).expanduser().resolve()),
            "verbose": bool(args.verbose),
        },
        inputs={
            "allow_adr": bool(args.allow_adr),
            "stooq_source_count": len(args.stooq_source),
            "stooq_sources": [str(Path(p).expanduser().resolve()) for p in args.stooq_source],
            "price_symbol_count": len(args.price_symbol),
            "price_symbols": list(args.price_symbol),
            "price_start_date": args.price_start_date,
            "price_end_date": args.price_end_date,
            "finra_output_dir": str(Path(args.finra_output_dir).expanduser().resolve()),
            "finra_source_market": args.finra_source_market,
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
        notes=[
            "ADR are no longer forced implicitly in market universe build.",
            "Runtime flags are propagated only when child scripts support them.",
            "Preflight validates required child scripts and declared Stooq sources.",
        ],
    )

    summary.steps = [
        {
            "step_name": step.step_name,
            "skipped": step.skipped,
            "reason": step.reason,
            "returncode": step.returncode,
            "started_at": step.started_at,
            "finished_at": step.finished_at,
            "duration_seconds": step.duration_seconds,
            "command": step.command,
        }
        for step in steps
    ]

    print("===== REBUILD COMPLETE =====", flush=True)
    print(
        json.dumps(
            {
                "preflight": preflight,
                "summary": summary.__dict__,
            },
            indent=2,
            default=_json_default,
            sort_keys=True,
        ),
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
