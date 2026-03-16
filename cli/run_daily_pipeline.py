#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_daily_pipeline.py
#
# Objectif
# --------
# Orchestrer le pipeline quotidien complet du projet stock-quant-oop.
#
# Ce script est l'entrée principale pour rafraîchir:
#
#   1) les prix de marché
#   2) les données FINRA short interest
#   3) les filings SEC + fondamentaux
#
# Ce script ne contient PAS de logique métier.
# Il agit uniquement comme orchestrateur subprocess.
#
# Approche
# --------
# Python reste volontairement mince ici.
# Chaque étape réelle du pipeline est implémentée dans un script dédié.
#
# Ce script:
#   - parse les arguments
#   - construit les commandes enfants
#   - exécute les étapes séquentiellement
#   - stop immédiatement en cas d'erreur
#
# Avantages
# ---------
# - pipeline lisible
# - orchestration centralisée
# - debug simple
# - aucun code métier dupliqué
#
# IMPORTANT
# ---------
# Ce pipeline est PROD ONLY:
#
#   - aucun fallback fixture
#   - aucune seed DB
#   - aucune donnée factice
#
# Toutes les données doivent provenir de:
#
#   data/raw/...
#
# =============================================================================

import argparse
import subprocess
import sys
from pathlib import Path

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    def tqdm(iterable, **kwargs):
        return iterable


# =============================================================================
# CLI ARGUMENTS
# =============================================================================

def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Remarques importantes:
    - db-path est propagé aux scripts enfants.
    - certaines étapes peuvent être désactivées (skip flags).
    """

    parser = argparse.ArgumentParser(
        description="Run unified daily market refresh pipeline."
    )

    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[1]),
        help="Project root directory."
    )

    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop/market.duckdb",
        help="DuckDB database path."
    )

    # -------------------------------------------------------------------------
    # Prix
    # -------------------------------------------------------------------------

    parser.add_argument("--symbol", action="append", dest="symbols", default=[])
    parser.add_argument("--as-of", default=None)

    parser.add_argument("--price-start-date", default=None)
    parser.add_argument("--price-end-date", default=None)

    parser.add_argument("--skip-prices", action="store_true")

    # -------------------------------------------------------------------------
    # FINRA
    # -------------------------------------------------------------------------

    parser.add_argument("--skip-finra", action="store_true")

    parser.add_argument("--finra-start-date", default=None)
    parser.add_argument("--finra-end-date", default=None)

    parser.add_argument(
        "--finra-output-dir",
        default="~/stock-quant-oop/data/raw/finra/short_interest",
        help="Directory for FINRA raw files."
    )

    parser.add_argument(
        "--finra-source-market",
        default="regular",
        choices=["regular", "otc", "both"],
    )

    parser.add_argument("--overwrite-finra-downloads", action="store_true")
    parser.add_argument("--truncate-finra-raw", action="store_true")

    # -------------------------------------------------------------------------
    # SEC
    # -------------------------------------------------------------------------

    parser.add_argument("--skip-sec", action="store_true")

    parser.add_argument(
        "--sec-source",
        action="append",
        dest="sec_sources",
        default=[]
    )

    parser.add_argument(
        "--skip-sec-load",
        action="store_true"
    )

    parser.add_argument("--verbose", action="store_true")

    return parser.parse_args()


# =============================================================================
# UTILITIES
# =============================================================================

def run_step(name: str, cmd: list[str], project_root: Path) -> None:
    """
    Exécute une commande enfant.

    Le pipeline échoue immédiatement si une étape échoue.
    """
    completed = subprocess.run(cmd, cwd=project_root, check=False)

    if completed.returncode != 0:
        raise SystemExit(
            f"Pipeline step failed: {name} (exit={completed.returncode})"
        )


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:

    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = str(Path(args.db_path).expanduser().resolve())

    steps: list[tuple[str, list[str]]] = []

    # =========================================================================
    # PRICES
    # =========================================================================

    if not args.skip_prices:

        cmd = [
            sys.executable,
            str(project_root / "cli/ops/run_price_daily_refresh.py"),
            "--project-root", str(project_root),
            "--db-path", db_path
        ]

        for symbol in args.symbols:
            cmd.extend(["--symbol", symbol])

        if args.as_of:
            cmd.extend(["--as-of", args.as_of])
        else:

            if args.price_start_date:
                cmd.extend(["--start-date", args.price_start_date])

            if args.price_end_date:
                cmd.extend(["--end-date", args.price_end_date])

        if args.verbose:
            cmd.append("--verbose")

        steps.append(("run_price_daily_refresh", cmd))

    # =========================================================================
    # FINRA
    # =========================================================================

    if not args.skip_finra:

        cmd = [
            sys.executable,
            str(project_root / "cli/ops/run_finra_daily_refresh.py"),
            "--project-root", str(project_root),
            "--db-path", db_path,
            "--output-dir",
            str(Path(args.finra_output_dir).expanduser().resolve()),
            "--source-market",
            args.finra_source_market
        ]

        if args.finra_start_date:
            cmd.extend(["--start-date", args.finra_start_date])

        if args.finra_end_date:
            cmd.extend(["--end-date", args.finra_end_date])

        if args.overwrite_finra_downloads:
            cmd.append("--overwrite-downloads")

        if args.truncate_finra_raw:
            cmd.append("--truncate-raw")

        if args.verbose:
            cmd.append("--verbose")

        steps.append(("run_finra_daily_refresh", cmd))

    # =========================================================================
    # SEC
    # =========================================================================

    if not args.skip_sec:

        cmd = [
            sys.executable,
            str(project_root / "cli/ops/run_sec_fundamentals_daily.py"),
            "--project-root", str(project_root),
            "--db-path", db_path
        ]

        for src in args.sec_sources:
            cmd.extend(["--sec-source", str(Path(src).expanduser().resolve())])

        if args.skip_sec_load:
            cmd.append("--skip-load")

        if args.verbose:
            cmd.append("--verbose")

        steps.append(("run_sec_fundamentals_daily", cmd))

    # =========================================================================
    # STATUS FINAL
    # =========================================================================

    status_cmd = [
        sys.executable,
        str(project_root / "cli/pipeline_status.py"),
        "--db-path", db_path
    ]

    steps.append(("pipeline_status", status_cmd))

    # =========================================================================
    # EXECUTION
    # =========================================================================

    print("===== DAILY PIPELINE START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {db_path}", flush=True)

    for name, cmd in tqdm(
        steps,
        desc="daily pipeline",
        unit="step",
        dynamic_ncols=True
    ):

        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)

        run_step(name, cmd, project_root)

        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== DAILY PIPELINE COMPLETE =====", flush=True)


if __name__ == "__main__":
    main()

