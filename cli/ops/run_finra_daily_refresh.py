#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_finra_daily_refresh.py
#
# Objectif
# --------
# Orchestrer le refresh FINRA "journalier" en plusieurs sous-étapes:
#
#   1) télécharger / compléter les fichiers FINRA short interest sur disque
#   2) charger les fichiers raw disque dans la table raw
#   3) construire les tables normalisées dérivées
#   4) afficher un status pipeline final
#
# Pourquoi ce script existe
# -------------------------
# On centralise ici la logique d'orchestration shell/subprocess pour éviter
# de la dupliquer dans plusieurs scripts. La logique métier reste dans les
# scripts appelés. Ici, on ne fait que:
#   - parser les arguments
#   - construire les commandes enfants
#   - les exécuter dans le bon ordre
#   - échouer explicitement si une étape échoue
#
# Notes importantes
# -----------------
# - Python reste volontairement mince ici: c'est de l'orchestration subprocess.
# - Les fichiers FINRA sont maintenant stockés sous:
#       ~/stock-quant-oop/data/raw/finra/short_interest
#   et non plus dans l'ancien chemin:
#       ~/stock-quant-oop/data/raw/finra_short_interest
# - Ce script ne fait aucun fallback fixture.
# - Ce script est prod-only.
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


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Remarques:
    - --output-dir est le dossier disque où vivent les CSV FINRA short interest.
    - --truncate-raw ne vide que la table raw FINRA, jamais les fichiers disque.
    - --source-market est transmis à l'étape de build normalisé.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Run FINRA daily refresh: download recent files -> load raw -> "
            "build normalized -> status."
        )
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to child steps.",
    )
    parser.add_argument(
        "--output-dir",
        # IMPORTANT:
        # Nouveau chemin raw unifié sous data/raw/finra/short_interest.
        default="~/stock-quant-oop/data/raw/finra/short_interest",
        help="Directory where FINRA short-interest files are stored/downloaded.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Inclusive start date YYYY-MM-DD for FINRA download/load window.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Inclusive end date YYYY-MM-DD for FINRA download/load window.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market passed to build_finra_short_interest.",
    )
    parser.add_argument(
        "--overwrite-downloads",
        action="store_true",
        help="Overwrite existing local FINRA download files.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Delete existing finra_short_interest_source_raw before load.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def build_base_command(
    project_root: Path,
    script_rel: str,
    db_path: str | None,
    verbose: bool,
) -> list[str]:
    """
    Construit une commande de base pour lancer un script enfant.

    Pourquoi cette fonction existe:
    - éviter la duplication
    - garantir un format homogène des appels enfant
    - simplifier les futurs changements de convention
    """
    cmd = [sys.executable, str(project_root / script_rel)]

    if db_path:
        cmd.extend(["--db-path", db_path])

    if verbose:
        cmd.append("--verbose")

    return cmd


def run_step(name: str, cmd: list[str], project_root: Path) -> None:
    """
    Exécute une étape enfant et échoue immédiatement si le return code != 0.

    Choix assumé:
    - on ne masque jamais l'échec d'une étape
    - on garde l'orchestration déterministe
    """
    completed = subprocess.run(cmd, cwd=project_root, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {name} (exit={completed.returncode})")


def main() -> None:
    """
    Point d'entrée principal.

    Pipeline exécuté:
    1) download_finra_short_interest
    2) load_finra_short_interest_source_raw
    3) build_finra_short_interest
    4) pipeline_status
    """
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    output_dir = str(Path(args.output_dir).expanduser().resolve())

    # -------------------------------------------------------------------------
    # Liste ordonnée des étapes.
    # On la construit explicitement pour rendre le pipeline lisible et facile
    # à déboguer dans les logs.
    # -------------------------------------------------------------------------
    steps: list[tuple[str, list[str]]] = []

    # -------------------------------------------------------------------------
    # Étape 1: téléchargement / synchronisation des fichiers FINRA sur disque.
    #
    # Cette étape ne touche pas à la DB.
    # Elle garantit seulement que le dossier output_dir contient les fichiers
    # demandés sur l'intervalle de dates voulu.
    # -------------------------------------------------------------------------
    download_cmd = [
        sys.executable,
        str(project_root / "cli" / "core" / "download_finra_short_interest.py"),
        "--output-dir",
        output_dir,
    ]
    if args.start_date:
        download_cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        download_cmd.extend(["--end-date", args.end_date])
    if args.overwrite_downloads:
        download_cmd.append("--overwrite")
    if args.verbose:
        download_cmd.append("--verbose")
    steps.append(("download_finra_short_interest", download_cmd))

    # -------------------------------------------------------------------------
    # Étape 2: chargement raw en base à partir des fichiers présents sur disque.
    #
    # IMPORTANT:
    # - le loader reçoit le dossier réel output_dir
    # - on ne fait aucun fallback fixture
    # - si le dossier ne contient rien d'exploitable, le loader doit échouer
    # -------------------------------------------------------------------------
    load_cmd = build_base_command(
        project_root=project_root,
        script_rel="cli/raw/load_finra_short_interest_source_raw.py",
        db_path=args.db_path,
        verbose=args.verbose,
    )
    load_cmd.extend(["--source", output_dir])
    if args.start_date:
        load_cmd.extend(["--start-date", args.start_date])
    if args.end_date:
        load_cmd.extend(["--end-date", args.end_date])
    if args.truncate_raw:
        load_cmd.append("--truncate")
    steps.append(("load_finra_short_interest_source_raw", load_cmd))

    # -------------------------------------------------------------------------
    # Étape 3: construction des tables normalisées.
    #
    # La table raw est déjà alimentée à l'étape précédente.
    # Ici, on transforme / agrège / filtre selon les règles métier.
    # -------------------------------------------------------------------------
    build_cmd = build_base_command(
        project_root=project_root,
        script_rel="cli/core/build_finra_short_interest.py",
        db_path=args.db_path,
        verbose=args.verbose,
    )
    build_cmd.extend(["--source-market", args.source_market])
    steps.append(("build_finra_short_interest", build_cmd))

    # -------------------------------------------------------------------------
    # Étape 4: status final.
    #
    # On n'a pas besoin de verbose ici: ce script est déjà une sortie de statut.
    # -------------------------------------------------------------------------
    status_cmd = build_base_command(
        project_root=project_root,
        script_rel="cli/pipeline_status.py",
        db_path=args.db_path,
        verbose=False,
    )
    steps.append(("pipeline_status", status_cmd))

    # -------------------------------------------------------------------------
    # Logging de démarrage.
    # -------------------------------------------------------------------------
    print("===== RUN FINRA DAILY REFRESH START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {args.db_path or '(default)'}", flush=True)
    print(f"OUTPUT DIR: {output_dir}", flush=True)
    print(f"SOURCE MARKET: {args.source_market}", flush=True)
    print(f"START DATE: {args.start_date or '(default)'}", flush=True)
    print(f"END DATE: {args.end_date or '(default)'}", flush=True)

    # -------------------------------------------------------------------------
    # Exécution séquentielle.
    #
    # tqdm donne une progression propre en terminal.
    # Les prints avant chaque étape aident énormément en troubleshooting pour
    # retrouver exactement quelle commande a été lancée.
    # -------------------------------------------------------------------------
    for name, cmd in tqdm(
        steps,
        desc="finra daily refresh",
        unit="step",
        dynamic_ncols=True,
    ):
        print(f"\n===== RUN STEP: {name} =====", flush=True)
        print("COMMAND:", " ".join(cmd), flush=True)
        run_step(name, cmd, project_root)
        print(f"===== STEP OK: {name} =====", flush=True)

    print("\n===== RUN FINRA DAILY REFRESH DONE =====", flush=True)


if __name__ == "__main__":
    main()
