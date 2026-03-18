#!/usr/bin/env python3
from __future__ import annotations

"""
Orchestrateur quotidien principal.

Objectif
--------
Lancer, en séquence et avec logs dédiés :
- mise à jour des prix
- FINRA daily short volume
- FINRA short interest
- short features
- SEC filings

Contraintes de conception
-------------------------
- séquentiel pour éviter les locks DuckDB
- sortie live conservée pour voir tqdm
- un log par étape dans logs/
- fail-fast : on arrête au premier échec
- beaucoup de commentaires pour faciliter la maintenance
"""

import subprocess
import sys
from datetime import datetime
from pathlib import Path


# Racine du projet.
PROJECT_ROOT = Path("/home/marty/stock-quant-oop")

# Base DuckDB canonique du projet.
DB_PATH = PROJECT_ROOT / "market.duckdb"

# Répertoire de logs du repo.
LOG_DIR = PROJECT_ROOT / "logs"


def utc_stamp() -> str:
    """
    Retourne un timestamp UTC compact pour nommer les logs.
    """
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def run_step(step_name: str, cmd: list[str]) -> None:
    """
    Exécute une étape en streaming stdout/stderr.

    Détails importants :
    - on garde stdout/stderr fusionnés pour conserver l'ordre exact
    - on affiche en live pour garder tqdm visible
    - on écrit simultanément dans un log dédié
    - on stoppe tout si une étape échoue
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_path = LOG_DIR / f"{step_name}_{utc_stamp()}.log"

    print(f"\n===== RUN STEP: {step_name} =====", flush=True)
    print("cmd =", " ".join(cmd), flush=True)
    print("log =", str(log_path), flush=True)

    with log_path.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None

        # Stream live + écriture log.
        for line in process.stdout:
            print(line, end="", flush=True)
            handle.write(line)

        return_code = process.wait()

    if return_code != 0:
        print(f"\n❌ STEP FAILED: {step_name} (exit={return_code})", flush=True)
        raise SystemExit(return_code)

    print(f"✅ STEP OK: {step_name}", flush=True)


def main() -> int:
    """
    Point d'entrée principal.

    Remarque importante :
    `build_prices.py` ne supporte pas `--mode daily` dans l'état actuel du repo.
    On appelle donc le CLI avec seulement `--db-path`, afin qu'il applique
    sa logique quotidienne interne déjà implémentée.
    """
    print("===== DAILY PIPELINE START =====", flush=True)
    print("db_path =", str(DB_PATH), flush=True)

    # 1) Prix quotidiens.
    run_step(
        "build_prices",
        [
            "python3",
            "cli/core/build_prices.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    # 2) FINRA daily short volume canonique.
    run_step(
        "build_finra_daily_short_volume",
        [
            "python3",
            "cli/core/build_finra_daily_short_volume.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    # 3) FINRA short interest canonique.
    run_step(
        "build_finra_short_interest",
        [
            "python3",
            "cli/core/build_finra_short_interest.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    # 4) Short features.
    # On garde les paramètres qui ont déjà fonctionné chez toi pour éviter l'OOM.
    run_step(
        "build_short_features",
        [
            "python3",
            "cli/core/build_short_features.py",
            "--db-path",
            str(DB_PATH),
            "--duckdb-threads",
            "2",
            "--duckdb-memory-limit",
            "36GB",
        ],
    )

    # 5) SEC filings.
    run_step(
        "build_sec_filings",
        [
            "python3",
            "cli/core/build_sec_filings.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    print("\n===== DAILY PIPELINE SUCCESS =====", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
