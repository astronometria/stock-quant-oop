#!/usr/bin/env python3
from __future__ import annotations

"""
Wrapper robuste pour le refresh FINRA.

But:
- rester compatible avec l'appel du rebuild wrapper:
    --project-root
    --db-path
    --output-dir
    --source-market
- accepter aussi des sources explicites:
    --short-interest-source
    --daily-short-volume-source
- autodétecter les fichiers FINRA dans le dossier output-dir si les sources
  explicites ne sont pas fournies
- appeler les loaders réels avec l'interface attendue
- garder une sortie terminal propre et lisible
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    On accepte volontairement plus d'arguments que le script historique
    pour rester compatible avec le wrapper de rebuild.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--project-root", default="~/stock-quant-oop")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--output-dir", default="~/stock-quant-oop-raw/data/raw/finra")
    parser.add_argument("--source-market", default="us")
    parser.add_argument("--python-bin", default=sys.executable)

    # Sources explicites optionnelles.
    parser.add_argument(
        "--short-interest-source",
        dest="short_interest_sources",
        action="append",
        default=[],
        help="Chemin explicite vers un ou plusieurs fichiers FINRA short interest.",
    )
    parser.add_argument(
        "--daily-short-volume-source",
        dest="daily_short_volume_sources",
        action="append",
        default=[],
        help="Chemin explicite vers un ou plusieurs fichiers FINRA daily short volume.",
    )

    return parser.parse_args()


def discover_short_interest_sources(finra_root: Path) -> list[Path]:
    """
    Détecte les fichiers short interest sous le root FINRA.

    On privilégie les sous-dossiers connus du repo:
    - short_interest/
    - tout fichier contenant short + interest dans son nom
    """
    candidates: list[Path] = []

    preferred_dirs = [
        finra_root / "short_interest",
        finra_root / "short-interest",
    ]

    for directory in preferred_dirs:
        if directory.exists():
            for path in directory.rglob("*"):
                if path.is_file():
                    candidates.append(path)

    # Fallback permissif si les dossiers standard ne sont pas présents.
    if not candidates and finra_root.exists():
        for path in finra_root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name.lower()
            if "short" in name and "interest" in name:
                candidates.append(path)

    # Déduplication stable.
    return sorted({path.resolve() for path in candidates})


def discover_daily_short_volume_sources(finra_root: Path) -> list[Path]:
    """
    Détecte les fichiers daily short volume sous le root FINRA.

    Dossiers habituels observés:
    - daily_short_sale_volume/
    - daily_short_volume/
    """
    candidates: list[Path] = []

    preferred_dirs = [
        finra_root / "daily_short_sale_volume",
        finra_root / "daily_short_volume",
    ]

    for directory in preferred_dirs:
        if directory.exists():
            for path in directory.rglob("*"):
                if path.is_file():
                    candidates.append(path)

    # Fallback permissif.
    if not candidates and finra_root.exists():
        for path in finra_root.rglob("*"):
            if not path.is_file():
                continue
            name = path.name.lower()
            if "short" in name and "volume" in name:
                candidates.append(path)

    return sorted({path.resolve() for path in candidates})


def build_short_interest_cmd(
    python_bin: Path,
    project_root: Path,
    db_path: Path,
    sources: list[Path],
) -> list[str]:
    """
    Construit la commande du loader short interest.
    """
    cmd = [
        str(python_bin),
        str(project_root / "cli/raw/load_finra_short_interest_source_raw.py"),
        "--db-path",
        str(db_path),
    ]
    for source in sources:
        cmd.extend(["--source", str(source)])
    return cmd


def build_daily_short_volume_cmd(
    python_bin: Path,
    project_root: Path,
    db_path: Path,
    sources: list[Path],
) -> list[str]:
    """
    Construit la commande du loader daily short volume.
    """
    cmd = [
        str(python_bin),
        str(project_root / "cli/raw/load_finra_daily_short_volume_raw.py"),
        "--db-path",
        str(db_path),
    ]
    for source in sources:
        cmd.extend(["--source", str(source)])
    return cmd


def run_step(step_name: str, cmd: list[str]) -> None:
    """
    Exécute une commande enfant et remonte une erreur claire si non-zéro.
    """
    print(f"===== RUN STEP: {step_name} =====")
    print("COMMAND:", " ".join(cmd))
    completed = subprocess.run(cmd)
    if completed.returncode != 0:
        raise SystemExit(f"Step failed: {step_name} (exit={completed.returncode})")


def main() -> None:
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    finra_root = Path(args.output_dir).expanduser().resolve()
    python_bin = Path(args.python_bin).expanduser()

    explicit_short_interest_sources = [
        Path(item).expanduser().resolve() for item in args.short_interest_sources
    ]
    explicit_daily_short_volume_sources = [
        Path(item).expanduser().resolve() for item in args.daily_short_volume_sources
    ]

    short_interest_sources = (
        explicit_short_interest_sources
        if explicit_short_interest_sources
        else discover_short_interest_sources(finra_root)
    )
    daily_short_volume_sources = (
        explicit_daily_short_volume_sources
        if explicit_daily_short_volume_sources
        else discover_daily_short_volume_sources(finra_root)
    )

    print("===== RUN FINRA DAILY REFRESH START =====")
    print(f"PROJECT ROOT: {project_root}")
    print(f"DB PATH: {db_path}")
    print(f"FINRA ROOT: {finra_root}")
    print(f"SOURCE MARKET: {args.source_market}")
    print(f"SHORT INTEREST SOURCES: {len(short_interest_sources)}")
    print(f"DAILY SHORT VOLUME SOURCES: {len(daily_short_volume_sources)}")
    print()

    # Si rien n'est trouvé, on sort proprement avec succès pour éviter
    # un crash trompeur du wrapper global. Le résumé indiquera le noop.
    if not short_interest_sources and not daily_short_volume_sources:
        summary = {
            "status": "SUCCESS",
            "mode": "noop",
            "reason": "no_finra_sources_found",
            "finished_at": datetime.utcnow().isoformat(),
            "project_root": str(project_root),
            "db_path": str(db_path),
            "finra_root": str(finra_root),
            "source_market": args.source_market,
            "short_interest_source_count": 0,
            "daily_short_volume_source_count": 0,
        }
        print(json.dumps(summary, indent=2))
        return

    steps: list[tuple[str, list[str]]] = []

    if short_interest_sources:
        steps.append(
            (
                "load_finra_short_interest_source_raw",
                build_short_interest_cmd(
                    python_bin=python_bin,
                    project_root=project_root,
                    db_path=db_path,
                    sources=short_interest_sources,
                ),
            )
        )

    if daily_short_volume_sources:
        steps.append(
            (
                "load_finra_daily_short_volume_raw",
                build_daily_short_volume_cmd(
                    python_bin=python_bin,
                    project_root=project_root,
                    db_path=db_path,
                    sources=daily_short_volume_sources,
                ),
            )
        )

    for step_name, cmd in tqdm(steps, desc="finra daily refresh", unit="step"):
        run_step(step_name, cmd)

    summary = {
        "status": "SUCCESS",
        "mode": "load_sources",
        "finished_at": datetime.utcnow().isoformat(),
        "project_root": str(project_root),
        "db_path": str(db_path),
        "finra_root": str(finra_root),
        "source_market": args.source_market,
        "short_interest_source_count": len(short_interest_sources),
        "daily_short_volume_source_count": len(daily_short_volume_sources),
    }

    print()
    print("===== RUN FINRA DAILY REFRESH DONE =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
