#!/usr/bin/env python3
from __future__ import annotations

"""
Wrapper robuste pour le refresh quotidien des prix.

Objectifs:
- ne plus utiliser l'ancien argument obsolète `--mode daily`
- appeler `cli/core/build_prices.py` avec l'interface actuelle
- garder une sortie terminal propre
- conserver un log exploitable pour les autres développeurs

Comportement:
- par défaut, on lance build_prices avec seulement --db-path
  ce qui laisse build_prices résoudre lui-même la fenêtre de refresh
- si des filtres sont fournis (symbols, as-of, lookback-days, catchup-max-days),
  on les transmet explicitement
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

    On garde une interface simple et compatible avec le wrapper de rebuild.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--as-of")
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--catchup-max-days", type=int, default=30)
    parser.add_argument("--python-bin", default=sys.executable)
    return parser.parse_args()


def build_step_command(args: argparse.Namespace) -> list[str]:
    """
    Construit la commande vers build_prices.py avec l'interface actuelle.

    Très important:
    - on n'utilise PAS `--mode daily`
    - on délègue au script build_prices la logique de fenêtre effective
    """
    project_root = Path(args.project_root).expanduser().resolve()
    build_prices_path = project_root / "cli/core/build_prices.py"

    cmd: list[str] = [
        str(Path(args.python_bin).expanduser()),
        str(build_prices_path),
        "--db-path",
        str(Path(args.db_path).expanduser().resolve()),
    ]

    if args.symbols:
        cmd.append("--symbols")
        cmd.extend(args.symbols)

    if args.as_of:
        cmd.extend(["--as-of", args.as_of])

    # On transmet ces paramètres uniquement si présents / utiles.
    if args.lookback_days is not None:
        cmd.extend(["--lookback-days", str(args.lookback_days)])

    if args.catchup_max_days is not None:
        cmd.extend(["--catchup-max-days", str(args.catchup_max_days)])

    return cmd


def run_step(step_name: str, cmd: list[str]) -> None:
    """
    Exécute une commande enfant et échoue bruyamment si elle retourne non-zéro.
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

    # Header lisible et stable pour le terminal / logs.
    print("===== RUN PRICE DAILY REFRESH START =====")
    print(f"PROJECT ROOT: {project_root}")
    print(f"DB PATH: {db_path}")
    print(f"SYMBOLS: {0 if not args.symbols else len(args.symbols)}")
    print("NORMALIZED TABLE: price_history")
    print("DERIVED TABLE: price_latest (serving only)")
    print()

    cmd = build_step_command(args)

    # Une petite tqdm propre pour garder un rendu homogène avec le reste.
    steps = [("build_prices_daily", cmd)]
    progress = tqdm(steps, desc="price daily refresh", unit="step")

    for step_name, step_cmd in progress:
        run_step(step_name, step_cmd)

    summary = {
        "status": "SUCCESS",
        "finished_at": datetime.utcnow().isoformat(),
        "project_root": str(project_root),
        "db_path": str(db_path),
        "symbols_count": 0 if not args.symbols else len(args.symbols),
        "lookback_days": args.lookback_days,
        "catchup_max_days": args.catchup_max_days,
    }

    print()
    print("===== RUN PRICE DAILY REFRESH DONE =====")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
