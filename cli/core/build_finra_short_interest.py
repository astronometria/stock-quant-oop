#!/usr/bin/env python3
from __future__ import annotations

"""
Canonical FINRA short-interest builder.

Ce CLI doit rester le point d'entrée unique du build short interest.
Important:
- on garde la logique d'orchestration la plus mince possible ici
- la logique métier et SQL-first vivent dans le repository / pipeline
- ce fichier ne doit PAS réintroduire d'ancienne signature cassée
"""

import argparse
from pathlib import Path

import duckdb

from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import (
    DuckDbShortInterestRepository,
)
from stock_quant.pipelines.build_short_interest_pipeline import BuildShortInterestPipeline


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    On garde un contrat simple:
    --db-path est obligatoire pour éviter toute ambiguïté d'environnement.
    """
    parser = argparse.ArgumentParser(
        description="Build FINRA short interest (canonical SQL-first pipeline)."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Point d'entrée principal.

    Très important:
    - le repository est branché sur une connexion DuckDB active
    - le service reçoit le repository
    - le pipeline reçoit uniquement le service
      (et NON repository=..., car cette signature n'existe plus)
    """
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_finra_short_interest] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        repository = DuckDbShortInterestRepository(con)
        service = ShortInterestService(repository=repository)

        # Correction clé:
        # l'ancienne signature avec repository=... n'est plus supportée.
        pipeline = BuildShortInterestPipeline(service=service)

        result = pipeline.run()
        print(result.to_json(), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
