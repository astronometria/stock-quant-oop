#!/usr/bin/env python3
from __future__ import annotations

"""
Canonical FINRA short interest builder.

Responsabilités :
- point d'entrée unique du pipeline short interest
- wiring propre repository -> service -> pipeline
- aucune logique métier ici

Important :
- ne dépend PAS du short volume
- ne dépend PAS des features
"""

import argparse
from pathlib import Path
import json
import duckdb

from stock_quant.pipelines.build_short_interest_pipeline import BuildShortInterestPipeline
from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import (
    DuckDbShortInterestRepository,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build FINRA short interest (incremental, canonical)")
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    db_path = str(Path(args.db_path).expanduser().resolve())

    print(f"[build_finra_short_interest] db_path={db_path}", flush=True)

    # ---------------------------------------------------------
    # DB connection
    # ---------------------------------------------------------
    con = duckdb.connect(db_path)

    try:
        # -----------------------------------------------------
        # Wiring
        # -----------------------------------------------------
        repository = DuckDbShortInterestRepository(con)
        service = ShortInterestService(repository=repository)

        pipeline = BuildShortInterestPipeline(
            repository=repository,
            service=service,
        )

        # -----------------------------------------------------
        # Run
        # -----------------------------------------------------
        result = pipeline.run()

        print(json.dumps(result.to_dict(), default=str), flush=True)

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
