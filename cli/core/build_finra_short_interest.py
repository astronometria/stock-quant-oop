#!/usr/bin/env python3
from __future__ import annotations

"""
Canonical FINRA short interest builder.

This CLI is the only entry point for short interest.
"""

import argparse
from pathlib import Path

from stock_quant.pipelines.build_short_interest_pipeline import BuildShortInterestPipeline
from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import DuckDbShortInterestRepository
import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build FINRA short interest (canonical pipeline)")
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()

    con = duckdb.connect(Path(args.db_path))
    repo = DuckDbShortInterestRepository(con)
    service = ShortInterestService(repository=repo)
    pipeline = BuildShortInterestPipeline(service=service)

    result = pipeline.run()

    print(result.to_json(), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
