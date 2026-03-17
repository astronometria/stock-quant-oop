#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path

import duckdb

from stock_quant.pipelines.build_short_features_pipeline import BuildShortFeaturesPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build canonical short features daily."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def _result_to_payload(result) -> dict:
    if isinstance(result, dict):
        return result
    if is_dataclass(result):
        return asdict(result)
    if hasattr(result, "_asdict"):
        return result._asdict()
    payload = {}
    for name in dir(result):
        if name.startswith("_"):
            continue
        try:
            value = getattr(result, name)
        except Exception:
            continue
        if callable(value):
            continue
        payload[name] = value
    return payload


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    print(f"[build_short_features] db_path={db_path}", flush=True)

    con = duckdb.connect(db_path)
    try:
        pipeline = BuildShortFeaturesPipeline(con=con)
        result = pipeline.run()
        print(json.dumps(_result_to_payload(result), default=str), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
