#!/usr/bin/env python3
from __future__ import annotations

"""
Canonical FINRA short interest builder.

Ce CLI est l'entrée canonique du domaine short interest.
Il reste volontairement mince:
- wiring repository/service/pipeline
- sérialisation robuste du PipelineResult
- sortie JSON stable pour les logs et probes
"""

import argparse
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import DuckDbShortInterestRepository
from stock_quant.pipelines.build_short_interest_pipeline import BuildShortInterestPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build FINRA short interest (canonical pipeline)."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def _json_default(value: Any) -> Any:
    """
    Convertisseur JSON robuste pour les objets non sérialisables nativement.
    """
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, Path):
        return str(value)

    if hasattr(value, "value"):
        try:
            return value.value
        except Exception:
            pass

    if hasattr(value, "name"):
        try:
            return value.name
        except Exception:
            pass

    return str(value)


def _result_to_payload(result: Any) -> dict[str, Any]:
    """
    Rend le résultat imprimable sans dépendre d'une méthode DTO spécifique.

    Ordre de préférence:
    1. to_json()  -> reparse en dict si possible
    2. to_dict()
    3. dataclass -> asdict()
    4. __dict__
    5. fallback minimal
    """
    if hasattr(result, "to_json"):
        try:
            raw = result.to_json()
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except Exception:
                    return {"result": raw}
            if isinstance(raw, dict):
                return raw
            return {"result": raw}
        except Exception:
            pass

    if hasattr(result, "to_dict"):
        try:
            payload = result.to_dict()
            if isinstance(payload, dict):
                return payload
            return {"result": payload}
        except Exception:
            pass

    if is_dataclass(result):
        try:
            payload = asdict(result)
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass

    if hasattr(result, "__dict__"):
        try:
            payload = {
                key: value
                for key, value in vars(result).items()
                if not key.startswith("_")
            }
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass

    return {"result": str(result)}


def main() -> int:
    args = parse_args()

    db_path = str(Path(args.db_path).expanduser().resolve())
    print(f"[build_finra_short_interest] db_path={db_path}", flush=True)

    con = duckdb.connect(db_path)
    try:
        repository = DuckDbShortInterestRepository(con)
        service = ShortInterestService(repository=repository)
        pipeline = BuildShortInterestPipeline(service=service)

        result = pipeline.run()
        payload = _result_to_payload(result)

        print(
            json.dumps(
                payload,
                ensure_ascii=False,
                default=_json_default,
            ),
            flush=True,
        )
        return 0
    finally:
        try:
            con.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
