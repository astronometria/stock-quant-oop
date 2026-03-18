#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.pipelines.build_short_features_pipeline import BuildShortFeaturesPipeline


def parse_args() -> argparse.Namespace:
    """
    CLI canonique pour construire `short_features_daily`.

    Notes d'exploitation
    --------------------
    - ce wrapper reste mince: l'orchestration lourde vit dans la pipeline SQL-first
    - on applique ici quelques réglages DuckDB défensifs pour réduire le risque OOM
    - l'exécution reste séparée de l'écriture du fichier pour garder tqdm visible
    """
    parser = argparse.ArgumentParser(
        description="Build canonical short features daily."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=4,
        help="DuckDB worker threads used for this build.",
    )
    parser.add_argument(
        "--duckdb-memory-limit",
        default="42GB",
        help="DuckDB memory_limit value for this build.",
    )
    parser.add_argument(
        "--preserve-insertion-order",
        action="store_true",
        help="Keep DuckDB preserve_insertion_order=true. Default is false for lower memory pressure.",
    )
    return parser.parse_args()


def _result_to_payload(result: Any) -> dict[str, Any]:
    """
    Normalise différents formats de PipelineResult / objets compatibles
    vers un dictionnaire sérialisable JSON.

    Cas supportés
    -------------
    - dict
    - dataclass
    - namedtuple / objet avec _asdict()
    - objet classique exposant des attributs publics
    """
    if isinstance(result, dict):
        return result

    if is_dataclass(result):
        return asdict(result)

    if hasattr(result, "_asdict"):
        try:
            return result._asdict()
        except Exception:
            pass

    payload: dict[str, Any] = {}
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


def _configure_duckdb(
    con: duckdb.DuckDBPyConnection,
    *,
    threads: int,
    memory_limit: str,
    preserve_insertion_order: bool,
) -> None:
    """
    Applique des pragmas/settings DuckDB défensifs.

    Pourquoi ici
    ------------
    `build_short_features` travaille sur des tables très volumineuses
    (`daily_short_volume_history` ~ dizaines de millions de lignes).
    Sur cette charge, réduire les threads et désactiver la préservation
    stricte de l'ordre d'insertion aide souvent à limiter la pression mémoire.
    """
    # Toujours borner à au moins 1 pour éviter une config invalide.
    effective_threads = max(1, int(threads))

    con.execute(f"SET threads = {effective_threads}")
    con.execute(
        f"SET preserve_insertion_order = {'true' if preserve_insertion_order else 'false'}"
    )
    con.execute(f"SET memory_limit = '{memory_limit}'")


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    print(f"[build_short_features] db_path={db_path}", flush=True)

    # Ouverture explicite de la connexion DuckDB du build.
    con = duckdb.connect(db_path)

    try:
        # Réglages runtime DuckDB pour ce job précis.
        _configure_duckdb(
            con,
            threads=args.duckdb_threads,
            memory_limit=args.duckdb_memory_limit,
            preserve_insertion_order=args.preserve_insertion_order,
        )

        # Construction de la pipeline SQL-first.
        pipeline = BuildShortFeaturesPipeline(con=con)

        # Exécution synchrone du build.
        result = pipeline.run()

        # Sortie JSON robuste pour scripting / logs.
        print(json.dumps(_result_to_payload(result), default=str), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
