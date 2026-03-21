#!/usr/bin/env python3
from __future__ import annotations

"""
Charge les fichiers raw FINRA short interest depuis le disque.

Objectif
--------
- Scanner le disque avant transformation
- Supporter un dossier raw persistant
- Rester compatible avec FinraProvider / FinraRawLoader
- Permettre un run incrémental côté disque

Important
---------
- Ce script ne télécharge rien
- Il choisit simplement les bons chemins à injecter
"""

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.infrastructure.providers.finra.finra_provider import FinraProvider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load FINRA short interest raw files from persistent disk paths."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Explicit file or directory source path. Repeat this flag if needed.",
    )
    parser.add_argument(
        "--default-root",
        default="/home/marty/stock-quant-oop/data/raw/finra/short_interest",
        help="Default root scanned when no explicit --source is supplied.",
    )
    parser.add_argument(
        "--source-name",
        default="finra_short_interest_disk_raw",
        help="Logical source name stored downstream.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _log(message: str, verbose: bool) -> None:
    if verbose:
        print(message, flush=True)


def discover_sources(explicit_sources: list[str], default_root: str) -> list[str]:
    """
    Retourne une liste de paths à passer au provider.
    On privilégie les répertoires racines pour laisser le loader parser
    tous les fichiers qu'il sait déjà gérer.
    """
    resolved: list[str] = []

    for raw in explicit_sources:
        path = Path(raw).expanduser().resolve()
        if path.exists():
            resolved.append(str(path))

    if not resolved:
        default_path = Path(default_root).expanduser().resolve()
        if default_path.exists():
            resolved.append(str(default_path))

    # Déduplication stable.
    out: list[str] = []
    seen: set[str] = set()
    for item in resolved:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)

    return out


def main() -> int:
    args = parse_args()

    source_paths = discover_sources(args.sources, args.default_root)
    if not source_paths:
        raise RuntimeError("No FINRA short interest source path found on disk.")

    print(f"[load_finra_short_interest_source_raw] source_paths={source_paths}", flush=True)

    con = duckdb.connect(args.db_path)
    try:
        provider = FinraProvider(con)
        result = provider.load_short_interest_source_raw(
            source_paths=source_paths,
            source_name=args.source_name,
        )
        print(json.dumps(result, indent=2, default=str), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
