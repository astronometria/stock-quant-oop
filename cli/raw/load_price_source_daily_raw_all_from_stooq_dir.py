#!/usr/bin/env python3
from __future__ import annotations

"""
Chunked SQL-first loader for extracted Stooq directory.

Objectif
--------
Charger massivement l'arborescence Stooq déjà extraite sur disque dans
price_source_daily_raw_all, de façon stable en mémoire et avec progression
visible.

Pourquoi cette version
----------------------
La version "one big INSERT over **/*.txt" est élégante, mais peut:
- consommer trop de RAM
- rester silencieuse trop longtemps
- donner l'impression d'un freeze

Cette version:
- découpe le chargement par "chunks" de dossiers
- garde DuckDB pour le parsing CSV et les transformations SQL
- garde Python très mince pour l'orchestration des chunks
- écrit des stats après chaque chunk

Philosophie
-----------
- SQL-first pour la lecture, les casts et les transformations
- Python mince pour la découverte des chunks et le reporting

Notes
-----
- aucun téléchargement
- aucun fallback legacy
- uniquement les fichiers présents sous --root-dir
- très commenté volontairement pour maintenance par d'autres développeurs
"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


# -----------------------------------------------------------------------------
# Modèle simple pour décrire un chunk de chargement.
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class LoadChunk:
    """
    Un chunk correspond à un sous-ensemble de l'arborescence Stooq.

    Exemples:
    - /.../us/nasdaq stocks/1
    - /.../us/nasdaq stocks/2
    - /.../us/nyse etfs/1
    - /.../us/nasdaq etfs
    """
    label: str
    glob_pattern: str
    file_count: int


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Chunked SQL-first load of extracted Stooq directory into price_source_daily_raw_all."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--root-dir",
        required=True,
        help="Root directory containing extracted Stooq txt files.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing rows from price_source_daily_raw_all before load.",
    )
    parser.add_argument(
        "--exclude-etfs",
        action="store_true",
        help="Skip ETF folders during load.",
    )
    parser.add_argument(
        "--memory-limit",
        default="24GB",
        help="DuckDB memory limit, e.g. 16GB, 24GB, 32GB.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="DuckDB thread count.",
    )
    parser.add_argument(
        "--temp-dir",
        default="~/stock-quant-oop/tmp/duckdb_stooq_loader",
        help="DuckDB temp directory for spilling.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print extra diagnostics.",
    )
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Découverte des chunks.
# -----------------------------------------------------------------------------
def _is_numeric_dir(path: Path) -> bool:
    return path.is_dir() and path.name.isdigit()


def _count_txt_files(path: Path) -> int:
    return sum(1 for p in path.rglob("*.txt") if p.is_file())


def discover_chunks(root_dir: Path, exclude_etfs: bool) -> list[LoadChunk]:
    """
    Découvre les chunks de chargement de manière déterministe.

    Règle:
    - on parcourt les sous-dossiers immédiats sous root_dir
    - si un sous-dossier contient des sous-dossiers numériques (1,2,3,...),
      on crée un chunk par sous-dossier numérique
    - sinon, on crée un chunk pour le dossier lui-même

    Pourquoi:
    - meilleure visibilité de progression
    - meilleure maîtrise mémoire
    - limite la taille de chaque requête DuckDB
    """
    chunks: list[LoadChunk] = []

    for top in sorted([p for p in root_dir.iterdir() if p.is_dir()]):
        top_low = top.name.lower()
        if exclude_etfs and "etfs" in top_low:
            continue

        numeric_children = sorted([p for p in top.iterdir() if _is_numeric_dir(p)])
        if numeric_children:
            for child in numeric_children:
                file_count = _count_txt_files(child)
                if file_count <= 0:
                    continue
                chunks.append(
                    LoadChunk(
                        label=f"{top.name}/{child.name}",
                        glob_pattern=str(child / "**" / "*.txt"),
                        file_count=file_count,
                    )
                )
        else:
            file_count = _count_txt_files(top)
            if file_count <= 0:
                continue
            chunks.append(
                LoadChunk(
                    label=top.name,
                    glob_pattern=str(top / "**" / "*.txt"),
                    file_count=file_count,
                )
            )

    return chunks


# -----------------------------------------------------------------------------
# SQL generation.
# -----------------------------------------------------------------------------
def build_insert_sql(glob_pattern: str, ingested_at: str) -> str:
    """
    Génère le SQL de chargement pour un chunk.

    Points importants:
    - DuckDB lit directement les txt avec read_csv_auto
    - filename=true fournit la colonne "filename"
    - CAST explicite pour éviter les BinderException
    - volume casté via DOUBLE -> BIGINT
    """
    escaped_glob = glob_pattern.replace("'", "''")

    return f"""
    INSERT INTO price_source_daily_raw_all (
        symbol,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        source_name,
        source_path,
        asset_class,
        venue_group,
        ingested_at
    )
    SELECT
        REPLACE(UPPER(CAST("<TICKER>" AS VARCHAR)), '.US', '') AS symbol,

        CAST(
            SUBSTR(CAST("<DATE>" AS VARCHAR), 1, 4) || '-' ||
            SUBSTR(CAST("<DATE>" AS VARCHAR), 5, 2) || '-' ||
            SUBSTR(CAST("<DATE>" AS VARCHAR), 7, 2)
            AS DATE
        ) AS price_date,

        CAST("<OPEN>" AS DOUBLE)  AS open,
        CAST("<HIGH>" AS DOUBLE)  AS high,
        CAST("<LOW>" AS DOUBLE)   AS low,
        CAST("<CLOSE>" AS DOUBLE) AS close,
        CAST(CAST("<VOL>" AS DOUBLE) AS BIGINT) AS volume,

        'stooq_dir_full' AS source_name,
        filename AS source_path,

        CASE
            WHEN lower(filename) LIKE '%etfs%' THEN 'ETF'
            WHEN lower(filename) LIKE '%stocks%' THEN 'STOCK'
            ELSE 'UNKNOWN'
        END AS asset_class,

        CASE
            WHEN lower(filename) LIKE '%nasdaq stocks%' THEN 'NASDAQ STOCKS'
            WHEN lower(filename) LIKE '%nyse stocks%' THEN 'NYSE STOCKS'
            WHEN lower(filename) LIKE '%nysemkt stocks%' THEN 'NYSEMKT STOCKS'
            WHEN lower(filename) LIKE '%nasdaq etfs%' THEN 'NASDAQ ETFS'
            WHEN lower(filename) LIKE '%nyse etfs%' THEN 'NYSE ETFS'
            WHEN lower(filename) LIKE '%nysemkt etfs%' THEN 'NYSEMKT ETFS'
            ELSE 'UNKNOWN'
        END AS venue_group,

        TIMESTAMP '{ingested_at}' AS ingested_at

    FROM read_csv_auto(
        '{escaped_glob}',
        delim=',',
        header=True,
        filename=True,
        ignore_errors=True
    )
    WHERE CAST("<PER>" AS VARCHAR) = 'D'
      AND "<TICKER>" IS NOT NULL
      AND "<DATE>" IS NOT NULL
    """


# -----------------------------------------------------------------------------
# Stats helpers.
# -----------------------------------------------------------------------------
def table_stats(con: duckdb.DuckDBPyConnection) -> dict:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            COUNT(DISTINCT source_path) AS files,
            MIN(price_date) AS min_price_date,
            MAX(price_date) AS max_price_date
        FROM price_source_daily_raw_all
        """
    ).fetchone()

    return {
        "rows": int(row[0] or 0),
        "symbols": int(row[1] or 0),
        "files": int(row[2] or 0),
        "min_price_date": row[3],
        "max_price_date": row[4],
    }


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    root_dir = Path(args.root_dir).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve()

    if not root_dir.exists():
        raise SystemExit(f"root_dir not found: {root_dir}")
    if not root_dir.is_dir():
        raise SystemExit(f"root_dir is not a directory: {root_dir}")

    temp_dir.mkdir(parents=True, exist_ok=True)

    chunks = discover_chunks(root_dir=root_dir, exclude_etfs=args.exclude_etfs)
    if not chunks:
        raise SystemExit(f"no Stooq txt chunks found under {root_dir}")

    total_chunk_files = sum(chunk.file_count for chunk in chunks)
    started_at = datetime.utcnow().isoformat(timespec="seconds")

    con = duckdb.connect(str(db_path))
    try:
        # Réglages DuckDB pour limiter la casse mémoire et permettre le spilling.
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        temp_dir_sql = str(temp_dir).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        if args.verbose:
            print(f"[loader] db_path={db_path}", flush=True)
            print(f"[loader] root_dir={root_dir}", flush=True)
            print(f"[loader] temp_dir={temp_dir}", flush=True)
            print(f"[loader] memory_limit={args.memory_limit}", flush=True)
            print(f"[loader] threads={args.threads}", flush=True)
            print(f"[loader] exclude_etfs={args.exclude_etfs}", flush=True)
            print(f"[loader] chunk_count={len(chunks)}", flush=True)
            print(f"[loader] chunk_file_count={total_chunk_files}", flush=True)

        if args.truncate:
            print("[loader] truncating table price_source_daily_raw_all...", flush=True)
            con.execute("DELETE FROM price_source_daily_raw_all")

        before = table_stats(con)
        print(
            "[loader] starting chunked load: "
            + json.dumps(
                {
                    "started_at": started_at,
                    "chunks": len(chunks),
                    "chunk_files": total_chunk_files,
                    "table_before": before,
                },
                default=str,
            ),
            flush=True,
        )

        running_chunk_index = 0

        for chunk in tqdm(
            chunks,
            desc="stooq raw chunks",
            unit="chunk",
            dynamic_ncols=True,
            mininterval=0.5,
        ):
            running_chunk_index += 1

            before_chunk = table_stats(con)
            sql = build_insert_sql(
                glob_pattern=chunk.glob_pattern,
                ingested_at=datetime.utcnow().isoformat(timespec="seconds"),
            )

            print(
                f"[loader] chunk {running_chunk_index}/{len(chunks)} "
                f"label={chunk.label} files={chunk.file_count} "
                f"rows_before={before_chunk['rows']}",
                flush=True,
            )

            con.execute(sql)

            after_chunk = table_stats(con)
            inserted_rows = after_chunk["rows"] - before_chunk["rows"]

            print(
                f"[loader] chunk_done label={chunk.label} "
                f"inserted_rows={inserted_rows} "
                f"rows_after={after_chunk['rows']} "
                f"symbols_after={after_chunk['symbols']} "
                f"min_date={after_chunk['min_price_date']} "
                f"max_date={after_chunk['max_price_date']}",
                flush=True,
            )

        after = table_stats(con)

        print("\n===== LOAD COMPLETE =====", flush=True)
        print(json.dumps(
            {
                "status": "success",
                "started_at": started_at,
                "finished_at": datetime.utcnow().isoformat(timespec="seconds"),
                "db_path": str(db_path),
                "root_dir": str(root_dir),
                "exclude_etfs": args.exclude_etfs,
                "chunk_count": len(chunks),
                "chunk_file_count": total_chunk_files,
                "table_before": before,
                "table_after": after,
            },
            indent=2,
            default=str,
        ), flush=True)

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
