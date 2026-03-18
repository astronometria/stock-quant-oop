#!/usr/bin/env python3
"""
SQL-first loader pour Stooq (version scalable)

Principes:
- DuckDB lit directement les fichiers CSV (glob)
- Transformation faite en SQL (INSERT SELECT)
- Pas de executemany Python (évite les freezes)
- Support millions / milliards de lignes

Auteur: refactor pipeline raw (SQL-first)
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime

import duckdb

# tqdm uniquement pour feedback utilisateur
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):
        return x


# -----------------------------------------------------------------------------
# Arguments CLI
# -----------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="SQL-first load Stooq directory into DuckDB")
    p.add_argument("--db-path", required=True)
    p.add_argument("--root-dir", required=True)
    p.add_argument("--truncate", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


# -----------------------------------------------------------------------------
# Helpers SQL
# -----------------------------------------------------------------------------
def build_insert_sql(root_dir: str, ingested_at: str) -> str:
    """
    Génère un INSERT SELECT massif basé sur read_csv_auto avec glob.
    """

    return f"""
    INSERT INTO price_source_daily_raw_all
    SELECT
        -- symbol normalisé (remove .US)
        REPLACE(UPPER("<TICKER>"), '.US', '') AS symbol,

        -- date conversion YYYYMMDD -> DATE
        CAST(
            SUBSTR("<DATE>", 1, 4) || '-' ||
            SUBSTR("<DATE>", 5, 2) || '-' ||
            SUBSTR("<DATE>", 7, 2)
            AS DATE
        ) AS price_date,

        CAST("<OPEN>" AS DOUBLE)  AS open,
        CAST("<HIGH>" AS DOUBLE)  AS high,
        CAST("<LOW>" AS DOUBLE)   AS low,
        CAST("<CLOSE>" AS DOUBLE) AS close,
        CAST("<VOL>" AS BIGINT)   AS volume,

        'stooq_dir_full' AS source_name,

        filename AS source_path,

        CASE
            WHEN lower(filename) LIKE '%etfs%' THEN 'ETF'
            WHEN lower(filename) LIKE '%stocks%' THEN 'STOCK'
            ELSE 'UNKNOWN'
        END AS asset_class,

        -- FIX venue_group (ignore /1 /2 /3 folders)
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
        '{root_dir}/**/*.txt',
        delim=',',
        header=True,
        filename=True,
        ignore_errors=True
    )

    WHERE "<PER>" = 'D'
    """
    

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    db_path = str(Path(args.db_path).expanduser())
    root_dir = str(Path(args.root_dir).expanduser().resolve())

    if not Path(root_dir).exists():
        raise SystemExit(f"root_dir not found: {root_dir}")

    con = duckdb.connect(db_path)

    try:
        if args.verbose:
            print(f"[loader] db_path={db_path}")
            print(f"[loader] root_dir={root_dir}")

        # Reset si demandé
        if args.truncate:
            print("[loader] truncating table...")
            con.execute("DELETE FROM price_source_daily_raw_all")

        # Timestamp unique
        now = datetime.utcnow().isoformat()

        print("[loader] building SQL...")
        sql = build_insert_sql(root_dir=root_dir, ingested_at=now)

        print("[loader] executing INSERT (this may take a few minutes)...")
        con.execute(sql)

        print("[loader] computing stats...")

        result = con.execute("""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
                COUNT(DISTINCT source_path) AS files,
                MIN(price_date),
                MAX(price_date)
            FROM price_source_daily_raw_all
        """).fetchone()

        print("\n===== LOAD COMPLETE =====")
        print("rows       =", result[0])
        print("symbols    =", result[1])
        print("files      =", result[2])
        print("min_date   =", result[3])
        print("max_date   =", result[4])

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
