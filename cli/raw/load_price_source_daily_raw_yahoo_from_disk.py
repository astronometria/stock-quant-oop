#!/usr/bin/env python3
from __future__ import annotations

"""
load_price_source_daily_raw_yahoo_from_disk.py

Objectif
--------
Charger les fichiers JSONL raw Yahoo/yfinance persistés sur disque local
dans la table DuckDB `price_source_daily_raw_yahoo`.

Règle d'architecture
--------------------
Ce script :
- lit le raw disque local
- charge la table raw DB
- ne touche PAS à price_history
- ne touche PAS à price_latest

C'est la deuxième étape du flux raw-first :

1) réseau -> fichiers raw disque
2) fichiers raw disque -> price_source_daily_raw_yahoo   <-- ce script
3) raw yahoo + raw stooq -> price_source_daily_raw
4) price_source_daily_raw -> price_history
5) price_history -> price_latest

Entrée attendue
---------------
Des fichiers JSONL produits par :
`cli/raw/fetch_price_source_daily_raw_yahoo.py`

Format JSONL attendu (une ligne par barre journalière) :
{
  "provider_name": "yfinance",
  "source_name": "yfinance_raw_disk",
  "requested_symbols": [...],
  "provider_symbols": [...],
  "fetch_started_at_utc": "...",
  "fetch_finished_at_utc": "...",
  "symbol": "...",
  "price_date": "YYYY-MM-DD",
  "open": ...,
  "high": ...,
  "low": ...,
  "close": ...,
  "volume": ...
}

Comportement
------------
- découverte récursive des fichiers JSONL
- idempotence via NOT EXISTS logique
- possibilité de filtrer une date de dossier spécifique
- création prudente de la table si elle n'existe pas
- ajout d'une petite table de manifest DB pour tracer les fichiers chargés

Tables manipulées
-----------------
1) price_source_daily_raw_yahoo
2) price_source_daily_raw_yahoo_files   (manifest DB minimal)

Philosophie
-----------
- SQL-first pour la partie transformation / insertion
- Python mince pour l'orchestration fichier
- beaucoup de commentaires
"""

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
from tqdm import tqdm


# ============================================================================
# CLI / helpers
# ============================================================================


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.
    """
    parser = argparse.ArgumentParser(
        description="Load local yfinance raw JSONL files into price_source_daily_raw_yahoo."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database.",
    )
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop-raw/data/raw/prices/yfinance",
        help="Root directory containing raw yfinance daily JSONL files.",
    )
    parser.add_argument(
        "--run-date",
        help="Optional run date YYYY-MM-DD. If provided, load only daily/<run-date>/...",
    )
    parser.add_argument(
        "--overwrite-file-manifest",
        action="store_true",
        help="If enabled, refresh the file manifest rows for files processed in this run.",
    )
    return parser.parse_args()


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Crée les tables raw Yahoo si elles n'existent pas.

    On reste compatible avec le schéma déjà observé dans la DB :
    price_source_daily_raw_yahoo(
        symbol,
        price_date,
        open,
        high,
        low,
        close,
        volume,
        source_name,
        fetched_at
    )

    On ajoute une petite table manifest séparée pour tracer les fichiers.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo (
            symbol VARCHAR NOT NULL,
            price_date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE NOT NULL,
            volume BIGINT,
            source_name VARCHAR NOT NULL,
            fetched_at TIMESTAMP
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS price_source_daily_raw_yahoo_files (
            source_file VARCHAR PRIMARY KEY,
            source_run_id VARCHAR,
            source_run_date DATE,
            rows_loaded BIGINT,
            discovered_at TIMESTAMP,
            loaded_at TIMESTAMP,
            source_name VARCHAR
        )
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_source_daily_raw_yahoo_symbol_date
        ON price_source_daily_raw_yahoo(symbol, price_date)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_source_daily_raw_yahoo_source_name
        ON price_source_daily_raw_yahoo(source_name)
        """
    )


def resolve_daily_root(data_root: Path, run_date: str | None) -> Path:
    """
    Résout le dossier daily ciblé.
    """
    if run_date:
        return data_root / "daily" / run_date
    return data_root / "daily"


def discover_jsonl_files(root: Path) -> list[Path]:
    """
    Découvre les fichiers JSONL de batch.
    """
    if not root.exists():
        return []
    return sorted(
        [
            path.resolve()
            for path in root.rglob("*.jsonl")
            if path.is_file()
        ]
    )


def infer_run_id_from_file(path: Path) -> str | None:
    """
    Tente d'extraire le run_id depuis le nom de fichier batch.

    Exemple :
    yfinance_daily_20260328T135406Z__batch_00001.jsonl
    -> yfinance_daily_20260328T135406Z
    """
    name = path.name
    marker = "__batch_"
    if marker not in name:
        return None
    return name.split(marker, 1)[0]


def infer_run_date_from_path(path: Path) -> str | None:
    """
    Tente d'extraire la date depuis le segment daily/YYYY-MM-DD.
    """
    parts = list(path.parts)
    if "daily" not in parts:
        return None
    idx = parts.index("daily")
    if idx + 1 >= len(parts):
        return None
    value = parts[idx + 1]
    if len(value) == 10 and value.count("-") == 2:
        return value
    return None


def json_escape_single_quotes(text: str) -> str:
    """
    Échappe les quotes simples pour injecter en SQL littéral si nécessaire.

    On essaie d'éviter cette voie, mais ce helper peut servir dans les notes.
    """
    return text.replace("'", "''")


# ============================================================================
# Main
# ============================================================================


def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. résolution chemins
    2. découverte fichiers
    3. staging JSONL
    4. insertion idempotente table raw
    5. manifest fichier
    6. métriques
    """
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    daily_root = resolve_daily_root(data_root=data_root, run_date=args.run_date)

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=6, desc="load_price_source_daily_raw_yahoo_from_disk", unit="step")

    try:
        # --------------------------------------------------------------------
        # 1) Préparation schéma
        # --------------------------------------------------------------------
        steps.set_description("load_price_source_daily_raw_yahoo_from_disk:ensure_schema")
        ensure_schema(con)
        steps.update(1)

        # --------------------------------------------------------------------
        # 2) Découverte des fichiers JSONL
        # --------------------------------------------------------------------
        steps.set_description("load_price_source_daily_raw_yahoo_from_disk:discover_files")
        files = discover_jsonl_files(daily_root)
        steps.update(1)

        if not files:
            print(
                json.dumps(
                    {
                        "db_path": str(db_path),
                        "data_root": str(data_root),
                        "daily_root": str(daily_root),
                        "files_discovered": 0,
                        "rows_inserted": 0,
                        "mode": "raw_disk_to_raw_db_no_files",
                    },
                    indent=2,
                )
            )
            return 0

        inserted_total = 0
        manifest_rows_written = 0

        # --------------------------------------------------------------------
        # 3) Traitement fichier par fichier
        # --------------------------------------------------------------------
        file_bar = tqdm(files, desc="load_yahoo_jsonl_files", unit="file", dynamic_ncols=True, leave=False)

        for file_path in file_bar:
            run_id = infer_run_id_from_file(file_path)
            run_date = infer_run_date_from_path(file_path)

            steps.set_description("load_price_source_daily_raw_yahoo_from_disk:stage_insert_manifest")

            # Table temporaire stage pour ce fichier.
            con.execute("DROP TABLE IF EXISTS tmp_price_source_daily_raw_yahoo_stage")

            # read_json_auto sait lire le JSONL ligne par ligne.
            con.execute(
                f"""
                CREATE TEMP TABLE tmp_price_source_daily_raw_yahoo_stage AS
                SELECT
                    UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                    CAST(price_date AS DATE) AS price_date,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    CAST(volume AS BIGINT) AS volume,
                    CAST(source_name AS VARCHAR) AS source_name,
                    CAST(fetch_finished_at_utc AS TIMESTAMP) AS fetched_at
                FROM read_json_auto(?, format='newline_delimited')
                WHERE symbol IS NOT NULL
                  AND price_date IS NOT NULL
                  AND close IS NOT NULL
                """,
                [str(file_path)],
            )

            stage_rows = int(
                con.execute(
                    "SELECT COUNT(*) FROM tmp_price_source_daily_raw_yahoo_stage"
                ).fetchone()[0]
            )

            # ----------------------------------------------------------------
            # 4) Insertion idempotente dans la table raw Yahoo.
            #
            # Clé logique utilisée :
            # (symbol, price_date, source_name)
            #
            # Cette clé est cohérente avec le rôle de la table :
            # stocker les barres raw Yahoo datées.
            # ----------------------------------------------------------------
            before_rows = int(
                con.execute(
                    "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
                ).fetchone()[0]
            )

            con.execute(
                """
                INSERT INTO price_source_daily_raw_yahoo (
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    fetched_at
                )
                SELECT
                    s.symbol,
                    s.price_date,
                    s.open,
                    s.high,
                    s.low,
                    s.close,
                    s.volume,
                    COALESCE(s.source_name, 'yfinance_raw_disk') AS source_name,
                    s.fetched_at
                FROM tmp_price_source_daily_raw_yahoo_stage s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM price_source_daily_raw_yahoo t
                    WHERE t.symbol = s.symbol
                      AND t.price_date = s.price_date
                      AND COALESCE(t.source_name, '') = COALESCE(s.source_name, '')
                )
                """
            )

            after_rows = int(
                con.execute(
                    "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
                ).fetchone()[0]
            )
            inserted_rows = after_rows - before_rows
            inserted_total += inserted_rows

            # ----------------------------------------------------------------
            # 5) Manifest DB par fichier.
            # ----------------------------------------------------------------
            if args.overwrite_file_manifest:
                con.execute(
                    """
                    DELETE FROM price_source_daily_raw_yahoo_files
                    WHERE source_file = ?
                    """,
                    [str(file_path)],
                )

            con.execute(
                """
                INSERT INTO price_source_daily_raw_yahoo_files (
                    source_file,
                    source_run_id,
                    source_run_date,
                    rows_loaded,
                    discovered_at,
                    loaded_at,
                    source_name
                )
                SELECT
                    ?,
                    ?,
                    CAST(? AS DATE),
                    ?,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP,
                    'yfinance_raw_disk'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM price_source_daily_raw_yahoo_files
                    WHERE source_file = ?
                )
                """,
                [
                    str(file_path),
                    run_id,
                    run_date,
                    stage_rows,
                    str(file_path),
                ],
            )
            manifest_rows_written += 1

        steps.update(1)

        # --------------------------------------------------------------------
        # 6) Métriques finales
        # --------------------------------------------------------------------
        steps.set_description("load_price_source_daily_raw_yahoo_from_disk:metrics")

        raw_total = int(
            con.execute(
                "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
            ).fetchone()[0]
        )

        raw_symbols = int(
            con.execute(
                "SELECT COUNT(DISTINCT symbol) FROM price_source_daily_raw_yahoo"
            ).fetchone()[0]
        )

        raw_date_min, raw_date_max = con.execute(
            """
            SELECT MIN(price_date), MAX(price_date)
            FROM price_source_daily_raw_yahoo
            """
        ).fetchone()

        source_breakdown = con.execute(
            """
            SELECT source_name, COUNT(*) AS rows_count
            FROM price_source_daily_raw_yahoo
            GROUP BY 1
            ORDER BY rows_count DESC, source_name
            """
        ).fetchall()

        steps.update(1)
        steps.update(1)
        steps.update(1)

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "data_root": str(data_root),
                    "daily_root": str(daily_root),
                    "files_discovered": len(files),
                    "manifest_rows_written": int(manifest_rows_written),
                    "rows_inserted_this_run": int(inserted_total),
                    "price_source_daily_raw_yahoo_total_rows": int(raw_total),
                    "price_source_daily_raw_yahoo_total_symbols": int(raw_symbols),
                    "price_source_daily_raw_yahoo_min_date": str(raw_date_min) if raw_date_min is not None else None,
                    "price_source_daily_raw_yahoo_max_date": str(raw_date_max) if raw_date_max is not None else None,
                    "source_breakdown": source_breakdown,
                    "mode": "raw_disk_to_raw_db_sql_first",
                },
                indent=2,
            )
        )

        return 0

    finally:
        steps.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
