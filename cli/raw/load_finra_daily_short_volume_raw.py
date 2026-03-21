#!/usr/bin/env python3
from __future__ import annotations

"""
Load FINRA daily short volume raw files into DuckDB.

Objectif
--------
- Découvrir les fichiers raw déjà présents sur disque
- Accepter aussi des manifests CSV
- Charger de façon incrémentale vers la table raw
- Éviter de tout supprimer/recharger à chaque run

Format FINRA supporté
---------------------
Header 5 colonnes:
Date|Symbol|ShortVolume|TotalVolume|Market

Header 6 colonnes:
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
"""

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable

import duckdb

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable

from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager

EXPECTED_HEADER_5 = "Date|Symbol|ShortVolume|TotalVolume|Market"
EXPECTED_HEADER_6 = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"
ALLOWED_MARKET_DIRS = {"CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF"}

BATCH_SIZE = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load FINRA daily short volume raw files into DuckDB using robust historical format handling."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Source path. Can be a file, directory, glob, or manifest CSV. Repeat this flag.",
    )
    parser.add_argument(
        "--manifest",
        action="append",
        dest="manifests",
        default=[],
        help="Manifest CSV path. Repeat this flag.",
    )
    parser.add_argument(
        "--source-name",
        default="finra_disk_raw",
        help="Logical source name written in the raw table.",
    )
    parser.add_argument(
        "--default-root",
        default="/home/marty/stock-quant-oop/data/raw/finra/daily_short_sale_volume",
        help="Default disk root scanned when no explicit --source is supplied.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _log(message: str, verbose: bool) -> None:
    if verbose:
        print(message, flush=True)


def _table_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def ensure_incremental_index(con: duckdb.DuckDBPyConnection) -> None:
    """
    Ajoute une clé technique minimale pour éviter de recharger les mêmes fichiers.
    """
    cols = _table_columns(con, "finra_daily_short_volume_source_raw")

    if "source_file" not in cols:
        raise RuntimeError("finra_daily_short_volume_source_raw.source_file is required")

    if "source_name" not in cols:
        raise RuntimeError("finra_daily_short_volume_source_raw.source_name is required")

    # Index non unique supportant le skip incrémental.
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_daily_short_raw_source_file
        ON finra_daily_short_volume_source_raw(source_name, source_file)
        """
    )


def expand_sources(raw_sources: list[str], manifests: list[str], default_root: str) -> list[Path]:
    """
    Résout:
    - fichiers
    - répertoires
    - globs
    - manifests CSV (colonne local_path ou path)
    - fallback vers le default_root si aucun source explicite
    """
    candidates: list[Path] = []

    # 1) Sources explicites.
    for raw in raw_sources:
        path = Path(raw).expanduser()
        if any(ch in raw for ch in ["*", "?", "["]):
            for matched in sorted(Path().glob(raw)):
                if matched.is_file():
                    candidates.append(matched.resolve())
            continue

        if path.is_file():
            candidates.append(path.resolve())
        elif path.is_dir():
            for file_path in sorted(path.rglob("*")):
                if file_path.is_file():
                    candidates.append(file_path.resolve())

    # 2) Manifests.
    for manifest in manifests:
        manifest_path = Path(manifest).expanduser().resolve()
        if not manifest_path.exists():
            continue

        with manifest_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                local_path = (row.get("local_path") or row.get("path") or "").strip()
                if not local_path:
                    continue
                path = Path(local_path).expanduser()
                if path.exists() and path.is_file():
                    candidates.append(path.resolve())

    # 3) Fallback si rien n'est fourni.
    if not candidates:
        root = Path(default_root).expanduser().resolve()
        if root.exists():
            for file_path in sorted(root.rglob("*")):
                if file_path.is_file():
                    candidates.append(file_path.resolve())

    # Déduplication stable.
    seen: set[str] = set()
    out: list[Path] = []
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        out.append(path)
    return out


def is_supported_finra_daily_short_file(path: Path) -> bool:
    if path.suffix.lower() not in {".txt", ".csv"}:
        return False

    # On supporte les historiques déjà rangés par market.
    if path.parent.name.upper() in ALLOWED_MARKET_DIRS:
        return True

    # On supporte aussi les noms finra historiques si le parent n'est pas classé.
    name = path.name.upper()
    return name.startswith(("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")) and "SHVOL" in name


def read_finra_file(path: Path) -> list[tuple]:
    """
    Retourne une liste de tuples compatibles avec la table raw.
    On normalise vers:
    trade_date, symbol, short_volume, short_exempt_volume, total_volume, market, source_file
    """
    rows: list[tuple] = []

    text = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if not text:
        return rows

    header = text[0].strip()

    if header == EXPECTED_HEADER_5:
        has_short_exempt = False
    elif header == EXPECTED_HEADER_6:
        has_short_exempt = True
    else:
        # Fichier non reconnu: on skip.
        return rows

    for line in text[1:]:
        line = line.strip()
        if not line:
            continue

        parts = line.split("|")
        if has_short_exempt and len(parts) != 6:
            continue
        if not has_short_exempt and len(parts) != 5:
            continue

        if has_short_exempt:
            trade_date, symbol, short_volume, short_exempt_volume, total_volume, market = parts
        else:
            trade_date, symbol, short_volume, total_volume, market = parts
            short_exempt_volume = None

        rows.append(
            (
                trade_date.strip(),
                symbol.strip().upper(),
                int(short_volume),
                int(short_exempt_volume) if short_exempt_volume not in (None, "", " ") else None,
                int(total_volume),
                market.strip().upper(),
                str(path.resolve()),
            )
        )

    return rows


def files_already_loaded(
    con: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
) -> set[str]:
    rows = con.execute(
        """
        SELECT DISTINCT source_file
        FROM finra_daily_short_volume_source_raw
        WHERE source_name = ?
        """,
        [source_name],
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0] is not None}


def insert_rows(
    con: duckdb.DuckDBPyConnection,
    *,
    source_name: str,
    records: list[tuple],
) -> int:
    if not records:
        return 0

    con.executemany(
        """
        INSERT INTO finra_daily_short_volume_source_raw (
            trade_date,
            symbol,
            short_volume,
            short_exempt_volume,
            total_volume,
            market,
            source_file,
            source_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                record[0],
                record[1],
                record[2],
                record[3],
                record[4],
                record[5],
                record[6],
                source_name,
            )
            for record in records
        ],
    )
    return len(records)


def main() -> int:
    args = parse_args()

    con = duckdb.connect(args.db_path)
    try:
        ShortDataSchemaManager(con).ensure_tables()
        ensure_incremental_index(con)

        discovered = expand_sources(args.sources, args.manifests, args.default_root)
        discovered = [path for path in discovered if is_supported_finra_daily_short_file(path)]

        print(f"[load_finra_daily_short_volume_raw] discovered_files={len(discovered)}", flush=True)

        loaded_files = files_already_loaded(con, source_name=args.source_name)
        to_load = [path for path in discovered if str(path.resolve()) not in loaded_files]

        print(f"[load_finra_daily_short_volume_raw] already_loaded_files={len(loaded_files)}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] files_to_load={len(to_load)}", flush=True)

        inserted_rows_total = 0
        loaded_files_count = 0
        skipped_empty_or_invalid = 0

        for batch_start in range(0, len(to_load), BATCH_SIZE):
            batch_paths = to_load[batch_start: batch_start + BATCH_SIZE]

            for path in tqdm(
                batch_paths,
                desc="finra_daily_short_raw",
                unit="file",
                dynamic_ncols=True,
                mininterval=0.5,
            ):
                records = read_finra_file(path)
                if not records:
                    skipped_empty_or_invalid += 1
                    _log(f"[skip_invalid_or_empty] path={path}", args.verbose)
                    continue

                inserted = insert_rows(con, source_name=args.source_name, records=records)
                inserted_rows_total += inserted
                loaded_files_count += 1

                _log(
                    f"[loaded] path={path} inserted_rows={inserted}",
                    args.verbose,
                )

        payload = {
            "source_name": args.source_name,
            "discovered_files": len(discovered),
            "already_loaded_files": len(loaded_files),
            "files_loaded_now": loaded_files_count,
            "files_skipped_invalid_or_empty": skipped_empty_or_invalid,
            "inserted_rows_total": inserted_rows_total,
        }
        print(json.dumps(payload, indent=2), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
