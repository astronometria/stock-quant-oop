#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# load_price_source_daily_raw_all_from_stooq_dir.py
#
# Objectif
# --------
# Charger l'historique complet Stooq déjà décompressé depuis le disque vers la
# table bronze `price_source_daily_raw_all`.
#
# Cette version est adaptée au layout réel du projet:
#   ~/stock-quant-oop/data/raw/stooq/daily/us
#
# Pourquoi ce script existe
# -------------------------
# Le zip Stooq a déjà été extrait sur disque. On veut donc:
# - éviter de relire le zip si les fichiers txt sont déjà présents
# - garder une ingestion "prod-only"
# - pousser un maximum de logique en SQL / DuckDB
# - conserver Python mince: découverte fichiers + parsing texte simple +
#   batching des inserts
#
# Philosophie SQL-first
# ---------------------
# On n'essaie pas ici de faire trop "d'intelligence métier" en Python.
# Python:
# - découvre les fichiers
# - parse chaque fichier texte Stooq
# - accumule des batches
# - insert dans DuckDB
#
# DuckDB / étapes aval:
# - filtrage effectif
# - déduplication
# - construction price_history / price_latest
#
# Notes importantes
# -----------------
# - Aucun fixture.
# - Aucun fallback vers un ancien dossier stock-quant.
# - On travaille uniquement avec le layout stock-quant-oop.
# - Les symboles Stooq sont normalisés en uppercase sans suffixe ".US".
# - Les fichiers ETF peuvent être exclus avec --exclude-etfs.
#
# Format Stooq attendu
# --------------------
# Les fichiers contiennent habituellement des colonnes comme:
#   <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>,<OPENINT>
#
# On ne garde que:
# - périodes journalières (<PER> == D)
# - OHLCV
#
# Très commenté volontairement pour faciliter la maintenance par d'autres
# développeurs.
# =============================================================================

import argparse
import csv
import io
from datetime import datetime
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW_ALL
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork

try:
    from tqdm import tqdm
except Exception:
    # Fallback très léger si tqdm n'est pas disponible.
    # On garde la même signature minimale pour éviter de casser le script.
    def tqdm(iterable, **kwargs):
        return iterable


# -----------------------------------------------------------------------------
# Helpers de parsing / arguments
# -----------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Points importants:
    - --root-dir pointe par défaut sur le nouveau layout réel du projet.
    - --truncate efface uniquement la table bronze cible.
    - --limit-files et --limit-rows servent surtout au probe / debug.
    - --batch-size permet de contrôler le compromis mémoire / vitesse.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Load the full extracted Stooq directory into price_source_daily_raw_all "
            "using the real stock-quant-oop raw directory layout."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--root-dir",
        default="~/stock-quant-oop/data/raw/stooq/daily/us",
        help="Extracted Stooq root directory.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing bronze price rows before insert.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Maximum number of txt files to parse. 0 means no limit.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=0,
        help="Maximum number of rows to insert. 0 means no limit.",
    )
    parser.add_argument(
        "--exclude-etfs",
        action="store_true",
        help="Skip files under ETF directories.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50000,
        help="Insert batch size.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


# -----------------------------------------------------------------------------
# Helpers de normalisation
# -----------------------------------------------------------------------------
def normalize_symbol(ticker: str) -> str:
    """
    Convertit un ticker Stooq vers le format canonique interne.

    Exemples:
    - 'aapl.us' -> 'AAPL'
    - 'msft'    -> 'MSFT'

    Pourquoi:
    - stabiliser les jointures futures
    - éviter les doublons liés à la casse
    """
    raw = ticker.strip().upper()
    if raw.endswith(".US"):
        raw = raw[:-3]
    return raw


def infer_asset_class(source_path: str) -> str:
    """
    Déduit une classe d'actif grossière depuis le chemin source.

    Exemple:
    - '.../nasdaq etfs/spy.us.txt'   -> ETF
    - '.../nyse stocks/ko.us.txt'    -> STOCK

    Cette information bronze est utile pour les étapes aval sans avoir à
    reparcourir l'arborescence d'origine.
    """
    low = source_path.lower()
    if "etfs" in low:
        return "ETF"
    if "stocks" in low:
        return "STOCK"
    return "UNKNOWN"


def infer_venue_group(source_path: str) -> str:
    """
    Déduit le groupe de marché depuis le parent du fichier.

    Exemple:
    - .../nasdaq stocks/aapl.us.txt   -> NASDAQ STOCKS
    - .../nyse etfs/spy.us.txt        -> NYSE ETFS

    On garde volontairement une logique simple et explicite.
    """
    parent = Path(source_path).parent.name.strip()
    return parent.upper() if parent else "UNKNOWN"


# -----------------------------------------------------------------------------
# Découverte des fichiers
# -----------------------------------------------------------------------------
def iter_txt_files(root_dir: Path, exclude_etfs: bool) -> list[Path]:
    """
    Retourne tous les fichiers Stooq .txt sous root_dir.

    Règles:
    - parcours récursif
    - tri stable pour reproductibilité
    - exclusion optionnelle des ETF
    """
    files = [p for p in root_dir.rglob("*.txt") if p.is_file()]
    files.sort()

    if exclude_etfs:
        files = [p for p in files if "etfs" not in str(p).lower()]

    return files


# -----------------------------------------------------------------------------
# Parsing d'un fichier texte Stooq
# -----------------------------------------------------------------------------
def parse_member_text(
    text: str,
    source_path: str,
    ingested_at: datetime,
) -> list[tuple]:
    """
    Parse le contenu d'un fichier txt Stooq vers des tuples prêts à insérer.

    Colonnes produites pour price_source_daily_raw_all:
    - symbol
    - price_date
    - open
    - high
    - low
    - close
    - volume
    - source_name
    - source_path
    - asset_class
    - venue_group
    - ingested_at

    Stratégie de robustesse:
    - ignorer les lignes invalides
    - ne garder que les lignes daily (<PER> == D)
    - ne pas faire échouer tout le fichier pour une seule ligne corrompue
    """
    rows: list[tuple] = []

    reader = csv.DictReader(io.StringIO(text))
    asset_class = infer_asset_class(source_path)
    venue_group = infer_venue_group(source_path)

    for record in reader:
        try:
            # On ne conserve que les séries journalières.
            period = (record.get("<PER>") or "").strip().upper()
            if period != "D":
                continue

            symbol = normalize_symbol(record["<TICKER>"])

            yyyymmdd = (record.get("<DATE>") or "").strip()
            if len(yyyymmdd) != 8 or not yyyymmdd.isdigit():
                continue

            price_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"

            open_price = float(record["<OPEN>"])
            high_price = float(record["<HIGH>"])
            low_price = float(record["<LOW>"])
            close_price = float(record["<CLOSE>"])
            volume = int(float(record["<VOL>"]))
        except Exception:
            # Une ligne mal formée ne doit pas casser l'ingestion du fichier.
            continue

        rows.append(
            (
                symbol,
                price_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                "stooq_dir_full",
                source_path,
                asset_class,
                venue_group,
                ingested_at,
            )
        )

    return rows


# -----------------------------------------------------------------------------
# Insertion batch
# -----------------------------------------------------------------------------
def flush_batch(con, batch: list[tuple]) -> int:
    """
    Insère le batch courant dans la table bronze.

    On retourne le nombre de lignes insérées pour simplifier le reporting.
    """
    if not batch:
        return 0

    con.executemany(
        f"""
        INSERT INTO {PRICE_SOURCE_DAILY_RAW_ALL} (
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        batch,
    )

    inserted = len(batch)
    batch.clear()
    return inserted


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> int:
    args = parse_args()

    # Validation simple des paramètres numériques pour éviter un comportement
    # bizarre silencieux.
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0")
    if args.limit_files < 0:
        raise SystemExit("--limit-files must be >= 0")
    if args.limit_rows < 0:
        raise SystemExit("--limit-rows must be >= 0")

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    root_dir = Path(args.root_dir).expanduser().resolve()
    if not root_dir.exists():
        raise SystemExit(f"stooq root dir not found: {root_dir}")
    if not root_dir.is_dir():
        raise SystemExit(f"stooq root dir is not a directory: {root_dir}")

    # Timestamp d'ingestion unique pour ce run.
    now = datetime.utcnow()

    parsed_files = 0
    inserted_rows = 0
    distinct_symbols: set[str] = set()
    batch: list[tuple] = []

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            # Important: on ne touche qu'à la table bronze cible.
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW_ALL}")

        files = iter_txt_files(root_dir=root_dir, exclude_etfs=args.exclude_etfs)
        if args.limit_files > 0:
            files = files[: args.limit_files]

        if not files:
            raise SystemExit(
                f"load_price_source_daily_raw_all_from_stooq_dir: no txt files found under {root_dir}"
            )

        # Progression fichier par fichier.
        for file_path in tqdm(
            files,
            desc="stooq bronze dir files",
            unit="file",
            dynamic_ncols=True,
            mininterval=0.5,
        ):
            try:
                text = file_path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                # On saute les fichiers illisibles, mais on ne casse pas le run.
                continue

            parsed = parse_member_text(
                text=text,
                source_path=str(file_path),
                ingested_at=now,
            )
            if not parsed:
                continue

            parsed_files += 1

            for row in parsed:
                batch.append(row)
                distinct_symbols.add(row[0])

                # Limite dure de lignes si demandée, utile pour un probe.
                if args.limit_rows > 0 and (inserted_rows + len(batch)) >= args.limit_rows:
                    # On coupe précisément le batch à la limite demandée.
                    overflow = (inserted_rows + len(batch)) - args.limit_rows
                    if overflow > 0:
                        del batch[-overflow:]

                    inserted_rows += flush_batch(con, batch)
                    break

                if len(batch) >= args.batch_size:
                    inserted_rows += flush_batch(con, batch)

            # Si la limite de lignes a été atteinte, on arrête proprement.
            if args.limit_rows > 0 and inserted_rows >= args.limit_rows:
                break

        # Flush final.
        inserted_rows += flush_batch(con, batch)

        total_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_ALL}"
        ).fetchone()[0]

    if args.verbose:
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] root_dir={root_dir}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] exclude_etfs={args.exclude_etfs}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] parsed_files={parsed_files}")
        print(
            f"[load_price_source_daily_raw_all_from_stooq_dir] distinct_symbols={len(distinct_symbols)}"
        )
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] inserted_rows={inserted_rows}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] total_rows={total_rows}")

    print(
        "Loaded price_source_daily_raw_all from extracted Stooq directory: "
        f"parsed_files={parsed_files} inserted_rows={inserted_rows} total_rows={total_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
