#!/usr/bin/env python3
from __future__ import annotations

"""
fetch_price_source_daily_raw_yahoo.py

Objectif
--------
Télécharger des prix journaliers via yfinance en mode strictement raw-first.

Règle d'architecture
--------------------
Ce script NE DOIT PAS :
- écrire dans DuckDB
- écrire dans price_history
- écrire dans price_latest

Ce script DOIT :
- fetcher depuis le réseau
- persister le payload brut sur disque local
- produire un manifest de run
- être réutilisable pour un rebuild offline ultérieur

Structure disque cible
----------------------
Par défaut, le script écrit sous :

~/stock-quant-oop-raw/data/raw/prices/yfinance/daily/YYYY-MM-DD/

avec :
- batches/<run_id>__batch_00001.jsonl
- manifests/<run_id>__manifest.json

Format JSONL
------------
Une ligne par barre journalière normalisée minimalement.

On garde un payload "brut d'ingestion" suffisamment riche pour :
- reload en table raw plus tard
- auditer ce qui a été reçu du provider
- éviter un refetch si la DB est détruite

Notes importantes
-----------------
- Python mince ici, car on fait :
  * réseau
  * sérialisation disque
  * orchestration
- la transformation SQL-first viendra dans le loader DB séparé
- beaucoup de commentaires pour aider les autres développeurs
"""

import argparse
import json
import math
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf
from tqdm import tqdm


# ============================================================================
# Structures simples et helpers
# ============================================================================


@dataclass
class RunConfig:
    """
    Configuration résolue du run.

    On la garde séparée pour avoir un objet simple à journaliser
    dans le manifest.
    """

    data_root: str
    run_date: str
    start_date: str
    end_date: str
    symbols_count: int
    batch_size: int
    pause_seconds: float
    max_retries: int
    overwrite_existing_files: bool


@dataclass
class BatchSummary:
    """
    Résumé d'un batch yfinance.

    Ce résumé va dans le manifest final.
    """

    batch_index: int
    requested_symbols: list[str]
    provider_symbols: list[str]
    output_path: str | None
    rows_written: int
    symbols_with_rows: int
    symbols_without_rows: list[str]
    failed_symbols: list[str]
    attempts_used: int
    status: str
    error_message: str | None
    elapsed_seconds: float


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    On garde une interface simple et orientée raw-first.
    """
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo/yfinance daily prices to local raw JSONL files only."
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="Optional explicit symbols to fetch. If omitted, use --symbols-file.",
    )
    parser.add_argument(
        "--symbols-file",
        help="Optional text file containing one symbol per line.",
    )
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop-raw/data/raw/prices/yfinance",
        help="Root directory where raw yfinance files are persisted.",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of symbols per provider batch.",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.5,
        help="Sleep between batches to be conservative with Yahoo.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts per batch.",
    )
    parser.add_argument(
        "--overwrite-existing-files",
        action="store_true",
        help="If enabled, rewrite existing batch files for this exact run id layout.",
    )
    return parser.parse_args()


def resolve_symbols(args: argparse.Namespace) -> list[str]:
    """
    Résout les symboles à fetcher.

    Règles :
    - on accepte --symbols
    - on accepte --symbols-file
    - on normalise en uppercase
    - on retire vides / doublons
    """
    collected: list[str] = []

    if args.symbols:
        collected.extend(args.symbols)

    if args.symbols_file:
        path = Path(args.symbols_file).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"symbols file not found: {path}")
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                collected.append(line.strip())

    normalized: list[str] = []
    seen: set[str] = set()

    for raw_symbol in collected:
        symbol = normalize_symbol(raw_symbol)
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)

    if not normalized:
        raise SystemExit(
            "No symbols resolved. Provide --symbols and/or --symbols-file."
        )

    return normalized


def normalize_symbol(value: str | None) -> str | None:
    """
    Nettoyage prudent d'un symbole.

    On évite les transformations exotiques.
    """
    if value is None:
        return None
    symbol = value.strip().upper()
    if not symbol:
        return None
    return symbol


def chunk_list(items: list[str], batch_size: int) -> list[list[str]]:
    """
    Découpe une liste en batches stables.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be > 0")
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def today_iso() -> str:
    """
    Date du jour au format ISO.
    """
    return date.today().isoformat()


def utc_now_compact() -> str:
    """
    Timestamp compact UTC, utile pour noms de fichiers.
    """
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: Path) -> None:
    """
    Création de dossier robuste.
    """
    path.mkdir(parents=True, exist_ok=True)


def json_default(value: Any) -> Any:
    """
    Sérialiseur JSON défensif.
    """
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def safe_float(value: Any) -> float | None:
    """
    Convertit une valeur en float ou None.

    On filtre NaN/inf pour éviter d'écrire des valeurs instables
    dans le raw JSONL.
    """
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def safe_int(value: Any) -> int | None:
    """
    Convertit une valeur en int ou None.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return int(value)
    except Exception:
        try:
            as_float = float(value)
            if math.isnan(as_float) or math.isinf(as_float):
                return None
            return int(as_float)
        except Exception:
            return None


def build_run_paths(data_root: Path, run_date: str, run_id: str) -> dict[str, Path]:
    """
    Construit les dossiers de sortie du run.
    """
    day_root = data_root / "daily" / run_date
    batches_dir = day_root / "batches"
    manifests_dir = day_root / "manifests"

    ensure_dir(day_root)
    ensure_dir(batches_dir)
    ensure_dir(manifests_dir)

    return {
        "day_root": day_root,
        "batches_dir": batches_dir,
        "manifests_dir": manifests_dir,
        "manifest_path": manifests_dir / f"{run_id}__manifest.json",
    }


def write_json(path: Path, payload: Any) -> None:
    """
    Écrit un JSON pretty-print.
    """
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=json_default),
        encoding="utf-8",
    )


def detect_symbol_column(frame: pd.DataFrame) -> str | None:
    """
    Détecte la colonne symbole.

    Selon la forme renvoyée par yfinance, on peut obtenir une colonne
    explicitement nommée 'symbol' après reset_index / melt / stack.
    """
    lower_map = {str(col).strip().lower(): col for col in frame.columns}
    for candidate in ["symbol", "ticker"]:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def normalize_yfinance_frame(raw: pd.DataFrame, requested_symbols: list[str]) -> pd.DataFrame:
    """
    Normalise la sortie de yf.download(...) vers un DataFrame canonique minimal
    pour persistance disque raw.

    On supporte :
    - cas 1 symbole
    - cas multi-symboles avec colonnes MultiIndex

    Sortie attendue :
    - symbol
    - price_date
    - open
    - high
    - low
    - close
    - volume
    """
    if raw is None or raw.empty:
        return pd.DataFrame(
            columns=["symbol", "price_date", "open", "high", "low", "close", "volume"]
        )

    # ----------------------------------------------------------------------
    # Cas multi-index colonnes fréquent avec yfinance :
    # level 0 = field, level 1 = symbol
    # ----------------------------------------------------------------------
    if isinstance(raw.columns, pd.MultiIndex):
        records: list[pd.DataFrame] = []

        # On parcourt les symboles explicitement demandés pour garder une sortie
        # stable même si Yahoo répond partiellement.
        for symbol in requested_symbols:
            if symbol not in raw.columns.get_level_values(-1):
                continue

            symbol_frame = raw.xs(symbol, axis=1, level=-1, drop_level=True).copy()
            symbol_frame = symbol_frame.reset_index()

            # Harmonisation du nom de la colonne date.
            if "Date" in symbol_frame.columns:
                symbol_frame = symbol_frame.rename(columns={"Date": "price_date"})
            elif "index" in symbol_frame.columns:
                symbol_frame = symbol_frame.rename(columns={"index": "price_date"})

            symbol_frame["symbol"] = symbol
            records.append(symbol_frame)

        if not records:
            return pd.DataFrame(
                columns=["symbol", "price_date", "open", "high", "low", "close", "volume"]
            )

        frame = pd.concat(records, ignore_index=True)
    else:
        # ------------------------------------------------------------------
        # Cas simple : un seul symbole.
        # ------------------------------------------------------------------
        frame = raw.copy().reset_index()
        if "Date" in frame.columns:
            frame = frame.rename(columns={"Date": "price_date"})
        elif "index" in frame.columns:
            frame = frame.rename(columns={"index": "price_date"})

        inferred_symbol = requested_symbols[0] if len(requested_symbols) == 1 else None
        symbol_col = detect_symbol_column(frame)

        if symbol_col is None:
            frame["symbol"] = inferred_symbol
        elif symbol_col != "symbol":
            frame = frame.rename(columns={symbol_col: "symbol"})

    # ----------------------------------------------------------------------
    # Harmonisation des noms OHLCV.
    # ----------------------------------------------------------------------
    rename_map = {}
    for source_name, target_name in [
        ("Open", "open"),
        ("High", "high"),
        ("Low", "low"),
        ("Close", "close"),
        ("Adj Close", "adj_close"),
        ("Volume", "volume"),
    ]:
        if source_name in frame.columns:
            rename_map[source_name] = target_name

    frame = frame.rename(columns=rename_map)

    expected_cols = ["symbol", "price_date", "open", "high", "low", "close", "volume"]
    for col in expected_cols:
        if col not in frame.columns:
            frame[col] = None

    frame = frame[expected_cols].copy()

    # Nettoyage minimal des types.
    frame["symbol"] = frame["symbol"].map(normalize_symbol)
    frame["price_date"] = pd.to_datetime(frame["price_date"], errors="coerce").dt.date

    for numeric_col in ["open", "high", "low", "close"]:
        frame[numeric_col] = frame[numeric_col].map(safe_float)
    frame["volume"] = frame["volume"].map(safe_int)

    # Retrait des lignes invalides.
    frame = frame[
        frame["symbol"].notna()
        & frame["price_date"].notna()
        & frame["close"].notna()
    ].copy()

    frame = frame.sort_values(["symbol", "price_date"]).reset_index(drop=True)
    return frame


def write_batch_jsonl(
    batch_path: Path,
    normalized_frame: pd.DataFrame,
    requested_symbols: list[str],
    provider_symbols: list[str],
    fetch_started_at: datetime,
    fetch_finished_at: datetime,
) -> int:
    """
    Écrit le batch au format JSONL.

    Chaque ligne représente une barre journalière minimale normalisée,
    enrichie de métadonnées de provenance.
    """
    rows_written = 0

    with batch_path.open("w", encoding="utf-8") as handle:
        for row in normalized_frame.to_dict(orient="records"):
            payload = {
                "provider_name": "yfinance",
                "source_name": "yfinance_raw_disk",
                "requested_symbols": requested_symbols,
                "provider_symbols": provider_symbols,
                "fetch_started_at_utc": fetch_started_at.isoformat(),
                "fetch_finished_at_utc": fetch_finished_at.isoformat(),
                "symbol": row.get("symbol"),
                "price_date": row.get("price_date"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume"),
            }
            handle.write(json.dumps(payload, ensure_ascii=False, default=json_default) + "\n")
            rows_written += 1

    return rows_written


def fetch_batch_from_yfinance(
    symbols: list[str],
    start_date: str,
    end_date_exclusive: str,
) -> pd.DataFrame:
    """
    Fetch d'un batch via yfinance.

    On utilise threads=False pour rester plus conservateur côté provider.
    """
    return yf.download(
        tickers=symbols,
        start=start_date,
        end=end_date_exclusive,
        interval="1d",
        auto_adjust=False,
        actions=False,
        progress=False,
        threads=False,
        group_by="column",
    )


def compute_end_date_exclusive(end_date_inclusive: str) -> str:
    """
    Convertit une date de fin inclusive en borne exclusive pour yfinance.

    yfinance utilise un end exclusif.
    """
    end_obj = date.fromisoformat(end_date_inclusive)
    end_exclusive = pd.Timestamp(end_obj) + pd.Timedelta(days=1)
    return end_exclusive.date().isoformat()


def infer_symbols_with_rows(frame: pd.DataFrame) -> set[str]:
    """
    Retourne les symboles effectivement présents dans la réponse.
    """
    if frame.empty or "symbol" not in frame.columns:
        return set()
    return {
        value for value in frame["symbol"].dropna().astype(str).tolist()
        if value
    }


# ============================================================================
# Main
# ============================================================================


def main() -> int:
    """
    Point d'entrée principal.

    Étapes :
    1. résolution config
    2. découpage batches
    3. fetch provider
    4. persistance JSONL disque
    5. manifest final
    """
    args = parse_args()

    symbols = resolve_symbols(args)
    run_date = today_iso()
    run_id = f"yfinance_daily_{utc_now_compact()}"

    data_root = Path(args.data_root).expanduser().resolve()
    paths = build_run_paths(data_root=data_root, run_date=run_date, run_id=run_id)

    config = RunConfig(
        data_root=str(data_root),
        run_date=run_date,
        start_date=args.start_date,
        end_date=args.end_date,
        symbols_count=len(symbols),
        batch_size=int(args.batch_size),
        pause_seconds=float(args.pause_seconds),
        max_retries=int(args.max_retries),
        overwrite_existing_files=bool(args.overwrite_existing_files),
    )

    batches = chunk_list(symbols, int(args.batch_size))
    end_date_exclusive = compute_end_date_exclusive(args.end_date)

    run_started_at = datetime.utcnow()
    batch_summaries: list[BatchSummary] = []

    print(
        json.dumps(
            {
                "pipeline": "fetch_price_source_daily_raw_yahoo",
                "mode": "raw_disk_only",
                "run_id": run_id,
                "run_date": run_date,
                "data_root": str(data_root),
                "symbols_count": len(symbols),
                "batch_count": len(batches),
                "start_date": args.start_date,
                "end_date_inclusive": args.end_date,
                "end_date_exclusive_for_provider": end_date_exclusive,
            },
            indent=2,
        ),
        flush=True,
    )

    batch_bar = tqdm(
        enumerate(batches, start=1),
        total=len(batches),
        desc="fetch_yahoo_raw_disk",
        unit="batch",
        dynamic_ncols=True,
    )

    for batch_index, batch_symbols in batch_bar:
        batch_started_at = datetime.utcnow()
        batch_path = paths["batches_dir"] / f"{run_id}__batch_{batch_index:05d}.jsonl"

        if batch_path.exists() and not args.overwrite_existing_files:
            batch_summaries.append(
                BatchSummary(
                    batch_index=batch_index,
                    requested_symbols=batch_symbols,
                    provider_symbols=batch_symbols,
                    output_path=str(batch_path),
                    rows_written=0,
                    symbols_with_rows=0,
                    symbols_without_rows=batch_symbols,
                    failed_symbols=[],
                    attempts_used=0,
                    status="skipped_existing_file",
                    error_message=None,
                    elapsed_seconds=0.0,
                )
            )
            continue

        attempt = 0
        last_error: str | None = None
        batch_summary: BatchSummary | None = None

        while attempt < int(args.max_retries):
            attempt += 1
            fetch_started_at = datetime.utcnow()

            try:
                raw = fetch_batch_from_yfinance(
                    symbols=batch_symbols,
                    start_date=args.start_date,
                    end_date_exclusive=end_date_exclusive,
                )
                fetch_finished_at = datetime.utcnow()

                normalized = normalize_yfinance_frame(
                    raw=raw,
                    requested_symbols=batch_symbols,
                )

                symbols_with_rows_set = infer_symbols_with_rows(normalized)
                symbols_without_rows = [
                    symbol for symbol in batch_symbols
                    if symbol not in symbols_with_rows_set
                ]

                rows_written = write_batch_jsonl(
                    batch_path=batch_path,
                    normalized_frame=normalized,
                    requested_symbols=batch_symbols,
                    provider_symbols=batch_symbols,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=fetch_finished_at,
                )

                elapsed_seconds = (datetime.utcnow() - batch_started_at).total_seconds()

                batch_summary = BatchSummary(
                    batch_index=batch_index,
                    requested_symbols=batch_symbols,
                    provider_symbols=batch_symbols,
                    output_path=str(batch_path),
                    rows_written=int(rows_written),
                    symbols_with_rows=len(symbols_with_rows_set),
                    symbols_without_rows=symbols_without_rows,
                    failed_symbols=[],
                    attempts_used=attempt,
                    status="ok",
                    error_message=None,
                    elapsed_seconds=elapsed_seconds,
                )
                break

            except Exception as exc:
                last_error = repr(exc)
                if attempt < int(args.max_retries):
                    time.sleep(float(args.pause_seconds))
                else:
                    elapsed_seconds = (datetime.utcnow() - batch_started_at).total_seconds()
                    batch_summary = BatchSummary(
                        batch_index=batch_index,
                        requested_symbols=batch_symbols,
                        provider_symbols=batch_symbols,
                        output_path=None,
                        rows_written=0,
                        symbols_with_rows=0,
                        symbols_without_rows=[],
                        failed_symbols=batch_symbols,
                        attempts_used=attempt,
                        status="failed",
                        error_message=last_error,
                        elapsed_seconds=elapsed_seconds,
                    )

        if batch_summary is None:
            batch_summary = BatchSummary(
                batch_index=batch_index,
                requested_symbols=batch_symbols,
                provider_symbols=batch_symbols,
                output_path=None,
                rows_written=0,
                symbols_with_rows=0,
                symbols_without_rows=[],
                failed_symbols=batch_symbols,
                attempts_used=attempt,
                status="failed_unknown",
                error_message=last_error,
                elapsed_seconds=(datetime.utcnow() - batch_started_at).total_seconds(),
            )

        batch_summaries.append(batch_summary)

        # Pause entre batches pour rester conservateur côté Yahoo.
        if batch_index < len(batches):
            time.sleep(float(args.pause_seconds))

    run_finished_at = datetime.utcnow()

    total_rows_written = sum(item.rows_written for item in batch_summaries)
    total_failed_symbols = sum(len(item.failed_symbols) for item in batch_summaries)
    total_symbols_with_rows = sum(item.symbols_with_rows for item in batch_summaries)

    manifest = {
        "pipeline": "fetch_price_source_daily_raw_yahoo",
        "mode": "raw_disk_only",
        "run_id": run_id,
        "run_started_at_utc": run_started_at.isoformat(),
        "run_finished_at_utc": run_finished_at.isoformat(),
        "config": asdict(config),
        "paths": {key: str(value) for key, value in paths.items()},
        "batch_count": len(batches),
        "total_rows_written": int(total_rows_written),
        "total_failed_symbols": int(total_failed_symbols),
        "total_symbols_with_rows": int(total_symbols_with_rows),
        "batches": [asdict(item) for item in batch_summaries],
    }

    write_json(paths["manifest_path"], manifest)

    print(
        json.dumps(
            {
                "run_id": run_id,
                "manifest_path": str(paths["manifest_path"]),
                "batches_dir": str(paths["batches_dir"]),
                "batch_count": len(batches),
                "total_rows_written": int(total_rows_written),
                "total_failed_symbols": int(total_failed_symbols),
                "total_symbols_with_rows": int(total_symbols_with_rows),
                "mode": "raw_disk_only",
            },
            indent=2,
        ),
        flush=True,
    )

    # Code de retour non fatal même si certains symboles échouent :
    # on préfère laisser le manifest détailler les erreurs.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
