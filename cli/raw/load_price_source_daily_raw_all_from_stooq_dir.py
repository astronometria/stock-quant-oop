#!/usr/bin/env python3
from __future__ import annotations

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
    def tqdm(iterable, **kwargs):
        return iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load full extracted Stooq directory into price_source_daily_raw_all."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--root-dir",
        default="~/stock-quant/data/extracted/stooq/data/daily/us",
        help="Root extracted Stooq daily US directory.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing bronze rows before insert.",
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
        help="Skip ETF directories.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols to include, e.g. AAPL,MSFT,BABA",
    )
    parser.add_argument(
        "--symbols-file",
        default="",
        help="Optional file with one symbol per line.",
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


def normalize_symbol(ticker: str) -> str:
    raw = ticker.strip().upper()
    if raw.endswith(".US"):
        raw = raw[:-3]
    return raw


def infer_asset_class(source_path: str) -> str:
    low = source_path.lower()
    if "etfs" in low:
        return "ETF"
    if "stocks" in low:
        return "STOCK"
    return "UNKNOWN"


def infer_venue_group(source_path: str) -> str:
    low = source_path.lower()
    if "nasdaq" in low:
        return "NASDAQ"
    if "nysemkt" in low:
        return "NYSEMKT"
    if "nyse" in low:
        return "NYSE"
    return "UNKNOWN"


def load_symbol_filter(args: argparse.Namespace) -> set[str]:
    symbols: set[str] = set()

    if args.symbols.strip():
        for item in args.symbols.split(","):
            sym = item.strip().upper()
            if sym:
                symbols.add(sym)

    if args.symbols_file.strip():
        path = Path(args.symbols_file).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"symbols file not found: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            sym = line.strip().upper()
            if sym:
                symbols.add(sym)

    return symbols


def iter_txt_files(root_dir: Path, exclude_etfs: bool, symbol_filter: set[str]) -> list[Path]:
    files = sorted(root_dir.rglob("*.txt"))

    if exclude_etfs:
        files = [p for p in files if "etfs" not in str(p).lower()]

    if symbol_filter:
        filtered: list[Path] = []
        for path in files:
            sym = normalize_symbol(path.stem)
            if sym in symbol_filter:
                filtered.append(path)
        files = filtered

    return files


def parse_text(
    text: str,
    source_path: str,
    ingested_at: datetime,
) -> list[tuple]:
    rows: list[tuple] = []
    reader = csv.DictReader(io.StringIO(text))
    asset_class = infer_asset_class(source_path)
    venue_group = infer_venue_group(source_path)

    for record in reader:
        try:
            period = (record.get("<PER>") or "").strip().upper()
            if period != "D":
                continue

            symbol = normalize_symbol(record["<TICKER>"])
            yyyymmdd = (record["<DATE>"] or "").strip()
            if len(yyyymmdd) != 8 or not yyyymmdd.isdigit():
                continue

            price_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
            open_price = float(record["<OPEN>"])
            high_price = float(record["<HIGH>"])
            low_price = float(record["<LOW>"])
            close_price = float(record["<CLOSE>"])
            volume = int(float(record["<VOL>"]))
        except Exception:
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


def flush_batch(con, batch: list[tuple]) -> int:
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


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    root_dir = Path(args.root_dir).expanduser().resolve()
    if not root_dir.exists():
        raise SystemExit(f"root dir not found: {root_dir}")

    symbol_filter = load_symbol_filter(args)
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
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW_ALL}")

        files = iter_txt_files(root_dir, exclude_etfs=args.exclude_etfs, symbol_filter=symbol_filter)
        if args.limit_files > 0:
            files = files[:args.limit_files]

        for path in tqdm(files, desc="stooq bronze dir files", unit="file", dynamic_ncols=True):
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            source_path = str(path)
            rows = parse_text(text, source_path=source_path, ingested_at=now)
            if not rows:
                continue

            if args.limit_rows > 0:
                remaining = args.limit_rows - inserted_rows - len(batch)
                if remaining <= 0:
                    break
                rows = rows[:remaining]

            for row in rows:
                batch.append(row)
                distinct_symbols.add(row[0])

            parsed_files += 1

            if len(batch) >= args.batch_size:
                inserted_rows += flush_batch(con, batch)

            if args.limit_rows > 0 and inserted_rows >= args.limit_rows:
                break

        inserted_rows += flush_batch(con, batch)
        total_rows = con.execute(f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_ALL}").fetchone()[0]

    if args.verbose:
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] root_dir={root_dir}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] symbol_filter_count={len(symbol_filter)}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] parsed_files={parsed_files}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] distinct_symbols={len(distinct_symbols)}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] inserted_rows={inserted_rows}")
        print(f"[load_price_source_daily_raw_all_from_stooq_dir] total_rows={total_rows}")

    print(
        "Loaded price_source_daily_raw_all from extracted Stooq dir: "
        f"parsed_files={parsed_files} distinct_symbols={len(distinct_symbols)} "
        f"inserted_rows={inserted_rows} total_rows={total_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
