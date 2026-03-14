#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import zipfile
from datetime import datetime
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load real Stooq zip data into price_source_daily_raw."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--zip-path",
        default="~/stock-quant/data/download/stooq/d_us_txt.zip",
        help="Path to Stooq zip archive.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing staging rows before insert.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=20,
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
        help="Skip files under ETF directories for a more stock-focused pilot.",
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


def iter_members(zf: zipfile.ZipFile, exclude_etfs: bool, symbol_filter: set[str]) -> list[str]:
    members = [
        name
        for name in zf.namelist()
        if name.lower().endswith(".txt") and not name.endswith("/")
    ]

    if exclude_etfs:
        members = [name for name in members if "etfs" not in name.lower()]

    if symbol_filter:
        filtered = []
        for name in members:
            stem = Path(name).stem.upper()
            sym = normalize_symbol(stem)
            if sym in symbol_filter:
                filtered.append(name)
        members = filtered

    members.sort()
    return members


def parse_member_text(text: str) -> list[tuple[str, str, float, float, float, float, int]]:
    rows: list[tuple[str, str, float, float, float, float, int]] = []
    reader = csv.DictReader(io.StringIO(text))
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
            )
        )
    return rows


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    zip_path = Path(args.zip_path).expanduser().resolve()
    if not zip_path.exists():
        raise SystemExit(f"zip file not found: {zip_path}")

    symbol_filter = load_symbol_filter(args)
    now = datetime.utcnow()
    inserted_rows = 0
    parsed_files = 0
    kept_symbols: set[str] = set()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW}")

        with zipfile.ZipFile(zip_path, "r") as zf:
            members = iter_members(
                zf,
                exclude_etfs=args.exclude_etfs,
                symbol_filter=symbol_filter,
            )
            if args.limit_files > 0:
                members = members[:args.limit_files]

            for member in tqdm(members, desc="stooq files", unit="file", dynamic_ncols=True):
                try:
                    text = zf.read(member).decode("utf-8", errors="replace")
                except Exception:
                    continue

                parsed = parse_member_text(text)
                if not parsed:
                    continue

                rows = [
                    (
                        symbol,
                        price_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        "stooq_zip_real",
                        now,
                    )
                    for (
                        symbol,
                        price_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                    ) in parsed
                ]

                if args.limit_rows > 0:
                    remaining = args.limit_rows - inserted_rows
                    if remaining <= 0:
                        break
                    rows = rows[:remaining]

                if not rows:
                    break

                con.executemany(
                    f"""
                    INSERT INTO {PRICE_SOURCE_DAILY_RAW} (
                        symbol,
                        price_date,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        source_name,
                        ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

                inserted_rows += len(rows)
                parsed_files += 1
                for row in rows:
                    kept_symbols.add(row[0])

                if args.limit_rows > 0 and inserted_rows >= args.limit_rows:
                    break

        total_rows = con.execute(f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_price_source_daily_raw_from_stooq_zip] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] zip_path={zip_path}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] symbol_filter_count={len(symbol_filter)}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] parsed_files={parsed_files}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] distinct_symbols={len(kept_symbols)}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] inserted_rows={inserted_rows}")
        print(f"[load_price_source_daily_raw_from_stooq_zip] total_rows={total_rows}")

    print(
        "Loaded price_source_daily_raw from real Stooq zip: "
        f"parsed_files={parsed_files} distinct_symbols={len(kept_symbols)} "
        f"inserted_rows={inserted_rows} total_rows={total_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
