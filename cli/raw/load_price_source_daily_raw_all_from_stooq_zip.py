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
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW_ALL
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the full Stooq zip archive into price_source_daily_raw_all."
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
    parts = Path(source_path).parts
    if len(parts) >= 2:
        return parts[-2].upper()
    return "UNKNOWN"


def iter_members(zf: zipfile.ZipFile, exclude_etfs: bool) -> list[str]:
    members = [
        name
        for name in zf.namelist()
        if name.lower().endswith(".txt") and not name.endswith("/")
    ]
    if exclude_etfs:
        members = [name for name in members if "etfs" not in name.lower()]
    members.sort()
    return members


def parse_member_text(
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
                "stooq_zip_full",
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

    zip_path = Path(args.zip_path).expanduser().resolve()
    if not zip_path.exists():
        raise SystemExit(f"zip file not found: {zip_path}")

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

        with zipfile.ZipFile(zip_path, "r") as zf:
            members = iter_members(zf, exclude_etfs=args.exclude_etfs)
            if args.limit_files > 0:
                members = members[:args.limit_files]

            for member in tqdm(members, desc="stooq bronze files", unit="file", dynamic_ncols=True):
                try:
                    text = zf.read(member).decode("utf-8", errors="replace")
                except Exception:
                    continue

                rows = parse_member_text(text, source_path=member, ingested_at=now)
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
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] zip_path={zip_path}")
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] parsed_files={parsed_files}")
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] distinct_symbols={len(distinct_symbols)}")
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] inserted_rows={inserted_rows}")
        print(f"[load_price_source_daily_raw_all_from_stooq_zip] total_rows={total_rows}")

    print(
        "Loaded price_source_daily_raw_all from full Stooq zip: "
        f"parsed_files={parsed_files} distinct_symbols={len(distinct_symbols)} "
        f"inserted_rows={inserted_rows} total_rows={total_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
