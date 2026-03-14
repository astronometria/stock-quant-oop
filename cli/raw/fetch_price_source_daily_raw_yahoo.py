#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import time

import pandas as pd
import yfinance as yf

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import (
    MARKET_UNIVERSE,
    PRICE_LATEST,
    PRICE_SOURCE_DAILY_RAW_YAHOO,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", default=None)
    p.add_argument("--max-symbols", type=int, default=None)
    p.add_argument("--sleep", type=float, default=0.5)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def normalize_download_df(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


def main():
    args = parse_args()
    config = build_app_config(db_path=args.db_path)

    session_factory = DuckDbSessionFactory(config.db_path)

    inserted = 0
    skipped = 0
    errors = 0

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        rows = con.execute(
            f"""
            SELECT mu.symbol,
                   COALESCE(
                       (SELECT MAX(price_date) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO} y WHERE y.symbol = mu.symbol),
                       (SELECT MAX(latest_price_date) FROM {PRICE_LATEST} l WHERE l.symbol = mu.symbol)
                   )
            FROM {MARKET_UNIVERSE} mu
            WHERE mu.include_in_universe = TRUE
            ORDER BY mu.symbol
            """
        ).fetchall()

        if args.max_symbols:
            rows = rows[: args.max_symbols]

        for symbol, last_date in tqdm(rows, desc="fetch yahoo", unit="sym", dynamic_ncols=True):
            if last_date:
                start = last_date + dt.timedelta(days=1)
            else:
                start = dt.date(1980, 1, 1)

            end = dt.date.today()

            if start > end:
                skipped += 1
                continue

            success = False

            for attempt in range(args.retries):
                try:
                    df = yf.download(
                        symbol,
                        start=start.isoformat(),
                        end=(end + dt.timedelta(days=1)).isoformat(),
                        progress=False,
                        auto_adjust=False,
                    )

                    df = normalize_download_df(df)

                    if df.empty:
                        skipped += 1
                        success = True
                        break

                    rows_to_insert = []

                    for idx, r in df.iterrows():
                        try:
                            open_v = float(r["Open"])
                            high_v = float(r["High"])
                            low_v = float(r["Low"])
                            close_v = float(r["Close"])
                            vol_v = int(r["Volume"])
                        except Exception:
                            continue

                        rows_to_insert.append(
                            (
                                symbol,
                                idx.date(),
                                open_v,
                                high_v,
                                low_v,
                                close_v,
                                vol_v,
                                "yahoo_daily_live",
                            )
                        )

                    if not rows_to_insert:
                        skipped += 1
                        success = True
                        break

                    con.execute("DROP TABLE IF EXISTS yahoo_fetch_stage")
                    con.execute(
                        """
                        CREATE TEMP TABLE yahoo_fetch_stage (
                            symbol VARCHAR,
                            price_date DATE,
                            open DOUBLE,
                            high DOUBLE,
                            low DOUBLE,
                            close DOUBLE,
                            volume BIGINT,
                            source_name VARCHAR
                        )
                        """
                    )

                    con.executemany(
                        """
                        INSERT INTO yahoo_fetch_stage (
                            symbol,
                            price_date,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            source_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows_to_insert,
                    )

                    before_count = con.execute(
                        f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}"
                    ).fetchone()[0]

                    con.execute(
                        f"""
                        INSERT INTO {PRICE_SOURCE_DAILY_RAW_YAHOO} (
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
                            s.source_name,
                            CURRENT_TIMESTAMP
                        FROM yahoo_fetch_stage s
                        LEFT JOIN {PRICE_SOURCE_DAILY_RAW_YAHOO} y
                          ON y.symbol = s.symbol
                         AND y.price_date = s.price_date
                        WHERE y.symbol IS NULL
                        """
                    )

                    after_count = con.execute(
                        f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}"
                    ).fetchone()[0]

                    inserted += int(after_count - before_count)
                    success = True
                    break

                except Exception:
                    time.sleep(2 * (attempt + 1))

            if not success:
                errors += 1
                if args.verbose:
                    print(f"fetch error: {symbol}")

            time.sleep(args.sleep)

    print(
        f"Yahoo fetch summary: inserted_rows={inserted} skipped_symbols={skipped} errors={errors}"
    )


if __name__ == "__main__":
    main()
