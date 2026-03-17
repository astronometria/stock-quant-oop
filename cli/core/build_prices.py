#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.infrastructure.providers.prices.provider_frame_adapter import ProviderFrameAdapter
from stock_quant.app.services.price_refresh_window_service import PriceRefreshWindowService


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--symbols", nargs="*")
    p.add_argument("--start-date")
    p.add_argument("--end-date")
    p.add_argument("--as-of")
    p.add_argument("--lookback-days", type=int, default=5)
    p.add_argument("--catchup-max-days", type=int, default=30)
    return p.parse_args()


def parse_date(x):
    return date.fromisoformat(x) if x else None


def main():
    args = parse_args()

    Path("logs").mkdir(exist_ok=True)

    repo = DuckDbPriceRepository(args.db_path)
    provider = YfinancePriceProvider()
    adapter = ProviderFrameAdapter(provider)

    window = PriceRefreshWindowService(
        repo,
        catchup_max_days=args.catchup_max_days
    ).resolve_window(
        requested_start_date=parse_date(args.start_date),
        requested_end_date=parse_date(args.end_date),
        as_of=parse_date(args.as_of),
        lookback_days=args.lookback_days,
        symbols=args.symbols,
        today=date.today(),
    )

    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "effective_start_date": str(window.effective_start_date),
        "effective_end_date": str(window.effective_end_date),
        "gap_days": window.gap_days,
        "window_reason": window.window_reason,
        "is_noop": window.is_noop,
        "requires_range_fetch": window.requires_range_fetch,
        "catchup_capped": window.catchup_capped,
        "last_any_date": str(window.last_any_date),
        "last_complete_date": str(window.last_complete_date),
        "expected_symbol_count": window.expected_count,
        "observed_latest_count": window.observed_latest_count,
    }

    with open("logs/build_prices.log", "a") as f:
        f.write(json.dumps(log) + "\n")

    if window.is_noop:
        print(json.dumps(log, indent=2))
        return

    df = adapter.fetch_prices(
        symbols=args.symbols,
        start_date=window.effective_start_date,
        end_date=window.effective_end_date,
        requires_range_fetch=window.requires_range_fetch,
    )

    for _ in tqdm(range(1), desc="processing"):
        pass

    log["rows_fetched"] = 0 if df is None else len(df)

    print(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()
