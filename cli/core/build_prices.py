#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, date
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

    p.add_argument("--verbose", action="store_true")

    return p.parse_args()


def parse_date(x):
    return date.fromisoformat(x) if x else None


def main():
    args = parse_args()

    Path("logs").mkdir(exist_ok=True)

    repo = DuckDbPriceRepository(args.db_path)
    provider = YfinancePriceProvider()
    adapter = ProviderFrameAdapter(provider)

    window_service = PriceRefreshWindowService(
        repo,
        catchup_max_days=args.catchup_max_days,
    )

    today = date.today()

    result = window_service.resolve_window(
        requested_start_date=parse_date(args.start_date),
        requested_end_date=parse_date(args.end_date),
        as_of=parse_date(args.as_of),
        lookback_days=args.lookback_days,
        symbols=args.symbols,
        today=today,
    )

    # ------------------------------------------------------------------
    # LOG WINDOW
    # ------------------------------------------------------------------
    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "effective_start_date": str(result.effective_start_date),
        "effective_end_date": str(result.effective_end_date),
        "gap_days": result.gap_days,
        "is_noop": result.is_noop,
        "requires_range_fetch": result.requires_range_fetch,
        "window_reason": result.window_reason,
        "catchup_capped": result.catchup_capped,
    }

    with open("logs/build_prices.log", "a") as f:
        f.write(json.dumps(log_data) + "\n")

    # ------------------------------------------------------------------
    # NOOP
    # ------------------------------------------------------------------
    if result.is_noop:
        print(json.dumps(log_data, indent=2))
        return

    # ------------------------------------------------------------------
    # FETCH
    # ------------------------------------------------------------------
    df = adapter.fetch_prices(
        symbols=args.symbols,
        start_date=result.effective_start_date,
        end_date=result.effective_end_date,
        requires_range_fetch=result.requires_range_fetch,
    )

    # tqdm clean (même si dataframe)
    for _ in tqdm(range(1), desc="processing"):
        pass

    rows = 0 if df is None else len(df)

    # ------------------------------------------------------------------
    # FINAL OUTPUT
    # ------------------------------------------------------------------
    output = {
        **log_data,
        "rows_fetched": rows,
    }

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
