#!/usr/bin/env python3
from __future__ import annotations

"""
build_prices CLI (incremental + provider-aware)

Flow :
1. resolve canonical symbols
2. build provider plan (Yahoo)
3. compute refresh window
4. fetch via adapter (batching inside provider)
5. log metrics

IMPORTANT :
- symbol canonique != symbol provider
"""

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.infrastructure.providers.prices.provider_frame_adapter import ProviderFrameAdapter
from stock_quant.app.services.price_refresh_window_service import PriceRefreshWindowService
from stock_quant.app.services.price_provider_symbol_service import PriceProviderSymbolService


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

    # ------------------------------------------------------------------
    # 1. resolve canonical scope
    # ------------------------------------------------------------------
    canonical_symbols, symbol_scope_source = repo.get_refresh_symbols(args.symbols)

    provider_service = PriceProviderSymbolService()
    plan = provider_service.build_yfinance_plan(canonical_symbols)

    # ------------------------------------------------------------------
    # 2. log provider plan
    # ------------------------------------------------------------------
    log = {
        "timestamp": datetime.utcnow().isoformat(),
        "symbol_scope_source": symbol_scope_source,
        "research_scope_symbol_count": len(canonical_symbols),
        "provider_candidate_symbol_count": plan.total_input_count,
        "provider_eligible_symbol_count": plan.eligible_count,
        "provider_excluded_symbol_count": plan.excluded_count,
        "mapping_applied_count": plan.mapping_applied_count,
        "exclusion_breakdown": plan.exclusion_breakdown,
    }

    # ------------------------------------------------------------------
    # 3. compute window
    # ------------------------------------------------------------------
    window = PriceRefreshWindowService(
        repo,
        catchup_max_days=args.catchup_max_days,
    ).resolve_window(
        requested_start_date=parse_date(args.start_date),
        requested_end_date=parse_date(args.end_date),
        as_of=parse_date(args.as_of),
        lookback_days=args.lookback_days,
        symbols=canonical_symbols,
        today=date.today(),
    )

    log.update({
        "effective_start_date": str(window.effective_start_date),
        "effective_end_date": str(window.effective_end_date),
        "gap_days": window.gap_days,
        "window_reason": window.window_reason,
        "is_noop": window.is_noop,
        "requires_range_fetch": window.requires_range_fetch,
    })

    # ------------------------------------------------------------------
    # noop
    # ------------------------------------------------------------------
    if window.is_noop or not plan.eligible_provider_symbols:
        print(json.dumps(log, indent=2))
        return

    # ------------------------------------------------------------------
    # 4. fetch (provider-aware)
    # ------------------------------------------------------------------
    provider = YfinancePriceProvider()
    adapter = ProviderFrameAdapter(provider)

    df = adapter.fetch_prices(
        provider_symbols=plan.eligible_provider_symbols,
        provider_to_canonical=plan.provider_to_canonical,
        start_date=window.effective_start_date,
        end_date=window.effective_end_date,
        requires_range_fetch=window.requires_range_fetch,
    )

    log["rows_fetched"] = 0 if df is None else len(df)

    # tqdm placeholder (clean terminal)
    for _ in tqdm(range(1), desc="processing"):
        pass

    print(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()
