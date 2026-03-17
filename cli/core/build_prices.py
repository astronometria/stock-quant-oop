# (fichier complet remplacé — version enrichie)

from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

from stock_quant.app.services.price_provider_symbol_service import PriceProviderSymbolService
from stock_quant.app.services.price_refresh_window_service import PriceRefreshWindowService
from stock_quant.infrastructure.providers.prices.provider_frame_adapter import ProviderFrameAdapter
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.infrastructure.repositories.duckdb_price_provider_failure_repository import DuckDbPriceProviderFailureRepository


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--symbols", nargs="*")
    p.add_argument("--lookback-days", type=int, default=5)
    p.add_argument("--catchup-max-days", type=int, default=30)
    return p.parse_args()


def main():
    args = parse_args()
    Path("logs").mkdir(exist_ok=True)

    repo = DuckDbPriceRepository(args.db_path)
    failure_repo = DuckDbPriceProviderFailureRepository(args.db_path)

    failure_repo.ensure_table()

    canonical_symbols, _ = repo.get_refresh_symbols(args.symbols)

    provider_service = PriceProviderSymbolService()
    plan = provider_service.build_yfinance_plan(canonical_symbols)

    # --------------------------------------------------
    # 🔥 FILTER FAILURES AVANT FETCH
    # --------------------------------------------------
    failed_symbols = set(
        failure_repo.get_recent_failures("yfinance")
    )

    filtered_provider_symbols = [
        s for s in plan.eligible_provider_symbols
        if s not in failed_symbols
    ]

    print(f"filtered_out_failures={len(plan.eligible_provider_symbols) - len(filtered_provider_symbols)}")

    # --------------------------------------------------
    # WINDOW
    # --------------------------------------------------
    window = PriceRefreshWindowService(
        repo,
        catchup_max_days=args.catchup_max_days,
    ).resolve_window(
        lookback_days=args.lookback_days,
        symbols=canonical_symbols,
        today=date.today(),
    )

    if window.is_noop:
        print("noop")
        return

    provider = YfinancePriceProvider()
    adapter = ProviderFrameAdapter(provider)

    df = adapter.fetch_prices(
        provider_symbols=filtered_provider_symbols,
        provider_to_canonical=plan.provider_to_canonical,
        start_date=window.effective_start_date,
        end_date=window.effective_end_date,
        requires_range_fetch=window.requires_range_fetch,
    )

    # --------------------------------------------------
    # 🔥 DETECT FAILURES
    # --------------------------------------------------
    fetched_symbols = set(df["symbol"].unique()) if df is not None and not df.empty else set()

    failures = []
    for ps in filtered_provider_symbols:
        canonical = plan.provider_to_canonical.get(ps)
        if canonical not in fetched_symbols:
            failures.append({
                "canonical_symbol": canonical,
                "provider_symbol": ps,
                "failure_reason": "no_data_returned"
            })

    failure_repo.upsert_failures("yfinance", failures)

    print(f"new_failures_logged={len(failures)}")

    # --------------------------------------------------
    # WRITE
    # --------------------------------------------------
    result = repo.upsert_price_history(df)
    repo.refresh_price_latest()

    print(result)

    for _ in tqdm(range(1), desc="processing"):
        pass


if __name__ == "__main__":
    main()
