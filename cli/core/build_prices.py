#!/usr/bin/env python3
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
from stock_quant.infrastructure.repositories.duckdb_price_provider_failure_repository import (
    DuckDbPriceProviderFailureRepository,
)
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository


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


def parse_date(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def write_jsonl_log(path: str, payload: dict) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def main():
    args = parse_args()
    Path("logs").mkdir(exist_ok=True)

    # ------------------------------------------------------------------
    # Repositories
    # ------------------------------------------------------------------
    repo = DuckDbPriceRepository(args.db_path)
    failure_repo = DuckDbPriceProviderFailureRepository(args.db_path)
    failure_repo.ensure_table()

    # ------------------------------------------------------------------
    # Resolve canonical symbol scope
    # ------------------------------------------------------------------
    canonical_symbols, symbol_scope_source = repo.get_refresh_symbols(args.symbols)

    provider_service = PriceProviderSymbolService()
    plan = provider_service.build_yfinance_plan(canonical_symbols)

    # ------------------------------------------------------------------
    # Filter provider symbols already known as repeated recent failures
    # ------------------------------------------------------------------
    failed_provider_symbols = set(
        failure_repo.get_recent_failures("yfinance")
    )

    filtered_provider_symbols = [
        symbol
        for symbol in plan.eligible_provider_symbols
        if symbol not in failed_provider_symbols
    ]

    filtered_out_failures = len(plan.eligible_provider_symbols) - len(filtered_provider_symbols)

    run_started_at = datetime.utcnow()

    log = {
        "timestamp": run_started_at.isoformat(),
        "symbol_scope_source": symbol_scope_source,
        "research_scope_symbol_count": len(canonical_symbols),
        "provider_candidate_symbol_count": plan.total_input_count,
        "provider_eligible_symbol_count": plan.eligible_count,
        "provider_excluded_symbol_count": plan.excluded_count,
        "mapping_applied_count": plan.mapping_applied_count,
        "exclusion_breakdown": plan.exclusion_breakdown,
        "filtered_out_failures": filtered_out_failures,
    }

    # ------------------------------------------------------------------
    # Optional logging of provider exclusions decided before fetch
    # ------------------------------------------------------------------
    if plan.excluded_count > 0:
        exclusion_payload = {
            "timestamp": run_started_at.isoformat(),
            "provider_name": plan.provider_name,
            "excluded_count": plan.excluded_count,
            "exclusion_breakdown": plan.exclusion_breakdown,
            "sample_excluded_records": [
                {
                    "canonical_symbol": record.canonical_symbol,
                    "provider_symbol": record.provider_symbol,
                    "exclusion_reason": record.exclusion_reason,
                }
                for record in plan.records
                if not record.is_fetchable
            ][:100],
        }
        write_jsonl_log("logs/provider_symbol_mapping.log", exclusion_payload)

    # ------------------------------------------------------------------
    # Resolve refresh window
    # IMPORTANT:
    # resolve_window requires explicit keyword-only args even when None.
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

    log.update(
        {
            "effective_start_date": str(window.effective_start_date),
            "effective_end_date": str(window.effective_end_date),
            "gap_days": window.gap_days,
            "window_reason": window.window_reason,
            "is_noop": window.is_noop,
            "requires_range_fetch": window.requires_range_fetch,
        }
    )

    # ------------------------------------------------------------------
    # No-op cases
    # ------------------------------------------------------------------
    if window.is_noop or not filtered_provider_symbols:
        write_jsonl_log("logs/build_prices.log", log)
        print(json.dumps(log, indent=2))
        return

    # ------------------------------------------------------------------
    # Fetch from provider
    # ------------------------------------------------------------------
    provider = YfinancePriceProvider()
    adapter = ProviderFrameAdapter(provider)

    log["provider_fetch_input_count"] = len(filtered_provider_symbols)

    df = adapter.fetch_prices(
        provider_symbols=filtered_provider_symbols,
        provider_to_canonical=plan.provider_to_canonical,
        start_date=window.effective_start_date,
        end_date=window.effective_end_date,
        requires_range_fetch=window.requires_range_fetch,
    )

    log["rows_fetched"] = 0 if df is None else len(df)

    # ------------------------------------------------------------------
    # Failure memory: any provider symbol asked but not returned gets logged.
    # NOTE:
    # df["symbol"] is canonical after adapter remap.
    # ------------------------------------------------------------------
    fetched_canonical_symbols = (
        set(df["symbol"].dropna().astype(str).unique())
        if df is not None and not df.empty
        else set()
    )

    failures = []
    for provider_symbol in filtered_provider_symbols:
        canonical_symbol = plan.provider_to_canonical.get(provider_symbol)
        if canonical_symbol not in fetched_canonical_symbols:
            failures.append(
                {
                    "canonical_symbol": canonical_symbol,
                    "provider_symbol": provider_symbol,
                    "failure_reason": "no_data_returned",
                }
            )

    failure_repo.upsert_failures("yfinance", failures)
    log["new_failures_logged"] = len(failures)

    # ------------------------------------------------------------------
    # Write canonical rows into price_history and refresh price_latest
    # ------------------------------------------------------------------
    write_result = repo.upsert_price_history(
        df,
        source_name="yfinance",
        ingested_at=run_started_at,
    )
    log["write_result"] = write_result

    latest_symbols = (
        list(df["symbol"].dropna().astype(str).unique())
        if df is not None and not df.empty
        else []
    )
    latest_result = repo.refresh_price_latest(symbols=latest_symbols)
    log["price_latest_refresh_result"] = latest_result

    write_jsonl_log("logs/build_prices.log", log)

    for _ in tqdm(range(1), desc="processing"):
        pass

    print(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()
