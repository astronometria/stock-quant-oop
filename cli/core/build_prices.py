#!/usr/bin/env python3
from __future__ import annotations

"""
CLI de build / refresh des prix.

Réécriture complète pour rendre le script plus robuste côté runtime:
- gestion propre des chemins de logs avec expansion de "~"
- création automatique du dossier de logs
- conservation de la logique métier existante
- commentaires détaillés pour les autres développeurs
"""

import argparse
import json
import os
from datetime import date, datetime
from pathlib import Path

from tqdm import tqdm

from stock_quant.app.services.price_provider_symbol_service import PriceProviderSymbolService
from stock_quant.app.services.price_refresh_window_service import PriceRefreshWindowService
from stock_quant.infrastructure.providers.prices.provider_frame_adapter import ProviderFrameAdapter
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import (
    YfinancePriceProvider,
)
from stock_quant.infrastructure.repositories.duckdb_price_provider_failure_repository import (
    DuckDbPriceProviderFailureRepository,
    is_permanent_failure,
)
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes:
    - Le wrapper de rebuild appelle maintenant ce script avec --start-date
      pour simuler un backfill complet.
    - On garde l'interface actuelle pour rester compatible avec le repo.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--symbols", nargs="*")
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--as-of")
    parser.add_argument("--lookback-days", type=int, default=5)
    parser.add_argument("--catchup-max-days", type=int, default=30)
    return parser.parse_args()


def parse_date(value: str | None) -> date | None:
    """
    Convertit une date ISO YYYY-MM-DD en objet date.
    """
    return date.fromisoformat(value) if value else None


def resolve_runtime_log_dir() -> Path:
    """
    Résout le dossier de logs runtime de façon robuste.

    Priorité:
    1) variable d'environnement STOCK_QUANT_LOG_DIR si fournie
    2) fallback runtime hors repo
    """
    raw_value = os.environ.get("STOCK_QUANT_LOG_DIR", "~/stock-quant-oop-runtime/logs")
    log_dir = Path(raw_value).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def write_jsonl_log(path: str | Path, payload: dict) -> None:
    """
    Écrit une ligne JSONL de manière robuste.

    Pourquoi cette fonction existe:
    - open("~/...") n'expanse pas "~" tout seul
    - on veut créer automatiquement le dossier parent
    - on veut garder une écriture append simple et lisible
    """
    resolved_path = Path(path).expanduser().resolve()
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    with open(resolved_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main() -> None:
    args = parse_args()

    # Répertoire de logs runtime hors repo.
    # On le résout une seule fois puis on réutilise des Path explicites.
    log_dir = resolve_runtime_log_dir()
    build_prices_log_path = log_dir / "build_prices.log"
    provider_symbol_mapping_log_path = log_dir / "provider_symbol_mapping.log"
    provider_fetch_failures_log_path = log_dir / "provider_fetch_failures.log"

    # Repositories principaux.
    repo = DuckDbPriceRepository(args.db_path)
    failure_repo = DuckDbPriceProviderFailureRepository(args.db_path)
    failure_repo.ensure_table()

    # Scope de symboles à considérer.
    canonical_symbols, symbol_scope_source = repo.get_refresh_symbols(args.symbols)

    run_started_at = datetime.utcnow()
    base_log = {
        "timestamp": run_started_at.isoformat(),
        "symbol_scope_source": symbol_scope_source,
        "research_scope_symbol_count": len(canonical_symbols),
        "log_dir": str(log_dir),
        "db_path": str(Path(args.db_path).expanduser().resolve()),
    }

    # ------------------------------------------------------------------
    # Résolution de la fenêtre de refresh.
    #
    # Important:
    # - on mesure d'abord l'état global du dataset
    # - ensuite on réduit éventuellement le fetch aux symboles en retard
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

    base_log.update(
        {
            "effective_start_date": str(window.effective_start_date),
            "effective_end_date": str(window.effective_end_date),
            "gap_days": window.gap_days,
            "window_reason": window.window_reason,
            "is_noop": window.is_noop,
            "requires_range_fetch": window.requires_range_fetch,
            "last_any_date": str(window.last_any_date) if window.last_any_date else None,
            "last_complete_date": str(window.last_complete_date) if window.last_complete_date else None,
            "expected_count": window.expected_count,
            "observed_latest_count": window.observed_latest_count,
        }
    )

    # Cas noop: rien à faire, mais on journalise proprement.
    if window.is_noop or window.effective_end_date is None:
        write_jsonl_log(build_prices_log_path, base_log)
        print(json.dumps(base_log, indent=2))
        return

    # ------------------------------------------------------------------
    # Réduction incrémentale au niveau symbole.
    #
    # Cas explicite:
    # - si l'utilisateur force symbols / start-date / end-date / as-of,
    #   on respecte strictement ce scope
    #
    # Cas automatique:
    # - on ne fetch que les symboles réellement en retard
    # ------------------------------------------------------------------
    explicit_request = bool(
        args.symbols or args.start_date or args.end_date or args.as_of
    )

    if explicit_request:
        symbols_to_refresh = canonical_symbols
        symbol_refresh_reason = "explicit_request_scope"
    else:
        symbols_to_refresh = repo.get_symbols_needing_refresh_through(
            target_date=window.effective_end_date,
            symbols=canonical_symbols,
        )
        symbol_refresh_reason = "lagging_symbols_only"

    base_log["refresh_scope_symbol_count"] = len(symbols_to_refresh)
    base_log["symbol_refresh_reason"] = symbol_refresh_reason

    # Cas où plus aucun symbole ne nécessite de refresh.
    if not symbols_to_refresh:
        base_log["is_noop"] = True
        base_log["window_reason"] = f"{window.window_reason}|no_symbols_need_refresh"
        write_jsonl_log(build_prices_log_path, base_log)
        print(json.dumps(base_log, indent=2))
        return

    # Plan de mapping fournisseur.
    provider_service = PriceProviderSymbolService()
    plan = provider_service.build_yfinance_plan(symbols_to_refresh)

    # Exclure les symboles avec failures récentes déjà connues.
    failed_provider_symbols = set(failure_repo.get_recent_failures("yfinance"))
    filtered_provider_symbols = [
        symbol
        for symbol in plan.eligible_provider_symbols
        if symbol not in failed_provider_symbols
    ]
    filtered_out_failures = len(plan.eligible_provider_symbols) - len(filtered_provider_symbols)

    log = dict(base_log)
    log.update(
        {
            "provider_candidate_symbol_count": plan.total_input_count,
            "provider_eligible_symbol_count": plan.eligible_count,
            "provider_excluded_symbol_count": plan.excluded_count,
            "mapping_applied_count": plan.mapping_applied_count,
            "exclusion_breakdown": plan.exclusion_breakdown,
            "filtered_out_failures": filtered_out_failures,
        }
    )

    # Journaliser les exclusions de mapping.
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
        write_jsonl_log(provider_symbol_mapping_log_path, exclusion_payload)

    # Cas noop après filtrage fournisseur.
    if not filtered_provider_symbols:
        log["is_noop"] = True
        log["window_reason"] = f"{window.window_reason}|no_provider_symbols_after_filter"
        write_jsonl_log(build_prices_log_path, log)
        print(json.dumps(log, indent=2))
        return

    # Fetch provider.
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
    # Capture des failures Yahoo remontées par le provider.
    #
    # On ne persiste comme failures permanentes que celles qui le sont
    # réellement selon la policy du repository.
    # ------------------------------------------------------------------
    provider_failures = provider.get_last_failures()
    failures_for_repository = []
    permanent_failure_count = 0
    transient_failure_count = 0

    for failure in provider_failures:
        canonical_symbol = plan.provider_to_canonical.get(failure.provider_symbol)

        if failure.is_transient:
            transient_failure_count += 1
        else:
            if is_permanent_failure(failure.failure_reason):
                permanent_failure_count += 1
                failures_for_repository.append(
                    {
                        "canonical_symbol": canonical_symbol,
                        "provider_symbol": failure.provider_symbol,
                        "failure_reason": failure.failure_reason,
                    }
                )

    failure_repo.upsert_failures("yfinance", failures_for_repository)

    log["provider_failure_count"] = len(provider_failures)
    log["provider_permanent_failure_count"] = permanent_failure_count
    log["provider_transient_failure_count"] = transient_failure_count

    if provider_failures:
        write_jsonl_log(
            provider_fetch_failures_log_path,
            {
                "timestamp": run_started_at.isoformat(),
                "provider_name": "yfinance",
                "failure_count": len(provider_failures),
                "permanent_failure_count": permanent_failure_count,
                "transient_failure_count": transient_failure_count,
                "sample_failures": [
                    {
                        "provider_symbol": failure.provider_symbol,
                        "failure_reason": failure.failure_reason,
                        "is_transient": failure.is_transient,
                    }
                    for failure in provider_failures[:200]
                ],
            },
        )

    # Écriture en base.
    write_result = repo.upsert_price_history(
        df,
        source_name="yfinance",
        ingested_at=run_started_at,
    )
    log["write_result"] = write_result

    # Refresh table serving latest.
    latest_symbols = (
        list(df["symbol"].dropna().astype(str).unique())
        if df is not None and not df.empty
        else []
    )
    latest_result = repo.refresh_price_latest(symbols=latest_symbols)
    log["price_latest_refresh_result"] = latest_result

    # Log final.
    write_jsonl_log(build_prices_log_path, log)

    # Petite tqdm minimaliste pour garder un comportement terminal stable.
    for _ in tqdm(range(1), desc="build_prices", leave=False):
        pass

    print(json.dumps(log, indent=2))


if __name__ == "__main__":
    main()
