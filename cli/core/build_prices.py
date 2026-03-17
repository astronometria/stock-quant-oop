#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd

from stock_quant.app.orchestrators.price_daily_refresh_orchestrator import (
    PriceDailyRefreshOrchestrator,
)
from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.prices.historical_price_provider import (
    HistoricalPriceProvider,
)
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import (
    YfinancePriceProvider,
)
from stock_quant.infrastructure.repositories.duckdb_price_repository import (
    DuckDbPriceRepository,
)
from stock_quant.pipelines.build_prices_pipeline import BuildPricesPipeline
from stock_quant.shared.exceptions import PipelineError, ServiceError


@dataclass(slots=True)
class LegacyBuildPricesResult:
    """
    Résultat du chemin SQL-first historique.

    Ce chemin est conservé pour compatibilité opérationnelle lorsque la staging raw
    existe déjà dans DuckDB.

    Règles importantes :
    - la sortie normalized canonique reste `price_history`
    - `price_latest` est une table de serving only
    """

    raw_bars: int
    allowed_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int
    skipped_not_in_universe: int
    skipped_invalid: int


@dataclass(slots=True)
class EffectiveDateWindow:
    """
    Fenêtre de dates réellement utilisée par le pipeline.

    Cette structure sert à expliciter :
    - les dates demandées par l'utilisateur
    - les dates retenues après sondage de la base
    - si l'exécution doit être sautée parce qu'il n'y a rien à rattraper
    """

    requested_start_date: str | None
    requested_end_date: str | None
    effective_start_date: str | None
    effective_end_date: str | None
    last_available_price_date: str | None
    skip_fetch: bool
    skip_reason: str | None
    lookback_days: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build price_history and price_latest from staged raw prices or incremental providers. "
            "Normalized canonical table is price_history; price_latest is serving only."
        )
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--mode",
        default="daily",
        choices=["daily", "backfill"],
        help="Price build mode.",
    )
    parser.add_argument(
        "--historical-source",
        action="append",
        default=[],
        help="Historical source path(s). Repeat for multiple CSV/ZIP/directories when mode=backfill.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter. Repeat for multiple symbols.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD).")
    parser.add_argument(
        "--as-of",
        default=None,
        help="Optional single target date (YYYY-MM-DD) for daily mode.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=3,
        help=(
            "Daily incremental overlap window in calendar days when no explicit start-date is provided. "
            "Used to replay a small recent window safely and fill minor gaps."
        ),
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _parse_iso_date(value: str | None) -> date | None:
    if value is None or not str(value).strip():
        return None
    return date.fromisoformat(str(value).strip())


def _iso_or_none(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _normalize_symbols(values: Iterable[str] | None) -> list[str]:
    return sorted(
        {
            str(v).strip().upper()
            for v in (values or [])
            if v is not None and str(v).strip()
        }
    )


class ProviderFrameAdapter:
    """
    Adapte les providers orientés DataFrame au contrat attendu
    par PriceIngestionService.

    Modes
    -----
    - daily    -> provider daily (Yahoo)
    - backfill -> provider historique (Stooq/local files)
    """

    def __init__(self, provider, mode: str) -> None:
        self._provider = provider
        self._mode = mode

    def fetch_prices(
        self,
        symbols: list[str],
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame:
        if self._mode == "daily":
            frame_method = getattr(self._provider, "fetch_daily_prices_frame", None)
            if callable(frame_method):
                # ------------------------------------------------------------------
                # IMPORTANT
                # ------------------------------------------------------------------
                # Le provider Yahoo du projet sait maintenant gérer une plage
                # [start_date, end_date] pour rattraper les trous.
                # On lui transmet donc les deux bornes, même en mode "daily".
                # ------------------------------------------------------------------
                return frame_method(
                    symbols=symbols,
                    start_date=_parse_iso_date(start_date),
                    end_date=_parse_iso_date(end_date),
                )
            raise ServiceError("daily provider does not support fetch_daily_prices_frame")

        start = _parse_iso_date(start_date)
        end = _parse_iso_date(end_date)
        frame_method = getattr(self._provider, "fetch_history_frame", None)
        if callable(frame_method):
            return frame_method(symbols=symbols, start_date=start, end_date=end)
        raise ServiceError("historical provider does not support fetch_history_frame")


def _count_staged_raw_rows(repository: DuckDbPriceRepository) -> int:
    try:
        return len(repository.load_raw_price_bars())
    except Exception:
        return 0


def _safe_get_last_price_date(repository: DuckDbPriceRepository) -> date | None:
    """
    Sonde la dernière date disponible dans la table canonique `price_history`.

    On ne lit jamais `price_latest` ici, car la source de vérité normalized
    pour la recherche et les refreshs incrémentaux est `price_history`.
    """
    try:
        row = repository.con.execute(
            """
            SELECT MAX(price_date) AS last_price_date
            FROM price_history
            """
        ).fetchone()
        if not row or row[0] is None:
            return None
        return row[0]
    except Exception as exc:
        raise PipelineError(f"failed to probe last available price_date from price_history: {exc}") from exc


def _build_effective_daily_window(
    repository: DuckDbPriceRepository,
    *,
    requested_start_date: str | None,
    requested_end_date: str | None,
    lookback_days: int,
) -> EffectiveDateWindow:
    """
    Calcule la fenêtre effective du refresh daily.

    Règles :
    - si start-date est fourni, on le respecte
    - sinon on sonde MAX(price_date) dans price_history
    - si une dernière date existe, on rejoue une petite fenêtre de recouvrement
      pour rattraper les trous récents et rester idempotent
    - si aucune borne de fin n'est fournie, on prend aujourd'hui
    - si la fenêtre est vide, on saute proprement
    """
    last_available_price_date = _safe_get_last_price_date(repository)

    effective_end = _parse_iso_date(requested_end_date)
    if effective_end is None:
        effective_end = date.today()

    explicit_start = _parse_iso_date(requested_start_date)
    if explicit_start is not None:
        effective_start = explicit_start
    else:
        if last_available_price_date is None:
            effective_start = effective_end
        else:
            replay_anchor = last_available_price_date - timedelta(days=max(0, lookback_days))
            effective_start = min(replay_anchor, effective_end)

    if effective_start > effective_end:
        return EffectiveDateWindow(
            requested_start_date=requested_start_date,
            requested_end_date=requested_end_date,
            effective_start_date=_iso_or_none(effective_start),
            effective_end_date=_iso_or_none(effective_end),
            last_available_price_date=_iso_or_none(last_available_price_date),
            skip_fetch=True,
            skip_reason="effective_start_date_after_effective_end_date",
            lookback_days=lookback_days,
        )

    return EffectiveDateWindow(
        requested_start_date=requested_start_date,
        requested_end_date=requested_end_date,
        effective_start_date=_iso_or_none(effective_start),
        effective_end_date=_iso_or_none(effective_end),
        last_available_price_date=_iso_or_none(last_available_price_date),
        skip_fetch=False,
        skip_reason=None,
        lookback_days=lookback_days,
    )


def _run_legacy_sql_first_build(
    repository: DuckDbPriceRepository,
    *,
    requested_symbols: list[str],
) -> LegacyBuildPricesResult:
    """
    Chemin SQL-first historique.

    Usage :
    - bootstrap depuis une staging raw locale déjà peuplée
    - reconstruction idempotente depuis raw existant

    Règle anti-biais :
    - la sortie normalized reste `price_history`
    - `price_latest` ne doit pas être lu par les pipelines de recherche
    """
    raw_bars = repository.load_raw_price_bars()
    if not raw_bars:
        raise PipelineError("no raw price bars available in price_source_daily_raw")

    allowed_symbols = repository.load_included_symbols()
    if requested_symbols:
        requested_set = set(requested_symbols)
        allowed_symbols = {symbol for symbol in allowed_symbols if symbol in requested_set}

    service = PriceIngestionService()
    normalized_entries, metrics = service.build(
        raw_bars,
        allowed_symbols=allowed_symbols,
    )

    written_rows = repository.replace_price_history(normalized_entries)
    latest_rows = repository.rebuild_price_latest()

    return LegacyBuildPricesResult(
        raw_bars=int(metrics.get("raw_bars", 0)),
        allowed_symbols=len(allowed_symbols),
        written_price_history_rows=written_rows,
        price_latest_rows_after_refresh=latest_rows,
        skipped_not_in_universe=int(metrics.get("skipped_not_in_universe", 0)),
        skipped_invalid=int(metrics.get("skipped_invalid", 0)),
    )


def _run_incremental_build(
    repository: DuckDbPriceRepository,
    *,
    mode: str,
    historical_source: list[str],
    requested_symbols: list[str],
    start_date: str | None,
    end_date: str | None,
):
    """
    Chemin incrémental moderne :
    - backfill via HistoricalPriceProvider
    - daily via YfinancePriceProvider

    Contrat :
    - Stooq/local history = rebuild / bootstrap
    - Yahoo daily = refresh / gap fill incrémental
    """
    if mode == "backfill":
        if not historical_source:
            raise PipelineError("--historical-source is required when --mode=backfill")
        provider = HistoricalPriceProvider(source_paths=historical_source)
        adapter = ProviderFrameAdapter(provider=provider, mode="backfill")
    else:
        provider = YfinancePriceProvider()
        adapter = ProviderFrameAdapter(provider=provider, mode="daily")

    service = PriceIngestionService(
        price_repository=repository,
        historical_price_provider=adapter,
    )
    pipeline = BuildPricesPipeline(price_ingestion_service=service)
    orchestrator = PriceDailyRefreshOrchestrator(prices_pipeline=pipeline)

    return orchestrator.run_daily_refresh(
        symbols=requested_symbols or None,
        start_date=start_date,
        end_date=end_date,
    )


def main() -> int:
    args = parse_args()

    if args.lookback_days < 0:
        raise SystemExit("--lookback-days must be >= 0")

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_prices] project_root={config.project_root}")
        print(f"[build_prices] db_path={config.db_path}")
        print(f"[build_prices] mode={args.mode}")
        print(f"[build_prices] historical_source_count={len(args.historical_source)}")
        print(f"[build_prices] symbols_count={len(args.symbols)}")
        print(f"[build_prices] lookback_days={args.lookback_days}")
        print("[build_prices] normalized_table=price_history")
        print("[build_prices] derived_table=price_latest (serving only)")

    start_date = args.start_date
    end_date = args.end_date

    if args.mode == "daily" and args.as_of:
        start_date = args.as_of
        end_date = args.as_of

    requested_symbols = _normalize_symbols(args.symbols)

    session_factory = DuckDbSessionFactory(config.db_path)

    # ------------------------------------------------------------------
    # Pass 1: validation / évolution du schéma de base
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    # ------------------------------------------------------------------
    # Pass 2: build réel
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        if uow.connection is None:
            raise PipelineError("missing active DB connection")

        repository = DuckDbPriceRepository(uow.connection)

        staged_raw_rows = _count_staged_raw_rows(repository)
        use_sql_first_staging = args.mode == "daily" and staged_raw_rows > 0

        if args.verbose:
            print(f"[build_prices] staged_raw_rows={staged_raw_rows}")
            print(
                f"[build_prices] path={'sql_first_staging' if use_sql_first_staging else 'incremental_provider'}"
            )

        if use_sql_first_staging:
            result = _run_legacy_sql_first_build(
                repository,
                requested_symbols=requested_symbols,
            )
            payload = {
                "status": "SUCCESS",
                "mode": args.mode,
                "path": "sql_first_staging",
                "normalized_table": "price_history",
                "derived_table": "price_latest",
                **asdict(result),
            }
        else:
            effective_window: EffectiveDateWindow | None = None

            effective_start_date = start_date
            effective_end_date = end_date

            if args.mode == "daily":
                effective_window = _build_effective_daily_window(
                    repository,
                    requested_start_date=start_date,
                    requested_end_date=end_date,
                    lookback_days=args.lookback_days,
                )
                effective_start_date = effective_window.effective_start_date
                effective_end_date = effective_window.effective_end_date

                if args.verbose:
                    print(
                        json.dumps(
                            {
                                "daily_window": {
                                    "requested_start_date": effective_window.requested_start_date,
                                    "requested_end_date": effective_window.requested_end_date,
                                    "effective_start_date": effective_window.effective_start_date,
                                    "effective_end_date": effective_window.effective_end_date,
                                    "last_available_price_date": effective_window.last_available_price_date,
                                    "skip_fetch": effective_window.skip_fetch,
                                    "skip_reason": effective_window.skip_reason,
                                    "lookback_days": effective_window.lookback_days,
                                }
                            },
                            indent=2,
                            sort_keys=True,
                        )
                    )

                if effective_window.skip_fetch:
                    payload = {
                        "status": "SUCCESS",
                        "mode": args.mode,
                        "path": "incremental_provider",
                        "normalized_table": "price_history",
                        "derived_table": "price_latest",
                        "requested_symbols": len(requested_symbols) if requested_symbols else 0,
                        "fetched_symbols": 0,
                        "written_price_history_rows": 0,
                        "price_latest_rows_after_refresh": repository.refresh_price_latest([]),
                        "start_date": effective_start_date,
                        "end_date": effective_end_date,
                        "last_available_price_date": effective_window.last_available_price_date,
                        "skip_fetch": True,
                        "skip_reason": effective_window.skip_reason,
                        "lookback_days": effective_window.lookback_days,
                    }
                    print(json.dumps(payload, indent=2, sort_keys=True))
                    return 0

            result = _run_incremental_build(
                repository,
                mode=args.mode,
                historical_source=args.historical_source,
                requested_symbols=requested_symbols,
                start_date=effective_start_date,
                end_date=effective_end_date,
            )
            payload = {
                "status": "SUCCESS",
                "mode": args.mode,
                "path": "incremental_provider",
                "normalized_table": "price_history",
                "derived_table": "price_latest",
                "requested_symbols": result.requested_symbols,
                "fetched_symbols": result.fetched_symbols,
                "written_price_history_rows": result.written_price_history_rows,
                "price_latest_rows_after_refresh": result.price_latest_rows_after_refresh,
                "start_date": result.start_date,
                "end_date": result.end_date,
            }
            if effective_window is not None:
                payload["last_available_price_date"] = effective_window.last_available_price_date
                payload["lookback_days"] = effective_window.lookback_days

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PipelineError, ServiceError) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
