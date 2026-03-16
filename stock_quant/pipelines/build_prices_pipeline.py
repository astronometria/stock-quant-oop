from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stock_quant.app.services.price_ingestion_service import (
    PriceIngestionResult,
    PriceIngestionService,
)
from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class BuildPricesPipelineResult:
    """
    Résultat canonique du pipeline prix.

    Contrat métier
    --------------
    - `price_history` = normalized canonique
    - `price_latest` = serving only
    - le pipeline ne doit jamais promouvoir `price_latest` comme source de recherche
    """
    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int
    start_date: str | None = None
    end_date: str | None = None


class BuildPricesPipeline:
    """
    Pipeline canonique du domaine prix.

    Responsabilités
    ----------------
    - déléguer l'ingestion incrémentale à PriceIngestionService
    - exposer un contrat stable au CLI / orchestrateur
    - garder un design homogène avec le reste du codebase

    Notes anti-biais
    ----------------
    - `price_history` est la seule table normalized canonique
    - `price_latest` ne doit jamais servir aux backtests / features
    """

    pipeline_name = "build_prices"

    def __init__(self, price_ingestion_service: PriceIngestionService) -> None:
        self.price_ingestion_service = price_ingestion_service

    def run(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> BuildPricesPipelineResult:
        try:
            result = self.price_ingestion_service.ingest_incremental(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
            return self._to_pipeline_result(result)
        except Exception as exc:
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError(f"failed to run build_prices pipeline: {exc}") from exc

    def _to_pipeline_result(
        self,
        result: PriceIngestionResult,
    ) -> BuildPricesPipelineResult:
        return BuildPricesPipelineResult(
            requested_symbols=result.requested_symbols,
            fetched_symbols=result.fetched_symbols,
            written_price_history_rows=result.written_price_history_rows,
            price_latest_rows_after_refresh=result.price_latest_rows_after_refresh,
            start_date=result.start_date,
            end_date=result.end_date,
        )


# ----------------------------------------------------------------------
# Aliases de compatibilité
# ----------------------------------------------------------------------
PricesPipelineResult = BuildPricesPipelineResult
PricesPipeline = BuildPricesPipeline

__all__ = [
    "BuildPricesPipeline",
    "BuildPricesPipelineResult",
    "PricesPipeline",
    "PricesPipelineResult",
]
