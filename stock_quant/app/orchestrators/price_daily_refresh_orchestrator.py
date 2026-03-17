from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stock_quant.pipelines.prices_pipeline import PricesPipeline, PricesPipelineResult
from stock_quant.shared.exceptions import ServiceError


@dataclass(slots=True)
class PriceDailyRefreshResult:
    """
    Résultat applicatif stable pour le refresh quotidien des prix.

    Notes importantes :
    - `price_history` reste la table canonique normalized.
    - `price_latest` reste une table de serving uniquement.
    - `start_date` / `end_date` doivent refléter la fenêtre EFFECTIVE réellement
      utilisée par le pipeline, pas seulement les arguments bruts du CLI.
    """
    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int
    start_date: str | None
    end_date: str | None


class PriceDailyRefreshOrchestrator:
    """
    Point d'entrée applicatif pour le refresh incrémental quotidien des prix.

    Responsabilités :
    - exposer une interface simple pour le CLI / les jobs ops
    - déléguer le vrai travail de données au pipeline
    - remonter un résultat stable et cohérent avec la fenêtre réellement utilisée
    """

    def __init__(self, prices_pipeline: PricesPipeline) -> None:
        self.prices_pipeline = prices_pipeline

    def run_daily_refresh(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PriceDailyRefreshResult:
        """
        Lance un refresh quotidien des prix.

        Important :
        - les arguments `start_date` / `end_date` sont des demandes utilisateur/CLI
        - le pipeline peut calculer une fenêtre effective différente
        - on doit donc restituer les dates provenant du résultat pipeline
        """
        try:
            result = self.prices_pipeline.run(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
            return self._to_orchestrator_result(result=result)
        except Exception as exc:
            if isinstance(exc, ServiceError):
                raise
            raise ServiceError(f"failed to run daily price refresh: {exc}") from exc

    def _to_orchestrator_result(
        self,
        result: PricesPipelineResult,
    ) -> PriceDailyRefreshResult:
        """
        Convertit le résultat pipeline en résultat applicatif.

        Point clé :
        - on remonte `result.start_date` et `result.end_date`
        - cela permet au JSON final de refléter la fenêtre effective calculée
        """
        return PriceDailyRefreshResult(
            requested_symbols=result.requested_symbols,
            fetched_symbols=result.fetched_symbols,
            written_price_history_rows=result.written_price_history_rows,
            price_latest_rows_after_refresh=result.price_latest_rows_after_refresh,
            start_date=result.start_date,
            end_date=result.end_date,
        )
