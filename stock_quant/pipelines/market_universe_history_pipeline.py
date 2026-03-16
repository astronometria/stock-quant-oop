"""
market_universe_history_pipeline.py

Pipeline standardisé pour construire market_universe_history.

Philosophie
-----------

Ce pipeline reste volontairement mince :
- pas de logique métier
- pas de SQL
- pas de gestion manuelle du timing

Tout cela est délégué respectivement :
- au service applicatif
- aux repositories
- au runner de pipeline partagé
"""

from __future__ import annotations

from stock_quant.app.services.market_universe_history_service import (
    MarketUniverseHistoryService,
)
from stock_quant.shared.pipeline_runner import run_pipeline


class BuildMarketUniverseHistoryPipeline:
    """
    Pipeline standard pour construire market_universe_history.
    """

    def __init__(
        self,
        service: MarketUniverseHistoryService,
    ) -> None:
        self.service = service

    def run(self):
        """
        Exécute le pipeline via le runner standard partagé.
        """
        return run_pipeline(
            "build_market_universe_history",
            lambda: self._execute(),
        )

    def _execute(self) -> dict:
        """
        Exécute réellement le service métier puis adapte la sortie
        au format standard attendu par le runner.
        """
        result = self.service.rebuild_from_active_listings()

        return {
            "rows_read": result.listings_read,
            "rows_written": result.versions_inserted,
            "rows_skipped": 0,
            "metrics": {
                "listings_read": result.listings_read,
                "versions_inserted": result.versions_inserted,
                "versions_closed": result.versions_closed,
                "versions_unchanged": result.versions_unchanged,
            },
        }
