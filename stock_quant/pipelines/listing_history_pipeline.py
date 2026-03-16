"""
listing_history_pipeline.py

Pipeline standardisé pour construire listing_status_history.

Philosophie
-----------

Ce pipeline ne contient aucune logique métier.

Il délègue entièrement :
- la logique métier au service applicatif
- la standardisation du résultat au pipeline runner partagé

Ce pattern doit devenir le modèle pour tous les autres pipelines du projet.
"""

from __future__ import annotations

from stock_quant.app.services.listing_history_service import ListingHistoryService
from stock_quant.shared.pipeline_runner import run_pipeline


class BuildListingHistoryPipeline:
    """
    Pipeline standard pour construire listing_status_history.
    """

    def __init__(
        self,
        service: ListingHistoryService,
    ) -> None:
        self.service = service

    def run(self):
        """
        Exécute le pipeline via le runner standard partagé.
        """
        return run_pipeline(
            "build_listing_history",
            lambda: self._execute(),
        )

    def _execute(self) -> dict:
        """
        Exécute réellement le service applicatif puis convertit
        son résultat métier vers le format attendu par run_pipeline().
        """
        result = self.service.build_all_available_dates()

        return {
            "rows_read": result.observations_read,
            "rows_written": result.versions_inserted,
            "rows_skipped": result.skipped_observations,
            "metrics": {
                "as_of_date_count": result.as_of_date_count,
                "observations_read": result.observations_read,
                "versions_inserted": result.versions_inserted,
                "versions_closed": result.versions_closed,
                "versions_touched": result.versions_touched,
                "skipped_observations": result.skipped_observations,
            },
        }
