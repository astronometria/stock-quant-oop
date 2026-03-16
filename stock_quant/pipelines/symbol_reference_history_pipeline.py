"""
symbol_reference_history_pipeline.py

Pipeline minimal utilisant le framework pipeline_runner.

Philosophie
-----------

Le pipeline ne contient AUCUNE logique métier.

Il délègue tout au service applicatif et se contente de :

- fournir le nom du pipeline
- exécuter le service
- retourner un PipelineResult standard

Cela garantit une architecture cohérente sur tout le projet.
"""

from __future__ import annotations

from stock_quant.app.services.symbol_reference_history_service import (
    SymbolReferenceHistoryService,
)
from stock_quant.shared.pipeline_runner import run_pipeline


class BuildSymbolReferenceHistoryPipeline:
    """
    Pipeline standard pour construire symbol_reference_history.
    """

    def __init__(
        self,
        service: SymbolReferenceHistoryService,
    ) -> None:
        self.service = service

    def run(self):
        """
        Exécute le pipeline en utilisant le runner standard.
        """

        return run_pipeline(
            "build_symbol_reference_history",
            lambda: self._execute(),
        )

    def _execute(self) -> dict:
        """
        Exécution réelle du service.

        Le service retourne un objet métier que l'on convertit
        en dictionnaire standard pour le runner.
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
