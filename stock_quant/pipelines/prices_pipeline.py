"""
Compatibility wrapper for the prices pipeline.

Historique
----------
Le pipeline canonique a été renommé pour suivre la convention :

    build_*_pipeline.py

Ancien fichier :
    prices_pipeline.py

Nouveau fichier :
    build_prices_pipeline.py

Ce wrapper reste présent temporairement pour éviter de casser
les imports existants dans l'orchestrateur prix et dans les CLI.
"""

from __future__ import annotations

from stock_quant.pipelines.build_prices_pipeline import (
    BuildPricesPipeline,
    BuildPricesPipelineResult,
    PricesPipeline,
    PricesPipelineResult,
)

__all__ = [
    "BuildPricesPipeline",
    "BuildPricesPipelineResult",
    "PricesPipeline",
    "PricesPipelineResult",
]
