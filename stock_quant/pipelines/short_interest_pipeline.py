"""
Compatibility wrapper for the FINRA / short-interest pipeline.

Historique
----------
Le pipeline canonique suit maintenant la convention :

    build_*_pipeline.py

Fichier canonique :
    build_short_interest_pipeline.py

Ce wrapper reste présent temporairement pour éviter de casser
les imports existants pendant la migration du domaine FINRA.
"""

from __future__ import annotations

from stock_quant.pipelines.build_short_interest_pipeline import (
    BuildFinraShortInterestPipeline,
    BuildShortDataPipeline,
    BuildShortInterestPipeline,
)

__all__ = [
    "BuildShortInterestPipeline",
    "BuildShortDataPipeline",
    "BuildFinraShortInterestPipeline",
]
