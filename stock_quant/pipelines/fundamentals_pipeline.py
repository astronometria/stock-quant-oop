"""
Compatibility wrapper for the fundamentals pipeline.

Historique
----------
Le pipeline canonique a été renommé pour suivre la convention :

    build_*_pipeline.py

Ancien fichier :
    fundamentals_pipeline.py

Nouveau fichier :
    build_fundamentals_pipeline.py

Ce wrapper est maintenu temporairement pour éviter de casser
les imports existants pendant la phase de migration.

Tous les nouveaux imports doivent utiliser :

    stock_quant.pipelines.build_fundamentals_pipeline
"""

from __future__ import annotations

# Re-export du pipeline canonique
from stock_quant.pipelines.build_fundamentals_pipeline import BuildFundamentalsPipeline

__all__ = ["BuildFundamentalsPipeline"]
