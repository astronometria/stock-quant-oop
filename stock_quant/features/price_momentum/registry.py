"""
Momentum indicators registry.

Le builder de momentum doit importer ALL_INDICATORS
et assembler les expressions SQL dynamiquement.

Règle:
- un fichier par indicateur
- ce registre est la seule liste à maintenir pour activer/désactiver
  des indicateurs dans le builder de groupe
"""

from __future__ import annotations

from stock_quant.features.contracts import validate_indicator_specs
from stock_quant.features.price_momentum.distance_from_252d_high import SPEC as DISTANCE_FROM_252D_HIGH_SPEC
from stock_quant.features.price_momentum.rsi_14 import SPEC as RSI_14_SPEC
from stock_quant.features.price_momentum.returns_10d import SPEC as RETURNS_10D_SPEC
from stock_quant.features.price_momentum.returns_1d import SPEC as RETURNS_1D_SPEC
from stock_quant.features.price_momentum.returns_20d import SPEC as RETURNS_20D_SPEC
from stock_quant.features.price_momentum.returns_5d import SPEC as RETURNS_5D_SPEC
from stock_quant.features.price_momentum.returns_60d import SPEC as RETURNS_60D_SPEC
from stock_quant.features.price_momentum.williams_r_14 import SPEC as WILLIAMS_R_14_SPEC

ALL_INDICATORS = validate_indicator_specs(
    [
        RETURNS_1D_SPEC,
        RETURNS_5D_SPEC,
        RETURNS_10D_SPEC,
        RETURNS_20D_SPEC,
        RETURNS_60D_SPEC,
        RSI_14_SPEC,
        WILLIAMS_R_14_SPEC,
        DISTANCE_FROM_252D_HIGH_SPEC,
    ]
)
