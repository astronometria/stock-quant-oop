"""
Registre du groupe momentum.

L'orchestrateur cli/core peut importer ALL_SPECS et injecter les
expressions SQL dynamiquement dans sa requête.
"""

from __future__ import annotations

from stock_quant.features.contracts import validate_specs
from stock_quant.features.price_momentum.distance_from_252d_high import SPEC as DISTANCE_FROM_252D_HIGH_SPEC
from stock_quant.features.price_momentum.returns_10d import SPEC as RETURNS_10D_SPEC
from stock_quant.features.price_momentum.returns_60d import SPEC as RETURNS_60D_SPEC
from stock_quant.features.price_momentum.williams_r_14 import SPEC as WILLIAMS_R_14_SPEC

ALL_SPECS = validate_specs(
    [
        RETURNS_10D_SPEC,
        RETURNS_60D_SPEC,
        WILLIAMS_R_14_SPEC,
        DISTANCE_FROM_252D_HIGH_SPEC,
    ]
)
