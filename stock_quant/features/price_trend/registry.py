"""
Registre du groupe trend.
"""

from __future__ import annotations

from stock_quant.features.contracts import validate_specs
from stock_quant.features.price_trend.close_to_sma_200 import SPEC as CLOSE_TO_SMA_200_SPEC
from stock_quant.features.price_trend.close_to_sma_50 import SPEC as CLOSE_TO_SMA_50_SPEC
from stock_quant.features.price_trend.macd_hist import SPEC as MACD_HIST_SPEC

ALL_SPECS = validate_specs(
    [
        CLOSE_TO_SMA_50_SPEC,
        CLOSE_TO_SMA_200_SPEC,
        MACD_HIST_SPEC,
    ]
)
