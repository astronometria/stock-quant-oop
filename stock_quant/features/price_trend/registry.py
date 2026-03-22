"""
Trend indicators registry.
"""

from __future__ import annotations

from stock_quant.features.contracts import validate_indicator_specs
from stock_quant.features.price_trend.close_to_sma_20 import SPEC as CLOSE_TO_SMA_20_SPEC
from stock_quant.features.price_trend.close_to_sma_50 import SPEC as CLOSE_TO_SMA_50_SPEC
from stock_quant.features.price_trend.close_to_sma_200 import SPEC as CLOSE_TO_SMA_200_SPEC
from stock_quant.features.price_trend.ema_12 import SPEC as EMA_12_SPEC
from stock_quant.features.price_trend.ema_26 import SPEC as EMA_26_SPEC
from stock_quant.features.price_trend.macd_hist import SPEC as MACD_HIST_SPEC
from stock_quant.features.price_trend.macd_line import SPEC as MACD_LINE_SPEC
from stock_quant.features.price_trend.macd_signal import SPEC as MACD_SIGNAL_SPEC
from stock_quant.features.price_trend.sma_20 import SPEC as SMA_20_SPEC
from stock_quant.features.price_trend.sma_50 import SPEC as SMA_50_SPEC
from stock_quant.features.price_trend.sma_200 import SPEC as SMA_200_SPEC

ALL_INDICATORS = validate_indicator_specs(
    [
        SMA_20_SPEC,
        SMA_50_SPEC,
        SMA_200_SPEC,
        CLOSE_TO_SMA_20_SPEC,
        CLOSE_TO_SMA_50_SPEC,
        CLOSE_TO_SMA_200_SPEC,
        EMA_12_SPEC,
        EMA_26_SPEC,
        MACD_LINE_SPEC,
        MACD_SIGNAL_SPEC,
        MACD_HIST_SPEC,
    ]
)
