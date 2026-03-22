"""
Registre du groupe volatility.
"""

from __future__ import annotations

from stock_quant.features.contracts import validate_specs
from stock_quant.features.price_volatility.atr_pct_14 import SPEC as ATR_PCT_14_SPEC
from stock_quant.features.price_volatility.bollinger_bandwidth_20 import SPEC as BOLLINGER_BANDWIDTH_20_SPEC
from stock_quant.features.price_volatility.bollinger_zscore_20 import SPEC as BOLLINGER_ZSCORE_20_SPEC

ALL_SPECS = validate_specs(
    [
        ATR_PCT_14_SPEC,
        BOLLINGER_ZSCORE_20_SPEC,
        BOLLINGER_BANDWIDTH_20_SPEC,
    ]
)
