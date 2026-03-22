"""
Indicateur bollinger_zscore_20.
"""

from __future__ import annotations

from stock_quant.features.price_volatility.base import volatility_spec

SPEC = volatility_spec(
    name="bollinger_zscore_20",
    output_columns=["bollinger_zscore_20"],
    sql_select_expressions=[
        """
        CASE
            WHEN stddev_20 = 0 THEN NULL
            ELSE (close - sma_20) / stddev_20
        END AS bollinger_zscore_20
        """.strip()
    ],
    required_input_columns=["close", "sma_20", "stddev_20"],
)
