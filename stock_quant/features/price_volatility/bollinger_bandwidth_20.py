"""
Indicateur bollinger_bandwidth_20.
"""

from __future__ import annotations

from stock_quant.features.price_volatility.base import volatility_spec

SPEC = volatility_spec(
    name="bollinger_bandwidth_20",
    output_columns=["bollinger_bandwidth_20"],
    sql_select_expressions=[
        """
        CASE
            WHEN sma_20 = 0 THEN NULL
            ELSE ((sma_20 + 2 * stddev_20) - (sma_20 - 2 * stddev_20)) / sma_20
        END AS bollinger_bandwidth_20
        """.strip()
    ],
    required_input_columns=["sma_20", "stddev_20"],
)
