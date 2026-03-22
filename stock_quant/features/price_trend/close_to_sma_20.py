"""
Trend indicator: close_to_sma_20.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="close_to_sma_20",
    group_name="price_trend",
    required_columns=["close", "sma_20"],
    output_columns=["close_to_sma_20"],
    sql_select_expressions=[
        """
        CASE
            WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
            ELSE (close / sma_20) - 1
        END AS close_to_sma_20
        """.strip()
    ],
)
