"""
Indicateur close_to_sma_200.
"""

from __future__ import annotations

from stock_quant.features.price_trend.base import trend_spec

SPEC = trend_spec(
    name="close_to_sma_200",
    output_columns=["close_to_sma_200"],
    sql_select_expressions=[
        """
        CASE
            WHEN sma_200 = 0 THEN NULL
            ELSE (close / sma_200) - 1
        END AS close_to_sma_200
        """.strip()
    ],
    required_input_columns=["close", "sma_200"],
)
