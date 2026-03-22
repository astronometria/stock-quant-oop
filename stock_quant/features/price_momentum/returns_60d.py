"""
Momentum indicator: returns_60d.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="returns_60d",
    group_name="price_momentum",
    required_columns=["close"],
    output_columns=["returns_60d"],
    sql_select_expressions=[
        """
        CASE
            WHEN LAG(close, 60) OVER price_w IS NULL
              OR LAG(close, 60) OVER price_w = 0
            THEN NULL
            ELSE (close / LAG(close, 60) OVER price_w) - 1
        END AS returns_60d
        """.strip()
    ],
)
