"""
Indicateur returns_60d.
"""

from __future__ import annotations

from stock_quant.features.price_momentum.base import momentum_spec

SPEC = momentum_spec(
    name="returns_60d",
    output_columns=["returns_60d"],
    sql_select_expressions=[
        """
        CASE
            WHEN LAG(close, 60) OVER price_w IS NULL OR LAG(close, 60) OVER price_w = 0 THEN NULL
            ELSE (close / LAG(close, 60) OVER price_w) - 1
        END AS returns_60d
        """.strip()
    ],
    required_input_columns=["symbol", "as_of_date", "close"],
)
