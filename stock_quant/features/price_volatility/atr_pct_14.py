"""
Indicateur atr_pct_14.
"""

from __future__ import annotations

from stock_quant.features.price_volatility.base import volatility_spec

SPEC = volatility_spec(
    name="atr_pct_14",
    output_columns=["atr_pct_14"],
    sql_select_expressions=[
        """
        CASE
            WHEN close = 0 THEN NULL
            ELSE atr_14 / close
        END AS atr_pct_14
        """.strip()
    ],
    required_input_columns=["atr_14", "close"],
)
