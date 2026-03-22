"""
Trend indicator: macd_line.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="macd_line",
    group_name="price_trend",
    required_columns=["ema_12", "ema_26"],
    output_columns=["macd_line"],
    sql_select_expressions=[
        """
        CASE
            WHEN ema_12 IS NULL OR ema_26 IS NULL THEN NULL
            ELSE ema_12 - ema_26
        END AS macd_line
        """.strip()
    ],
)
