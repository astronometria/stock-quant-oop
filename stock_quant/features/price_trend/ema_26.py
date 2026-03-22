"""
Trend indicator: ema_26.

Même note que pour ema_12:
version proxy stable en phase 1.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="ema_26",
    group_name="price_trend",
    required_columns=["close"],
    output_columns=["ema_26"],
    sql_select_expressions=[
        """
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS ema_26
        """.strip()
    ],
)
