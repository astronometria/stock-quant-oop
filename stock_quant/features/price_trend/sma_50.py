"""
Trend indicator: sma_50.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="sma_50",
    group_name="price_trend",
    required_columns=["close"],
    output_columns=["sma_50"],
    sql_select_expressions=[
        """
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50
        """.strip()
    ],
)
