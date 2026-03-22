"""
Trend indicator: sma_20.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="sma_20",
    group_name="price_trend",
    required_columns=["close"],
    output_columns=["sma_20"],
    sql_select_expressions=[
        """
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20
        """.strip()
    ],
)
