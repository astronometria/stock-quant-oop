"""
Trend indicator: sma_200.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="sma_200",
    group_name="price_trend",
    required_columns=["close"],
    output_columns=["sma_200"],
    sql_select_expressions=[
        """
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200
        """.strip()
    ],
)
