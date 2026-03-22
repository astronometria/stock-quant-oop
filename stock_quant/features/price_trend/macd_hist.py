"""
Trend indicator: macd_hist.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="macd_hist",
    group_name="price_trend",
    required_columns=["macd_line", "macd_signal"],
    output_columns=["macd_hist"],
    sql_select_expressions=[
        """
        CASE
            WHEN macd_line IS NULL OR macd_signal IS NULL THEN NULL
            ELSE macd_line - macd_signal
        END AS macd_hist
        """.strip()
    ],
)
