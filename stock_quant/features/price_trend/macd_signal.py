"""
Trend indicator: macd_signal.

Phase 1:
approximation simple par moyenne glissante 9 périodes du macd_line.
Le builder calcule cette colonne explicitement.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="macd_signal",
    group_name="price_trend",
    required_columns=["macd_line"],
    output_columns=["macd_signal"],
    sql_select_expressions=[
        """
        AVG(macd_line) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
        ) AS macd_signal
        """.strip()
    ],
)
