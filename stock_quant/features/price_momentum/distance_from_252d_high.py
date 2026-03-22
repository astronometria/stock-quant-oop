"""
Momentum indicator: distance_from_252d_high.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="distance_from_252d_high",
    group_name="price_momentum",
    required_columns=["close"],
    output_columns=["distance_from_252d_high"],
    sql_select_expressions=[
        """
        CASE
            WHEN MAX(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) = 0 THEN NULL
            ELSE
                (close / MAX(close) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                )) - 1
        END AS distance_from_252d_high
        """.strip()
    ],
)
