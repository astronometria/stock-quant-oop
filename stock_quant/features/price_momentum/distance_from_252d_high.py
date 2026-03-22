"""
Indicateur distance_from_252d_high.
"""

from __future__ import annotations

from stock_quant.features.price_momentum.base import momentum_spec

SPEC = momentum_spec(
    name="distance_from_252d_high",
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
    required_input_columns=["symbol", "as_of_date", "close"],
)
