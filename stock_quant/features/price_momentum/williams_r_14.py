"""
Momentum indicator: williams_r_14.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="williams_r_14",
    group_name="price_momentum",
    required_columns=["high", "low", "close"],
    output_columns=["williams_r_14"],
    sql_select_expressions=[
        """
        CASE
            WHEN (
                MAX(high) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) -
                MIN(low) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                )
            ) = 0 THEN NULL
            ELSE
                -100 * (
                    MAX(high) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) - close
                ) / (
                    MAX(high) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) -
                    MIN(low) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    )
                )
        END AS williams_r_14
        """.strip()
    ],
)
