"""
Trend indicator: ema_12.

Note:
Pour rester SQL-first simple dans cette phase, on approxime EMA_12
par une moyenne mobile exponentielle proxy via une formule récursive
plus tard. Ici on publie une approximation stable basée sur SMA_12.

On garde ce fichier séparé pour pouvoir le remplacer plus tard
sans toucher au builder ni au registre.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="ema_12",
    group_name="price_trend",
    required_columns=["close"],
    output_columns=["ema_12"],
    sql_select_expressions=[
        """
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY as_of_date
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ema_12
        """.strip()
    ],
)
