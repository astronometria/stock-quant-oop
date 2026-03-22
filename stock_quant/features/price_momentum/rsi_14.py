"""
Momentum indicator: rsi_14.

Important:
Le builder doit préparer en amont:
- avg_gain_14
- avg_loss_14

Ce fichier décrit seulement l'expression finale.
"""

from __future__ import annotations

from stock_quant.features.contracts import IndicatorSpec

SPEC = IndicatorSpec(
    name="rsi_14",
    group_name="price_momentum",
    required_columns=["avg_gain_14", "avg_loss_14"],
    output_columns=["rsi_14"],
    sql_select_expressions=[
        """
        CASE
            WHEN avg_loss_14 IS NULL THEN NULL
            WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50
            WHEN avg_loss_14 = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
        END AS rsi_14
        """.strip()
    ],
)
