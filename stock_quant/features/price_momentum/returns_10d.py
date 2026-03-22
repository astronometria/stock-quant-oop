"""
Indicateur returns_10d.

Hypothèse:
- la table source contient déjà close et les lignes sont une série journalière
  par symbole triées par as_of_date.
- le calcul final sera injecté dans une requête SQL window.
"""

from __future__ import annotations

from stock_quant.features.price_momentum.base import momentum_spec

SPEC = momentum_spec(
    name="returns_10d",
    output_columns=["returns_10d"],
    sql_select_expressions=[
        """
        CASE
            WHEN LAG(close, 10) OVER price_w IS NULL OR LAG(close, 10) OVER price_w = 0 THEN NULL
            ELSE (close / LAG(close, 10) OVER price_w) - 1
        END AS returns_10d
        """.strip()
    ],
    required_input_columns=["symbol", "as_of_date", "close"],
)
