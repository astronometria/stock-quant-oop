"""
Indicateur macd_hist.

Note:
- ce fichier prépare seulement l'expression finale.
- si le pipeline ne calcule pas encore ema_12 / ema_26 / macd_signal, il faudra
  les ajouter dans la requête amont du builder trend.
"""

from __future__ import annotations

from stock_quant.features.price_trend.base import trend_spec

SPEC = trend_spec(
    name="macd_hist",
    output_columns=["macd_hist"],
    sql_select_expressions=[
        "(macd_line - macd_signal) AS macd_hist"
    ],
    required_input_columns=["macd_line", "macd_signal"],
)
