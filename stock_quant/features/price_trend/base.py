"""
Base utilitaire pour les indicateurs trend.
"""

from __future__ import annotations

from stock_quant.features.contracts import FeatureIndicatorSpec


def trend_spec(
    *,
    name: str,
    output_columns: list[str],
    sql_select_expressions: list[str],
    required_input_columns: list[str],
) -> FeatureIndicatorSpec:
    return FeatureIndicatorSpec(
        name=name,
        output_columns=output_columns,
        sql_select_expressions=sql_select_expressions,
        required_input_columns=required_input_columns,
    )
