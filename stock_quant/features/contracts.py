"""
Contrats communs pour les indicateurs modulaires.

On garde un contrat très simple :
- nom stable de l'indicateur
- groupe fonctionnel
- colonnes nécessaires en entrée
- colonnes produites en sortie
- expressions SQL à injecter dans le builder

Le but est de permettre à un builder de groupe
(momentum / trend / volatility / short / etc.)
de lire un registre unique et d'assembler sa projection SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence


@dataclass(frozen=True)
class IndicatorSpec:
    """
    Décrit un indicateur modulaire calculé par SQL.

    name:
        Nom technique unique et stable.
    group_name:
        Nom du groupe logique, ex: price_momentum.
    required_columns:
        Colonnes attendues dans le dataset de travail du builder.
    output_columns:
        Colonnes produites par cet indicateur.
    sql_select_expressions:
        Expressions SQL déjà aliasées, injectées dans un SELECT.
    """
    name: str
    group_name: str
    required_columns: Sequence[str]
    output_columns: Sequence[str]
    sql_select_expressions: Sequence[str] = field(default_factory=tuple)

    def validate(self) -> None:
        """
        Validation défensive minimale.
        """
        if not self.name or not self.name.strip():
            raise ValueError("IndicatorSpec.name cannot be empty")
        if not self.group_name or not self.group_name.strip():
            raise ValueError(f"{self.name}: group_name cannot be empty")
        if not self.output_columns:
            raise ValueError(f"{self.name}: output_columns cannot be empty")
        if not self.sql_select_expressions:
            raise ValueError(f"{self.name}: sql_select_expressions cannot be empty")


def validate_indicator_specs(specs: Iterable[IndicatorSpec]) -> list[IndicatorSpec]:
    """
    Valide une liste de specs et détecte les doublons dangereux.
    """
    materialized = list(specs)
    seen_names: set[str] = set()
    seen_output_columns: set[str] = set()

    for spec in materialized:
        spec.validate()

        if spec.name in seen_names:
            raise ValueError(f"Duplicate indicator name detected: {spec.name}")
        seen_names.add(spec.name)

        for column_name in spec.output_columns:
            if column_name in seen_output_columns:
                raise ValueError(
                    f"Duplicate output column detected: {column_name}"
                )
            seen_output_columns.add(column_name)

    return materialized
