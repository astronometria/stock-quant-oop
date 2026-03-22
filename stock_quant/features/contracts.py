"""
Contrats communs pour les indicateurs modulaires.

Le but est d'avoir une petite interface stable:
- nom technique de l'indicateur
- nom de la colonne produite
- expression SQL à projeter
- dépendances éventuelles
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence


@dataclass(frozen=True)
class FeatureIndicatorSpec:
    """
    Décrit un indicateur modulaire.

    name:
        Nom interne stable de l'indicateur.
    output_columns:
        Colonnes produites par l'indicateur.
    sql_select_expressions:
        Expressions SQL à injecter dans un SELECT.
        Chaque entrée doit déjà contenir son alias SQL final.
    required_input_columns:
        Colonnes minimales attendues dans la source.
    """
    name: str
    output_columns: Sequence[str]
    sql_select_expressions: Sequence[str]
    required_input_columns: Sequence[str] = field(default_factory=tuple)

    def validate(self) -> None:
        """
        Validation minimale défensive pour éviter de casser l'orchestrateur.
        """
        if not self.name.strip():
            raise ValueError("FeatureIndicatorSpec.name cannot be empty")
        if not self.output_columns:
            raise ValueError(f"{self.name}: output_columns cannot be empty")
        if not self.sql_select_expressions:
            raise ValueError(f"{self.name}: sql_select_expressions cannot be empty")


def validate_specs(specs: Iterable[FeatureIndicatorSpec]) -> list[FeatureIndicatorSpec]:
    """
    Valide une liste de specs et retourne une liste matérialisée.
    """
    materialized = list(specs)
    seen_names: set[str] = set()
    seen_outputs: set[str] = set()

    for spec in materialized:
        spec.validate()
        if spec.name in seen_names:
            raise ValueError(f"Duplicate indicator name: {spec.name}")
        seen_names.add(spec.name)

        for output in spec.output_columns:
            if output in seen_outputs:
                raise ValueError(f"Duplicate output column: {output}")
            seen_outputs.add(output)

    return materialized
