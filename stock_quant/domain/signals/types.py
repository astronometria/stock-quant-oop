from __future__ import annotations

"""
Types simples liés à l'exécution des signaux.

Le but est de formaliser le contrat sans encore imposer une implémentation
pipeline/repository spécifique.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SignalSpec:
    """
    Description déclarative d'un signal demandé par un run.

    Attributes
    ----------
    signal_name:
        Nom stable du signal à résoudre via le registry.
    signal_version:
        Version attendue de la logique. Utile pour l'audit et la reproductibilité.
    params:
        Paramètres métier du signal.
    """
    signal_name: str
    signal_version: str = "v1"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SignalExecutionContext:
    """
    Contexte d'exécution commun aux futurs moteurs de signaux.

    Attributes
    ----------
    dataset_name:
        Dataset d'entrée sur lequel on calcule le signal.
    dataset_version:
        Version explicite du dataset.
    execution_lag_bars:
        Décalage décision -> exécution. Doit être >= 1.
    frequency:
        Fréquence des données. Daily pour l'instant.
    universe_name:
        Nom logique de l'univers déjà filtré PIT.
    """
    dataset_name: str
    dataset_version: str
    execution_lag_bars: int = 1
    frequency: str = "daily"
    universe_name: str | None = None


@dataclass(slots=True)
class SignalRecord:
    """
    Sortie standardisée minimale d'une ligne de signal.

    Cette structure servira plus tard de base à une table de signaux
    persistée ou temporaire.
    """
    instrument_id: str | None
    company_id: str | None
    symbol: str | None
    as_of_date: Any
    signal_name: str
    signal_version: str
    signal_value: float | None
    signal_direction: int
    is_active: bool
    params_json: str
