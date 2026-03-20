from __future__ import annotations

"""
Registry/factory des signaux disponibles.

Objectif
--------
Permettre de résoudre un signal par nom sans hardcoder la logique
dans les pipelines ou dans le moteur de backtest.
"""

from typing import Any

from stock_quant.domain.signals.base import BaseSignal
from stock_quant.domain.signals.price import RsiThresholdSignal
from stock_quant.domain.signals.short.short_volume_ratio_threshold_signal import (
    ShortVolumeRatioThresholdSignal,
)


class SignalRegistry:
    """
    Registre en mémoire des classes de signaux.

    Notes
    -----
    - On enregistre des classes, pas des instances.
    - L'instance est créée via create().
    """

    def __init__(self) -> None:
        self._signals: dict[str, type[BaseSignal]] = {}

    def register(self, signal_cls: type[BaseSignal]) -> None:
        """
        Enregistre une classe de signal sous son nom public.
        """
        signal_name = str(signal_cls.signal_name).strip()
        if not signal_name:
            raise ValueError("signal_cls.signal_name must be a non-empty string")

        if signal_name in self._signals:
            raise ValueError(f"signal '{signal_name}' is already registered")

        self._signals[signal_name] = signal_cls

    def has(self, signal_name: str) -> bool:
        """
        Vérifie si un signal est enregistré.
        """
        key = str(signal_name).strip()
        return key in self._signals

    def get(self, signal_name: str) -> type[BaseSignal]:
        """
        Retourne la classe d'un signal enregistré.
        """
        key = str(signal_name).strip()
        if key not in self._signals:
            available = ", ".join(sorted(self._signals))
            raise KeyError(f"unknown signal '{key}'. available signals: [{available}]")
        return self._signals[key]

    def create(self, signal_name: str, params: dict[str, Any] | None = None) -> BaseSignal:
        """
        Crée une instance de signal à partir de son nom public.
        """
        signal_cls = self.get(signal_name)
        return signal_cls(params=params)

    def available_signals(self) -> tuple[str, ...]:
        """
        Retourne la liste triée des noms de signaux disponibles.
        """
        return tuple(sorted(self._signals.keys()))


def build_default_signal_registry() -> SignalRegistry:
    """
    Construit le registry par défaut du projet.
    """
    registry = SignalRegistry()
    registry.register(ShortVolumeRatioThresholdSignal)
    registry.register(RsiThresholdSignal)
    return registry
