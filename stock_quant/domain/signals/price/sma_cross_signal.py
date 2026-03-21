"""
SMA Cross Signal

Signal long-only simple basé sur un croisement de moyennes mobiles.

Règles v1
---------
- si fast_sma > slow_sma : signal actif (1.0)
- sinon : flat (0.0)

Important
---------
- compute_signal_value est là pour le contrat, le debug et les tests
- l'exécution de recherche réelle reste SQL-first côté backtest
- aucune donnée future n'est utilisée ici
"""

from __future__ import annotations

from typing import Any

from stock_quant.domain.signals.base import BaseSignal


class SmaCrossSignal(BaseSignal):
    """
    Signal SMA crossover.

    Paramètres supportés
    --------------------
    - fast_feature_name: par défaut "sma_20"
    - slow_feature_name: par défaut "sma_50"
    """

    signal_name = "sma_cross"
    signal_version = "v1"

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        return {
            "fast_feature_name": "sma_20",
            "slow_feature_name": "sma_50",
        }

    @classmethod
    def required_features(cls) -> tuple[str, ...]:
        return ("sma_20", "sma_50")

    @classmethod
    def warmup_bars(cls) -> int:
        # Le croisement 20/50 a besoin d'au moins 50 barres
        return 50

    @classmethod
    def validate_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {
            **cls.default_params(),
            **(params or {}),
        }

        fast_feature_name = str(merged.get("fast_feature_name", "sma_20")).strip()
        slow_feature_name = str(merged.get("slow_feature_name", "sma_50")).strip()

        if not fast_feature_name:
            raise ValueError("fast_feature_name must be a non-empty string")
        if not slow_feature_name:
            raise ValueError("slow_feature_name must be a non-empty string")
        if fast_feature_name == slow_feature_name:
            raise ValueError("fast_feature_name and slow_feature_name must be different")

        allowed = {"sma_20", "sma_50", "sma_200"}
        if fast_feature_name not in allowed:
            raise ValueError(f"unsupported fast_feature_name={fast_feature_name!r}")
        if slow_feature_name not in allowed:
            raise ValueError(f"unsupported slow_feature_name={slow_feature_name!r}")

        return {
            "fast_feature_name": fast_feature_name,
            "slow_feature_name": slow_feature_name,
        }

    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        fast_name = str(self.params.get("fast_feature_name", "sma_20")).strip() or "sma_20"
        slow_name = str(self.params.get("slow_feature_name", "sma_50")).strip() or "sma_50"

        fast_value = row.get(fast_name)
        slow_value = row.get(slow_name)

        if fast_value is None or slow_value is None:
            return None

        if float(fast_value) > float(slow_value):
            return 1.0

        return 0.0
