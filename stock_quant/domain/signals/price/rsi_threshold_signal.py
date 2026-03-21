"""
RSI Threshold Signal

Signal long-only simple basé sur RSI14.

Règles
------
- si rsi_14 <= oversold_threshold : signal actif (1.0)
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


class RsiThresholdSignal(BaseSignal):
    """
    Signal RSI14 threshold.

    Paramètres supportés
    --------------------
    - feature_name: par défaut "rsi_14"
    - oversold_threshold: par défaut 30.0
    - overbought_threshold: par défaut 70.0

    Note
    ----
    La règle v1 est long-only :
    - RSI <= oversold_threshold => 1.0
    - sinon => 0.0

    Le paramètre overbought_threshold est conservé pour stabilité d'interface,
    documentation et évolutions futures.
    """

    signal_name = "rsi_threshold"
    signal_version = "v1"

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        """
        Paramètres par défaut du signal.
        """
        return {
            "feature_name": "rsi_14",
            "oversold_threshold": 30.0,
            "overbought_threshold": 70.0,
        }

    @classmethod
    def required_features(cls) -> tuple[str, ...]:
        """
        Features minimales requises par le signal.

        Important:
        - le contrat de classe doit rester stable
        - la feature réellement consommée peut être redéfinie via params
        - pour les contrôles statiques, on annonce le cas nominal
        """
        return ("rsi_14",)

    @classmethod
    def warmup_bars(cls) -> int:
        """
        RSI14 nécessite un warmup minimal de 14 barres.
        """
        return 14

    @classmethod
    def validate_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Valide et normalise les paramètres du signal.
        """
        merged: dict[str, Any] = {
            **cls.default_params(),
            **(params or {}),
        }

        feature_name = str(merged.get("feature_name", "rsi_14")).strip()
        if not feature_name:
            raise ValueError("feature_name must be a non-empty string")

        oversold = merged.get("oversold_threshold", 30.0)
        overbought = merged.get("overbought_threshold", 70.0)

        if not isinstance(oversold, (int, float)):
            raise ValueError("oversold_threshold must be numeric")
        if not isinstance(overbought, (int, float)):
            raise ValueError("overbought_threshold must be numeric")

        oversold = float(oversold)
        overbought = float(overbought)

        if not 0.0 <= oversold <= 100.0:
            raise ValueError("oversold_threshold must be between 0 and 100")
        if not 0.0 <= overbought <= 100.0:
            raise ValueError("overbought_threshold must be between 0 and 100")
        if oversold > overbought:
            raise ValueError("oversold_threshold cannot be greater than overbought_threshold")

        return {
            "feature_name": feature_name,
            "oversold_threshold": oversold,
            "overbought_threshold": overbought,
        }

    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        """
        Contrat de signal pour debug/tests.

        Retour:
        - None si feature absente
        - 1.0 si RSI <= oversold_threshold
        - 0.0 sinon
        """
        feature_name = str(self.params.get("feature_name", "rsi_14")).strip() or "rsi_14"
        value = row.get(feature_name)

        if value is None:
            return None

        rsi = float(value)
        oversold = float(self.params.get("oversold_threshold", 30.0))

        if rsi <= oversold:
            return 1.0

        return 0.0
