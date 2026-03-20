from __future__ import annotations

"""
RSI Threshold Signal

Signal long-only basé sur RSI.

Règles
------
- RSI <= oversold_threshold  -> long (signal_value = 1.0)
- RSI >= overbought_threshold -> flat (signal_value = 0.0)
- sinon -> flat (signal_value = 0.0)

Notes research-grade
--------------------
- dépend uniquement d'une feature PIT-safe : rsi_14
- ne reconstruit jamais l'univers
- compatible avec execution_lag_bars >= 1
- le calcul réel du backtest reste SQL-first ; la méthode Python ci-dessous
  sert surtout au contrat domaine, aux probes et aux futurs tests unitaires
"""

from typing import Any

from stock_quant.domain.signals.base import BaseSignal


class RsiThresholdSignal(BaseSignal):
    """
    Signal de seuil RSI simple.

    Paramètres attendus
    -------------------
    - feature_name: nom de la colonne RSI dans le dataset
    - oversold_threshold: seuil de survente
    - overbought_threshold: seuil de surachat
    """

    signal_name = "rsi_threshold"
    signal_version = "v1"

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        """
        Paramètres par défaut conservateurs.
        """
        return {
            "feature_name": "rsi_14",
            "oversold_threshold": 30.0,
            "overbought_threshold": 70.0,
        }

    @classmethod
    def required_features(cls) -> tuple[str, ...]:
        """
        Ce signal lit uniquement une colonne RSI.
        """
        return ("rsi_14",)

    @classmethod
    def warmup_bars(cls) -> int:
        """
        RSI(14) nécessite un historique minimal explicite.
        """
        return 14

    @classmethod
    def validate_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Valide et normalise les paramètres du signal.
        """
        merged = dict(cls.default_params())
        merged.update(params or {})

        feature_name = str(merged.get("feature_name", "")).strip()
        if not feature_name:
            raise ValueError("feature_name must be a non-empty string")

        try:
            oversold_threshold = float(merged.get("oversold_threshold", 30.0))
        except (TypeError, ValueError) as exc:
            raise ValueError("oversold_threshold must be numeric") from exc

        try:
            overbought_threshold = float(merged.get("overbought_threshold", 70.0))
        except (TypeError, ValueError) as exc:
            raise ValueError("overbought_threshold must be numeric") from exc

        if oversold_threshold < 0 or oversold_threshold > 100:
            raise ValueError("oversold_threshold must be between 0 and 100")

        if overbought_threshold < 0 or overbought_threshold > 100:
            raise ValueError("overbought_threshold must be between 0 and 100")

        if oversold_threshold >= overbought_threshold:
            raise ValueError("oversold_threshold must be strictly lower than overbought_threshold")

        return {
            "feature_name": feature_name,
            "oversold_threshold": oversold_threshold,
            "overbought_threshold": overbought_threshold,
        }

    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        """
        Calcule la valeur de signal sur une ligne.

        Convention v1
        -------------
        - feature absente/invalide => None
        - RSI <= oversold          => 1.0
        - RSI >= overbought        => 0.0
        - entre les deux           => 0.0
        """
        feature_name = str(self.params["feature_name"])
        oversold_threshold = float(self.params["oversold_threshold"])
        overbought_threshold = float(self.params["overbought_threshold"])

        raw_value = row.get(feature_name)
        if raw_value is None:
            return None

        try:
            rsi_value = float(raw_value)
        except (TypeError, ValueError):
            return None

        if rsi_value <= oversold_threshold:
            return 1.0

        if rsi_value >= overbought_threshold:
            return 0.0

        return 0.0
