from __future__ import annotations

"""
Signal encapsulé de type threshold sur short_volume_ratio.

Pourquoi commencer par celui-ci
-------------------------------
Le README public du repo indique que le backtest de recherche actuel
reste une stratégie single-rule long-only threshold sur short_volume_ratio.
On encapsule donc d'abord ce comportement existant avant d'ajouter
RSI, MACD et autres signaux prix. Cela minimise le risque de régression.
"""

import json
from typing import Any

from stock_quant.domain.signals.base import BaseSignal


class ShortVolumeRatioThresholdSignal(BaseSignal):
    """
    Signal long-only simple basé sur un seuil sur short_volume_ratio.

    Règle
    -----
    - si short_volume_ratio >= threshold : signal_value = short_volume_ratio
    - sinon                              : signal_value = 0.0

    Notes research-grade
    --------------------
    - Le signal ne reconstruit jamais l'univers.
    - Le signal suppose que la colonne short_volume_ratio est déjà PIT-safe
      et alignée sur as_of_date dans le dataset d'entrée.
    - L'exécution doit être différée d'au moins 1 barre.
    """

    signal_name = "short_volume_ratio_threshold"
    signal_version = "v1"

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        """
        Paramètres par défaut conservateurs.
        """
        return {
            "feature_name": "short_volume_ratio",
            "threshold": 0.5,
            "zero_below_threshold": True,
        }

    @classmethod
    def required_features(cls) -> tuple[str, ...]:
        """
        Le signal lit uniquement short_volume_ratio par défaut.
        """
        return ("short_volume_ratio",)

    @classmethod
    def warmup_bars(cls) -> int:
        """
        Pas de warmup technique spécial ici.
        """
        return 1

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
            threshold = float(merged.get("threshold", 0.5))
        except (TypeError, ValueError) as exc:
            raise ValueError("threshold must be numeric") from exc

        zero_below_threshold = bool(merged.get("zero_below_threshold", True))

        return {
            "feature_name": feature_name,
            "threshold": threshold,
            "zero_below_threshold": zero_below_threshold,
        }

    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        """
        Calcule la valeur du signal pour une ligne.

        Convention v1
        -------------
        - feature absente ou invalide => None
        - feature < threshold         => 0.0 ou la feature brute selon paramètre
        - feature >= threshold        => feature brute

        Cette convention garde un classement cross-sectional simple et
        proche du comportement historique.
        """
        feature_name = str(self.params["feature_name"])
        threshold = float(self.params["threshold"])
        zero_below_threshold = bool(self.params["zero_below_threshold"])

        raw_value = row.get(feature_name)
        if raw_value is None:
            return None

        try:
            feature_value = float(raw_value)
        except (TypeError, ValueError):
            return None

        if feature_value >= threshold:
            return feature_value

        if zero_below_threshold:
            return 0.0

        return feature_value

    def params_json(self) -> str:
        """
        Sérialisation utilitaire stable des paramètres pour audit futur.
        """
        return json.dumps(self.params, sort_keys=True)
