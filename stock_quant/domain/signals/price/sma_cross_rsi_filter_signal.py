"""
Signal composite :
- Trend filter (SMA cross)
- Entry timing (RSI oversold)

Design :
- Compatible SQL-first (simple CASE)
- Paramétrable
- Très extensible
"""

from typing import Any, Dict, Tuple

from stock_quant.domain.signals.base_signal import BaseSignal


class SmaCrossRsiFilterSignal(BaseSignal):
    signal_name = "sma_cross_rsi_filter"
    signal_version = "v1"

    # -------------------------
    # Default params
    # -------------------------
    @staticmethod
    def default_params() -> Dict[str, Any]:
        return {
            "fast_feature_name": "sma_20",
            "slow_feature_name": "sma_50",
            "rsi_feature_name": "rsi_14",
            "rsi_threshold": 35.0,
        }

    # -------------------------
    # Required features
    # -------------------------
    @staticmethod
    def required_features() -> Tuple[str, ...]:
        return ("sma_20", "sma_50", "rsi_14")

    # -------------------------
    # Warmup
    # -------------------------
    @staticmethod
    def warmup_bars() -> int:
        return 50  # SMA50 dominant

    # -------------------------
    # Validation
    # -------------------------
    def validate_params(self) -> None:
        fast = self.params["fast_feature_name"]
        slow = self.params["slow_feature_name"]
        rsi_threshold = float(self.params["rsi_threshold"])

        allowed = {"sma_20", "sma_50", "sma_200"}

        if fast not in allowed:
            raise ValueError(f"Unsupported fast_feature_name={fast}")

        if slow not in allowed:
            raise ValueError(f"Unsupported slow_feature_name={slow}")

        if not (0.0 < rsi_threshold < 100.0):
            raise ValueError("rsi_threshold must be between 0 and 100")

    # -------------------------
    # Compute (python fallback)
    # -------------------------
    def compute_signal_value(self, features: Dict[str, Any]) -> float | None:
        fast = features.get(self.params["fast_feature_name"])
        slow = features.get(self.params["slow_feature_name"])
        rsi = features.get(self.params["rsi_feature_name"])

        if fast is None or slow is None or rsi is None:
            return None

        if fast > slow and rsi <= self.params["rsi_threshold"]:
            return 1.0

        return 0.0
