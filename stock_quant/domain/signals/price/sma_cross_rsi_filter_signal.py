from __future__ import annotations

from typing import Any

from stock_quant.domain.signals.base import BaseSignal


class SmaCrossRsiFilterSignal(BaseSignal):
    signal_name = "sma_cross_rsi_filter"
    signal_version = "v1"

    @classmethod
    def default_params(cls) -> dict[str, Any]:
        return {
            "fast_feature_name": "sma_20",
            "slow_feature_name": "sma_50",
            "rsi_feature_name": "rsi_14",
            "rsi_threshold": 35.0,
        }

    @classmethod
    def required_features(cls) -> tuple[str, ...]:
        return ("sma_20", "sma_50", "rsi_14")

    @classmethod
    def warmup_bars(cls) -> int:
        return 50

    @classmethod
    def validate_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {
            **cls.default_params(),
            **(params or {}),
        }

        fast_feature_name = str(merged.get("fast_feature_name", "sma_20")).strip()
        slow_feature_name = str(merged.get("slow_feature_name", "sma_50")).strip()
        rsi_feature_name = str(merged.get("rsi_feature_name", "rsi_14")).strip()
        rsi_threshold = float(merged.get("rsi_threshold", 35.0))

        allowed = {"sma_20", "sma_50", "sma_200"}

        if not fast_feature_name:
            raise ValueError("fast_feature_name must be a non-empty string")
        if not slow_feature_name:
            raise ValueError("slow_feature_name must be a non-empty string")
        if not rsi_feature_name:
            raise ValueError("rsi_feature_name must be a non-empty string")
        if fast_feature_name == slow_feature_name:
            raise ValueError("fast_feature_name and slow_feature_name must be different")
        if fast_feature_name not in allowed:
            raise ValueError(f"unsupported fast_feature_name={fast_feature_name!r}")
        if slow_feature_name not in allowed:
            raise ValueError(f"unsupported slow_feature_name={slow_feature_name!r}")
        if not 0.0 < rsi_threshold < 100.0:
            raise ValueError("rsi_threshold must be between 0 and 100")

        return {
            "fast_feature_name": fast_feature_name,
            "slow_feature_name": slow_feature_name,
            "rsi_feature_name": rsi_feature_name,
            "rsi_threshold": rsi_threshold,
        }

    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        fast_name = str(self.params.get("fast_feature_name", "sma_20")).strip() or "sma_20"
        slow_name = str(self.params.get("slow_feature_name", "sma_50")).strip() or "sma_50"
        rsi_name = str(self.params.get("rsi_feature_name", "rsi_14")).strip() or "rsi_14"
        rsi_threshold = float(self.params.get("rsi_threshold", 35.0))

        fast_value = row.get(fast_name)
        slow_value = row.get(slow_name)
        rsi_value = row.get(rsi_name)

        if fast_value is None or slow_value is None or rsi_value is None:
            return None

        if float(fast_value) > float(slow_value) and float(rsi_value) <= rsi_threshold:
            return 1.0

        return 0.0
