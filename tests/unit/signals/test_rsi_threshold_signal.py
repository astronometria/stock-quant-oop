from __future__ import annotations

import pytest

from stock_quant.domain.signals.price.rsi_threshold_signal import RsiThresholdSignal


def test_rsi_threshold_default_params() -> None:
    params = RsiThresholdSignal.default_params()
    assert params["feature_name"] == "rsi_14"
    assert params["oversold_threshold"] == 30.0
    assert params["overbought_threshold"] == 70.0


def test_rsi_threshold_required_features_default() -> None:
    assert RsiThresholdSignal.required_features() == ("rsi_14",)


def test_rsi_threshold_warmup() -> None:
    assert RsiThresholdSignal.warmup_bars() == 14


def test_rsi_threshold_returns_none_when_feature_missing() -> None:
    signal = RsiThresholdSignal(params={})
    assert signal.compute_signal_value({}) is None


def test_rsi_threshold_triggers_long_when_oversold() -> None:
    signal = RsiThresholdSignal(
        params={
            "feature_name": "rsi_14",
            "oversold_threshold": 30.0,
            "overbought_threshold": 70.0,
        }
    )
    assert signal.compute_signal_value({"rsi_14": 25.0}) == 1.0


def test_rsi_threshold_returns_flat_when_not_oversold() -> None:
    signal = RsiThresholdSignal(
        params={
            "feature_name": "rsi_14",
            "oversold_threshold": 30.0,
            "overbought_threshold": 70.0,
        }
    )
    assert signal.compute_signal_value({"rsi_14": 45.0}) == 0.0
    assert signal.compute_signal_value({"rsi_14": 80.0}) == 0.0


def test_rsi_threshold_validate_params_rejects_invalid_order() -> None:
    with pytest.raises(ValueError):
        RsiThresholdSignal.validate_params(
            {
                "feature_name": "rsi_14",
                "oversold_threshold": 80.0,
                "overbought_threshold": 70.0,
            }
        )
