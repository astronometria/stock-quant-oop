from __future__ import annotations

import pytest

from stock_quant.domain.signals.price.sma_cross_signal import SmaCrossSignal


def test_sma_cross_default_params() -> None:
    params = SmaCrossSignal.default_params()
    assert params["fast_feature_name"] == "sma_20"
    assert params["slow_feature_name"] == "sma_50"


def test_sma_cross_required_features() -> None:
    assert SmaCrossSignal.required_features() == ("sma_20", "sma_50")


def test_sma_cross_warmup() -> None:
    assert SmaCrossSignal.warmup_bars() == 50


def test_sma_cross_returns_none_when_feature_missing() -> None:
    signal = SmaCrossSignal(params={})
    assert signal.compute_signal_value({}) is None
    assert signal.compute_signal_value({"sma_20": 10.0}) is None


def test_sma_cross_triggers_long_when_fast_above_slow() -> None:
    signal = SmaCrossSignal(params={})
    assert signal.compute_signal_value({"sma_20": 11.0, "sma_50": 10.0}) == 1.0


def test_sma_cross_returns_flat_when_fast_not_above_slow() -> None:
    signal = SmaCrossSignal(params={})
    assert signal.compute_signal_value({"sma_20": 10.0, "sma_50": 10.0}) == 0.0
    assert signal.compute_signal_value({"sma_20": 9.0, "sma_50": 10.0}) == 0.0


def test_sma_cross_validate_params_rejects_same_feature() -> None:
    with pytest.raises(ValueError):
        SmaCrossSignal.validate_params(
            {
                "fast_feature_name": "sma_20",
                "slow_feature_name": "sma_20",
            }
        )
