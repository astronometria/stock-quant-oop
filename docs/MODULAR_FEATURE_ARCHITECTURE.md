# Modular Feature Architecture

## Goal

Move from a monolithic feature build to modular, point-in-time-safe feature tables.

## Canonical Sources

- `price_history`
- `short_features_daily`

## Modular Feature Tables

### `feature_price_momentum_daily`

Columns:

- `close`
- `returns_1d`
- `returns_5d`
- `returns_20d`
- `rsi_14`

### `feature_price_trend_daily`

Columns:

- `close`
- `sma_20`
- `sma_50`
- `sma_200`
- `close_to_sma_20`

### `feature_price_volatility_daily`

Columns:

- `close`
- `atr_14`
- `volatility_20`

### `feature_short_daily`

Columns:

- `short_interest`
- `days_to_cover`
- `short_volume_ratio`
- `short_interest_change_pct`
- `short_squeeze_score`
- `short_pressure_zscore`
- `days_to_cover_zscore`

## Research Composition Table

`research_features_daily`

This table composes all modular feature families into one research-facing table.

## Research Artifacts

- `research_training_dataset`
- `research_labels`
- `research_backtest`

## Benefits

- isolated rebuilds by feature family
- simpler debugging
- cleaner signal contracts
- easier future expansion

## Recommended Flow

```text
price_history / short_features_daily
    -> feature_*_daily
    -> research_features_daily
    -> research_training_dataset
    -> research_labels
    -> research_backtest
```

## Migration Notes

Temporary rebuild scripts under `cli/dev/` can be removed once the modular builders are validated and adopted as the official path.
