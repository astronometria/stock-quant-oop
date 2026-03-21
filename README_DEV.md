# README_DEV

## Dev Architecture

This project follows a strict layered architecture:

```text
sources -> features -> composition -> dataset -> labels -> backtest
```

## Layers

### Sources

- `price_history`
- `short_features_daily`

### Feature Layer

Builders:

- `build_feature_price_momentum.py`
- `build_feature_price_trend.py`
- `build_feature_price_volatility.py`
- `build_feature_short.py`

Rules:

- SQL-first
- no forward-looking data
- independently rebuildable
- one family per table

### Composition Layer

`research_features_daily`

This table joins all modular feature tables on:

- `symbol`
- `as_of_date`

### Dataset Layer

`research_training_dataset`

Rules:

- projection of `research_features_daily`
- incremental build supported
- feature columns explicitly controlled by `TARGET_FEATURE_COLUMNS`

### Labels Layer

`research_labels`

Rules:

- built after dataset
- tied to `dataset_id`

### Backtest Layer

`research_backtest`

Rules:

- consumes dataset + labels
- split-aware: `train / valid / test`
- signal logic remains SQL-first

## Adding a New Indicator

1. Create a new feature table or extend the right feature family.
2. Build it from canonical sources using SQL window functions.
3. Add it to `research_features_daily`.
4. Add it to dataset projection if needed.
5. Consume it in a signal.

## PIT Safety Rules

- Never use future rows to compute a feature.
- For same-frequency daily joins, use exact date joins.
- For delayed sources, use `available_at <= as_of_date`.

Reference pattern:

```sql
f.as_of_date <= p.as_of_date
ORDER BY f.as_of_date DESC
LIMIT 1
```

## Debug Order

Always probe in this order:

1. feature table coverage
2. `research_features_daily` coverage
3. dataset coverage
4. labels coverage
5. backtest output

## Performance Rules

- use DuckDB window functions
- avoid Python row loops
- keep joins narrow
- chunk large builds
- prefer incremental rebuilds when safe

## Reference Signal

`rsi_threshold`

Use it as the template for future signals such as:

- `sma_cross`
- volatility regime filters
- composite multi-feature signals
