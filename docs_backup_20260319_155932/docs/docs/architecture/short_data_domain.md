# Short Data Domain

## Purpose

The short-data domain combines two different but complementary FINRA datasets:

- short interest
- daily short sale volume

The domain exists so the repository can derive research-safe short pressure features without mixing serving shortcuts into historical analysis.

## Core tables

### Raw stage
- `finra_short_interest_source_raw`
- `finra_daily_short_volume_source_raw`

### Source inventory / metadata
- `finra_short_interest_sources`
- `finra_daily_short_volume_sources`

### Canonical historical tables
- `finra_short_interest_history`
- `daily_short_volume_history`

### Serving table
- `finra_short_interest_latest`

### Derived feature table
- `short_features_daily`

## Why both datasets matter

### Short interest
Useful for:
- total short positioning
- days to cover
- short interest change
- short interest as a percentage of float

### Daily short volume
Useful for:
- same-day short participation
- rolling short volume intensity
- near-term short pressure

Together they allow a more useful daily feature set than either dataset alone.

## Point-in-time design

### Daily short volume
This dataset must preserve:
- `trade_date`
- `available_at`

### Short interest
This dataset must preserve:
- `settlement_date`
- `ingested_at`

### Derived short features
A valid feature row must only attach short interest information that was available by the feature row’s source-availability cutoff.

That prevents accidental future leakage.

## Bias rules

### No look-ahead bias
Do not attach a short-interest observation to a day before that short-interest record was actually available.

### No survivor bias
Keep historical rows for symbols that later disappear.

### No serving-table substitution
Do not rebuild research features from `finra_short_interest_latest`.

## Current status

The short-data domain is now functional:
- raw short volume load works
- canonical short volume history works
- canonical short interest history works
- derived short features work
- heavy short-feature rebuilds are now orchestrator-skippable when unchanged
