# Research Dataset Builder

## Current implementation target

The current research dataset builder writes to:

- `research_training_dataset`

not to the older `training_dataset_daily` naming used in older docs.

## Current schema

Each row is currently anchored on:

- `dataset_id`
- `snapshot_id`
- `symbol`
- `as_of_date`
- `close`
- `short_volume_ratio`

with `created_at` as metadata.

## Entrypoint

```bash
python3 cli/core/build_research_training_dataset.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --split-id <split_id> \
  --dataset-id <optional_dataset_id> \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

## How it works now

The current builder is:

- chunked by month
- partition-aware (`train`, `valid`, `test`)
- SQL-first
- memory-safe relative to a one-shot global join
- backward-looking on `short_features_daily`

The broad shape is:

```text
price_history chunk
+ backward-asof short_features_daily
-> research_training_dataset
```

## Important implementation details

### 1. Chunking
The builder creates monthly windows to cap join size and temp spill.

### 2. PIT-safe feature join
The join to short features is backward-looking:
- same symbol
- latest feature row with `feature.as_of_date <= price.as_of_date`

### 3. Dataset-scoped identity
The dataset is tied to:
- a snapshot
- a split manifest
- an explicit `dataset_id`

### 4. Symbol filtering
Recent code added an early source-side symbol filter mode for excluding likely:
- warrants
- rights
- units
- special/test symbols

That filter is materially better than trying to clean only at the label or backtest stage.

## What this builder is not yet

It is not yet a generalized multi-feature research matrix.
The current version still focuses on:
- prices
- short-derived feature(s)

A next step would be to add more features cleanly, such as technical indicators or fundamentals, without breaking PIT guarantees.
