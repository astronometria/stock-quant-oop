# Research Backtest Engine

## Current implementation

The current backtest implementation is not the older generic top-N cross-sectional engine described in prior notes.

The code that exists today builds:

- `research_backtest`

from:

- `research_training_dataset`
- `research_labels`
- `research_split_manifest`

## Entrypoint

```bash
python3 cli/core/build_research_backtest.py \
  --db-path /path/to/market.duckdb \
  --dataset-id <dataset_id> \
  --split-id <split_id> \
  --transaction-cost-bps 10 \
  --signal-threshold 0.5 \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

## Current signal logic

At the moment, the strategy is effectively:

```text
if short_volume_ratio > signal_threshold:
    signal = 1
else:
    signal = 0
```

So the current engine is:

- long-only
- threshold-based
- single-signal
- split-aware
- transaction-cost aware

## Current outputs

Backtest rows are stored with:
- `backtest_id`
- `dataset_id`
- `split_id`
- `partition_name`
- `signal_name`
- `transaction_cost_bps`
- performance metrics such as gross return, net return, sharpe, turnover, and observation count

## What changed versus older docs

Older docs described:
- a `research_dataset_daily` input
- a generic rank/top-N selector
- position tables and daily tables that are not the current main implementation

Those descriptions are now stale relative to the code.

## Recommended next evolution

The clean next step is to split the logic into:

1. **feature columns**
2. **signal definition**
3. **portfolio construction**
4. **evaluation**

Right now, only step 4 is generic.
The signal definition is still strategy-specific.
