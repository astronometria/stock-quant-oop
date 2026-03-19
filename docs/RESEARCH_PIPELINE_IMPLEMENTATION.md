# Research Pipeline Implementation

## What is in production code today

The current scientific research path is:

1. `build_research_training_dataset.py`
2. `build_research_labels.py`
3. `build_research_backtest.py`
4. `run_research_experiment.py`

## Step 1: Dataset build

Source tables:
- `price_history`
- `short_features_daily`
- split manifest metadata
- snapshot metadata

Output:
- `research_training_dataset`

Characteristics:
- month-by-month chunking
- backward as-of feature join
- memory-limit / threads / temp-dir runtime controls
- operator-visible progress logs

## Step 2: Labels build

Source tables:
- `research_training_dataset`
- `price_history`

Output:
- `research_labels`

Characteristics:
- dataset-scoped label generation
- 1d, 5d, 20d forward returns
- bad-close checks
- optional extreme-return sanitization through `--max-abs-return`

## Step 3: Backtest build

Source tables:
- `research_training_dataset`
- `research_labels`
- split manifest

Output:
- `research_backtest`

Characteristics:
- threshold-based long-only signal on `short_volume_ratio`
- split-aware reporting
- transaction-cost aware metrics

## Step 4: Experiment orchestration

`run_research_experiment.py` now:
- streams child logs live
- passes runtime settings consistently
- parses final child JSON robustly
- writes experiment metadata only after full success

## Known limitations

- signal logic is still tied to short-interest/short-volume feature usage
- there is no generalized factor registry yet
- portfolio construction is still very simple
- labels are cleaner after sanitization, but the research path is not yet a multi-signal framework

## Recommended near-term roadmap

1. keep dataset build generic
2. add more feature columns
3. externalize signal definitions from backtest code
4. support multiple strategy families without rewriting the backtest step
