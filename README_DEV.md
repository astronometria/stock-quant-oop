# README_DEV

Developer-oriented documentation for `stock-quant-oop`.

## What is current in the codebase

The codebase is no longer just a generic market-data pipeline. It now has a working research branch with:

- chunked Stooq historical loading
- daily Yahoo price refresh
- SEC filing normalization
- fundamentals build entrypoint
- FINRA short-data history and derived short features
- chunked research dataset building
- dataset-scoped label generation with outlier filtering
- split-aware, cost-aware backtests
- experiment manifests with streamed child-process logging

## Design rules

### 1. Canonical over serving
Research must read canonical historical tables first:

- `price_history`
- normalized SEC tables
- normalized FINRA history tables
- `short_features_daily`

Serving tables are operational conveniences:

- `price_latest`
- any latest/snapshot helper table

### 2. Point-in-time safety
The dataset builder and feature joins must stay PIT-safe.
Current research flow does this by:
- anchoring rows on `research_training_dataset.as_of_date`
- using backward-looking joins for features
- keeping forward-looking logic confined to `research_labels`

### 3. SQL-first, Python-thin
DuckDB SQL should keep the heavy lifting for:
- canonical rebuilds
- date alignment
- feature joins
- label derivation
- backtest aggregation

Python should stay focused on:
- CLI surfaces
- runtime parameter plumbing
- progress bars
- temp-dir / thread / memory settings
- lightweight orchestration

### 4. Reproducibility
The research path is dataset-id based, not ad hoc.
Core identities:
- `snapshot_id`
- `split_id`
- `dataset_id`
- `backtest_id`
- `experiment_id`

## Current research pipeline

### Dataset build
Entrypoint:

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

Behavior:
- monthly chunking by partition window
- SQL-first temp tables
- backward-looking join from prices to `short_features_daily`
- optional symbol filtering mode in current code (`common_only` path)

### Labels build
Entrypoint:

```bash
python3 cli/core/build_research_labels.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --dataset-id <dataset_id> \
  --split-id <split_id> \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --max-abs-return 1.0 \
  --verbose
```

Behavior:
- dataset-scoped label build
- forward returns: 1d / 5d / 20d
- invalid close checks
- extreme-return filtering via `--max-abs-return`
- quality summary emitted as JSON/log lines

### Backtest build
Entrypoint:

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

Behavior:
- split-aware partitioning (`train`, `valid`, `test`)
- long-only threshold rule on `short_volume_ratio`
- turnover and cost accounting
- JSON output stable enough for the runner

### Full experiment runner
Entrypoint:

```bash
python3 cli/core/run_research_experiment.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --split-id <split_id> \
  --experiment-name research_scientific_v3 \
  --transaction-cost-bps 10 \
  --signal-threshold 0.5 \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

Behavior:
- streams child stdout/stderr live
- passes runtime settings to children
- parses trailing JSON payloads robustly
- writes experiment manifest only if all steps succeed

## Operational notes

### Memory-heavy jobs
For the large research path, current operational defaults in code are:

- `--memory-limit 24GB`
- `--threads 6`
- explicit `--temp-dir`

### Long-running price backfills
Use the chunked Stooq loader for historical rebuilds; it is materially better than a one-big-glob approach for visibility and stability.

### Logging
The repository already uses `logs/` heavily. Keep heavy runs streamed and tee'd to log files.

### DuckDB locking
Do not open a second writer against the same `.duckdb` file while a heavy writer is already running.

## What is still not generalized

The current research backtest is **not** yet a signal framework.
It is still effectively:

```text
short_volume_ratio > threshold -> long, else flat
```

That means:
- the runner is reusable
- the dataset/labels/backtest plumbing is reusable
- the signal logic is still strategy-specific

A next step would be to decouple:
1. features
2. signal definition
3. portfolio construction
4. evaluation
