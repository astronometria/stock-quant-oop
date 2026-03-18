# Repo Walkthrough

## High-level view

`stock-quant-oop` is organized around a simple rule:

- raw sources land first
- canonical historical tables are built second
- derived research features are built from canonical history
- serving tables exist, but research should not depend on them

## Main directories

### `cli/`
Entry points for running the project.

- `cli/core/` — canonical build commands
- `cli/raw/` — raw staging loaders
- `cli/ops/` — orchestration / operations helpers

### `stock_quant/`
Application code.

- `app/` — services and DTOs
- `domain/` — entities and domain policies
- `infrastructure/` — repositories, providers, schema managers
- `pipelines/` — SQL-first build logic

### `docs/`
Human-readable documentation for operations, architecture, and pipelines.

### `logs/`
Runtime logs written by jobs and orchestration runs.

## Data flow

1. provider or local source files are discovered
2. raw loaders stage source-fidelity data
3. canonical builders normalize historical tables
4. derived builders generate research features
5. orchestrator runs daily refreshes

## Short-data domain walkthrough

### FINRA short interest
Main path:
- raw data
- `finra_short_interest_source_raw`
- `finra_short_interest_history`
- `finra_short_interest_latest`

### FINRA daily short volume
Main path:
- FINRA raw files
- `finra_daily_short_volume_source_raw`
- `finra_daily_short_volume_sources`
- `daily_short_volume_history`

### Derived short features
Main path:
- `daily_short_volume_history`
- `finra_short_interest_history`
- `short_features_daily`

## Daily orchestration walkthrough

`cli/run_daily_pipeline.py` currently:
- runs prices
- probes and conditionally skips FINRA daily short volume
- runs FINRA short interest
- probes and conditionally skips short features
- runs SEC filings

This means the heaviest short-data rebuild no longer runs every pass.

## How to think about tables

### Canonical tables
Use these for research:
- `price_history`
- `sec_filing`
- `finra_short_interest_history`
- `daily_short_volume_history`
- `short_features_daily`

### Serving tables
Use these for current-state access, not historical backtests:
- `price_latest`
- `finra_short_interest_latest`

## How to extend the repo safely

When adding a new dataset:
1. create raw stage
2. create canonical historical table
3. keep the transform SQL-first
4. preserve source availability timestamps where needed
5. add orchestration only after the canonical path is stable
