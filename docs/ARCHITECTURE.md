# Architecture

## Overview

`stock-quant-oop` currently has two main tracks:

1. **data engineering / canonical history**
2. **research / experiment execution**

That distinction matters because the codebase now contains both:
- operational wrappers for daily refreshes
- scientific-style dataset / label / backtest builders

## Layering

### `stock_quant/domain`
Pure business rules and policy objects.

Typical responsibilities:
- inclusion / exclusion policies
- classification rules
- normalization rules

### `stock_quant/app`
Application services and orchestrators.

Typical responsibilities:
- coordinating repositories and providers
- wrapping pipelines into user-facing workflows
- composing subprocess-based daily operations

### `stock_quant/infrastructure`
Persistence, schema management, providers, and repositories.

Typical responsibilities:
- DuckDB schema managers
- DuckDB repositories
- external data providers
- config/session factories

### `stock_quant/pipelines`
Structured execution units around a specific data product.

### `cli`
Concrete user/operator entrypoints.
Current sub-groups that matter most:
- `cli/core`
- `cli/ops`
- `cli/raw`
- `cli/tools`

## Current runtime architecture by domain

### Prices
Main entrypoints:
- `cli/core/build_prices.py`
- `cli/core/build_prices_research.py`
- `cli/raw/load_price_source_daily_raw_all_from_stooq_dir.py`
- `cli/ops/run_price_daily_refresh.py`

Main tables:
- raw: `price_source_daily_raw_all`, other price raw tables
- canonical: `price_history`
- serving: `price_latest`

### SEC / fundamentals
Main entrypoints:
- `cli/core/build_sec_filings.py`
- `cli/core/build_fundamentals.py`
- `cli/ops/run_sec_fundamentals_daily.py`

### FINRA / short data
The repository contains staging plus canonical history and derived short features.
Research currently depends most on:
- normalized short history
- `short_features_daily`

### Research
Main entrypoints:
- `cli/core/build_research_training_dataset.py`
- `cli/core/build_research_labels.py`
- `cli/core/build_research_backtest.py`
- `cli/core/run_research_experiment.py`
- `cli/ops/run_research_daily.py`

Main research tables:
- `research_training_dataset`
- `research_labels`
- `research_backtest`

Supporting manifests:
- split manifest(s)
- dataset manifest(s)
- experiment manifest(s)

## Data-flow summary

### Historical price rebuild path

```text
Stooq txt files
-> raw loader (chunked)
-> price_source_daily_raw_all
-> canonical price builders
-> price_history
-> price_latest (serving only)
```

### Research experiment path

```text
snapshot_id + split_id
-> research_training_dataset
-> research_labels
-> research_backtest
-> research experiment manifest
```

## Design choices visible in the current code

### SQL-first
Heavy joins and transforms are done in DuckDB SQL.

### Thin Python orchestration
Recent code changes reinforce this:
- child-process streaming in the experiment runner
- chunk discovery / progress reporting in Python
- heavy dataset work left to SQL temp tables

### Chunking for stability
Two important chunked workflows now exist:
- Stooq historical directory loading
- research training dataset monthly partition builds

### Runtime parameter alignment
The research runner now propagates:
- `--memory-limit`
- `--threads`
- `--temp-dir`
- `--verbose`

across its child steps.

## What is obsolete in older architecture descriptions

Older docs in the repo over-emphasized:
- news pipeline examples
- generic “future” backtest plans
- older `training_dataset_daily` naming

The current code is more concrete and should be documented around the actual research tables and CLI surfaces listed above.
