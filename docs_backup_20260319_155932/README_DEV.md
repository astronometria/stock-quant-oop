# README_DEV

Developer-oriented documentation for `stock-quant-oop`.

## Purpose

This repository is a quantitative research pipeline built around an OOP codebase with SQL-first canonical data transforms. The design goal is to keep historical research correct, rebuildable, and maintainable while still supporting practical day-to-day refreshes.

## Design rules

### Canonical over serving
Research and backtests should use canonical historical tables first:
- `price_history`
- `sec_filing`
- `finra_short_interest_history`
- `daily_short_volume_history`
- `short_features_daily`

Serving tables are convenience outputs:
- `price_latest`
- `finra_short_interest_latest`

### Point-in-time safety
Feature generation and joins must use the time a record became available, not just the business date printed by the source. In the short-data domain that means:
- daily short volume uses `available_at`
- short interest uses `ingested_at`

### Survivor-bias resistance
Historical rows must remain available even if a symbol later delists, renames, merges, or falls out of the current universe.

### SQL-first implementation
Business-heavy transforms belong in DuckDB SQL:
- canonical rebuilds
- deduplication
- date alignment
- historical feature derivation
- skip / alignment probes where practical

Python should stay thin around:
- CLI entrypoints
- orchestration
- provider calls
- filesystem discovery
- logging and progress display

## Current implemented pipelines

### Prices
Entry point:
`python3 cli/core/build_prices.py --db-path /path/to/market.duckdb`

Behavior:
- daily incremental refresh
- canonical write into `price_history`
- latest refresh into `price_latest`

### SEC filings
Entry point:
`python3 cli/core/build_sec_filings.py --db-path /path/to/market.duckdb`

Behavior:
- normalizes SEC filing inputs into `sec_filing`
- currently still runs every orchestrated pass because runtime is acceptable

### FINRA short interest
Entry point:
`python3 cli/core/build_finra_short_interest.py --db-path /path/to/market.duckdb`

Behavior:
- builds canonical history from raw staged data
- refreshes serving latest table
- cleanly noops when already aligned

### FINRA daily short volume raw loader
Entry point:
`python3 cli/raw/load_finra_daily_short_volume_raw.py --db-path /path/to/market.duckdb --source /path/to/daily_short_sale_volume`

Behavior:
- reads FINRA historical files with multiple layout variants
- stages raw rows into `finra_daily_short_volume_source_raw`
- writes per-source inventory into `finra_daily_short_volume_sources`

### FINRA daily short volume canonical builder
Entry point:
`python3 cli/core/build_finra_daily_short_volume.py --db-path /path/to/market.duckdb`

Behavior:
- canonicalizes staged short-volume rows
- maintains `daily_short_volume_history`

### Short features
Entry point:
`python3 cli/core/build_short_features.py --db-path /path/to/market.duckdb --duckdb-threads 2 --duckdb-memory-limit 36GB`

Behavior:
- derives `short_features_daily`
- uses monthly batching to avoid DuckDB OOM
- joins short interest history onto daily short volume with PIT-safe logic

## Daily orchestrator

Entry point:
`python3 cli/run_daily_pipeline.py`

Current order:
1. prices
2. FINRA daily short volume
3. FINRA short interest
4. short features
5. SEC filings

Current incremental behavior:
- prices still run each pass
- FINRA daily short volume is skipped when raw and canonical max dates are already aligned
- FINRA short interest noops when aligned
- short features are skipped when the source signature is unchanged
- SEC filings still run each pass by design for now

## Known-good operational settings

### Heavy short feature build
Use:
- `--duckdb-threads 2`
- `--duckdb-memory-limit 36GB`

This configuration successfully rebuilt roughly 33M+ short feature rows without exhausting memory.

### Logging
Write logs under `logs/`.

### Locking
Do not keep a DuckDB writer connection open while launching another writer process against the same database.

## Rebuild guidance

Recommended order:
1. schema foundations
2. symbol normalization
3. prices
4. SEC filings
5. FINRA short interest
6. FINRA daily short volume
7. short features

## What still matters most

The repository is now in a good state for:
- downstream research joins
- short-interest / short-volume feature experimentation
- signal generation
- backtesting

The next layer of work is mostly around:
- further incremental improvements
- richer derived features
- downstream research datasets
- portfolio / strategy workflows
