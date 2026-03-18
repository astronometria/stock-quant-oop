# build_finra_short_interest

## Entry point

`python3 cli/core/build_finra_short_interest.py --db-path /path/to/market.duckdb`

## Purpose

Build canonical FINRA short-interest history from staged raw data and refresh the latest serving table.

## Tables involved

### Inputs
- `finra_short_interest_source_raw`
- `finra_short_interest_sources`

### Outputs
- `finra_short_interest_history`
- `finra_short_interest_latest`

## Current behavior

The pipeline:
1. inspects current build state
2. decides whether a rebuild is needed
3. builds canonical history when necessary
4. refreshes the latest serving table
5. returns noop when already aligned

## Research rule

Use `finra_short_interest_history` for historical research.

Do not use `finra_short_interest_latest` for backtests.

## Current status

Stable and functioning.
