# build_short_features

## Entry point

`python3 cli/core/build_short_features.py --db-path /path/to/market.duckdb --duckdb-threads 2 --duckdb-memory-limit 36GB`

## Purpose

Build `short_features_daily` from canonical short-volume and short-interest history.

## Inputs

- `daily_short_volume_history`
- `finra_short_interest_history`
- active normalization rules in `symbol_normalization`

## Output

- `short_features_daily`

## Why this pipeline is heavy

This pipeline works over:
- tens of millions of canonical short-volume rows
- millions of short-interest rows
- rolling calculations and PIT-safe as-of joins

A naive rebuild can exceed memory limits.

## Current implementation status

The pipeline now uses monthly batching to control memory usage and has been successfully rebuilt end-to-end under proven DuckDB settings.

## Point-in-time rule

For any feature row:
- short-volume information comes from the canonical daily short-volume row
- short-interest data must only be attached if it was already available by the feature row’s availability cutoff

## Current orchestrator behavior

The daily orchestrator now saves and compares a source signature based on:
- daily short volume history count and max trade date
- short interest history count and max settlement date
- active symbol normalization rule count

If unchanged, the heavy rebuild is skipped.

## Research role

`short_features_daily` is the main derived short-data research table.
