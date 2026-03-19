# build_finra_daily_short_volume

## Entry point

`python3 cli/core/build_finra_daily_short_volume.py --db-path /path/to/market.duckdb`

## Purpose

Transform staged FINRA daily short-volume data into canonical historical short-volume rows.

## Tables involved

### Raw / staging
- `finra_daily_short_volume_source_raw`
- `finra_daily_short_volume_sources`

### Canonical
- `daily_short_volume_history`

## Upstream raw loader

Before this pipeline, raw files are loaded with:
`python3 cli/raw/load_finra_daily_short_volume_raw.py --db-path /path/to/market.duckdb --source /path/to/daily_short_sale_volume`

## Current behavior

The canonical builder:
1. inspects raw and canonical state
2. builds a temporary canonical source
3. upserts canonical history
4. returns final build metrics

## Current orchestrator behavior

The daily orchestrator now skips this pipeline when:
- raw max trade date
- canonical max trade date

are already aligned.

That removed an unnecessary heavy rebuild from repeated daily runs.

## Research rule

Use `daily_short_volume_history` as the canonical short-volume source.
