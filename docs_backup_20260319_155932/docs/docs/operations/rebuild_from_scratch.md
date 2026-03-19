# Rebuild From Scratch

## Goal

Rebuild the canonical database from source loaders and canonical builders in a clean, deterministic order.

## Recommended order

1. initialize schema foundations
2. initialize symbol normalization
3. build prices
4. build SEC filings
5. build FINRA short interest
6. load FINRA daily short volume raw files
7. build FINRA daily short volume canonical history
8. build short features

## Minimal short-data rebuild

### 1. initialize short-data tables
`python3 cli/core/init_short_data_foundation.py --db-path /home/marty/stock-quant-oop/market.duckdb`

### 2. load FINRA daily short volume raw files
`python3 cli/raw/load_finra_daily_short_volume_raw.py --db-path /home/marty/stock-quant-oop/market.duckdb --source /home/marty/stock-quant-oop/data/raw/finra/daily_short_sale_volume`

### 3. build canonical daily short volume history
`python3 cli/core/build_finra_daily_short_volume.py --db-path /home/marty/stock-quant-oop/market.duckdb`

### 4. build canonical FINRA short interest
`python3 cli/core/build_finra_short_interest.py --db-path /home/marty/stock-quant-oop/market.duckdb`

### 5. build derived short features
`python3 cli/core/build_short_features.py --db-path /home/marty/stock-quant-oop/market.duckdb --duckdb-threads 2 --duckdb-memory-limit 36GB`

## Daily orchestrator

For day-to-day refreshes use:
`python3 cli/run_daily_pipeline.py`

Current behavior:
- incremental skip for FINRA daily short volume
- noop behavior for FINRA short interest when aligned
- incremental skip for short features
- prices and SEC still run each pass

## Validation checks after rebuild

Recommended checks:
- max price date
- max SEC filing date
- max short interest settlement date
- max daily short volume trade date
- max short feature as_of_date

Also verify:
- `short_features_daily` row count is non-zero
- `short_features_daily.short_interest` coverage is non-zero
- no null `symbol`
- no null `as_of_date`

## Important operational notes

- do not run multiple DuckDB writer jobs against the same DB at once
- for short feature rebuilds, use the proven memory/thread settings above
- keep logs under `logs/` for later audit and troubleshooting
