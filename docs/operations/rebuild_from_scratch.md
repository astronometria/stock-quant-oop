# Rebuild From Scratch

## Goal
Recreate the canonical DuckDB database from source pipelines.

## High-level order
1. init schema foundations
2. symbol reference
3. market universe
4. SEC filings
5. SEC facts and fundamentals
6. price history
7. daily prices
8. FINRA short interest
9. FINRA daily short volume
10. short features daily

## Short-data rebuild
Run in this order:
1. python3 cli/raw/load_finra_daily_short_volume_raw.py --db-path ~/stock-quant-oop/market.duckdb --source ~/stock-quant-oop/data/raw/finra/daily_short_sale_volume
2. python3 cli/core/build_finra_daily_short_volume.py --db-path ~/stock-quant-oop/market.duckdb
3. python3 cli/core/build_finra_short_interest.py --db-path ~/stock-quant-oop/market.duckdb
4. python3 cli/core/build_short_features.py --db-path ~/stock-quant-oop/market.duckdb
