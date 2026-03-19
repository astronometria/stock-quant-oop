# build_prices

## Entry point

`python3 cli/core/build_prices.py --db-path /path/to/market.duckdb`

## Purpose

Refresh canonical price history and latest-price serving output.

## Output tables

### Canonical
- `price_history`

### Serving
- `price_latest`

## Current behavior

The price pipeline supports daily incremental refresh. In practice it:
- determines symbol scope
- filters unsupported provider symbols
- fetches daily bars
- writes canonical rows into `price_history`
- refreshes `price_latest`

## Research rule

Use `price_history` for backtests and historical research.

Do not use `price_latest` for historical research logic.

## Current orchestrator status

`cli/run_daily_pipeline.py` still runs `build_prices` each pass. That is acceptable for now, but it is separate from the heavier short-data incremental work.

## Notes

- provider exclusions exist for symbols not supported cleanly by the default yfinance path
- some symbols can fail or rate-limit, so provider failure handling matters
