# run_daily_pipeline

## Entry point

`python3 cli/run_daily_pipeline.py`

## Purpose

Run the daily project refresh in a practical order while avoiding unnecessary rebuilds where the cost is high.

## Current order

1. `build_prices`
2. `build_finra_daily_short_volume` (probed first)
3. `build_finra_short_interest`
4. `build_short_features` (probed first)
5. `build_sec_filings`

## Current skip / noop behavior

### Prices
Still run each pass.

### FINRA daily short volume
Skipped when raw and canonical max dates are already aligned.

### FINRA short interest
Pipeline itself returns noop when already aligned.

### Short features
Skipped when the saved source signature is unchanged.

### SEC filings
Still run each pass by choice because runtime is short.

## Why this matters

The short-feature build is one of the most expensive jobs in the repo. Skipping it when inputs are unchanged drastically reduces repeated daily runtime.

## Example schedule

A typical cron setup for 17:00 local time:

`0 17 * * * cd /home/marty/stock-quant-oop && /usr/bin/python3 cli/run_daily_pipeline.py >> logs/cron.log 2>&1`

## Operational note

Do not run multiple writer jobs against the same DuckDB file at the same time.
