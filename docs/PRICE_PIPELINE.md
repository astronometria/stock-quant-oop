# Price Pipeline

## Goal

Build and maintain price history that is usable for research and backtesting without relying on serving-only snapshots.

## Current layers

### Raw
Representative raw tables include:
- `price_source_daily_raw_all`
- `price_source_daily_raw_yahoo`
- other raw price staging tables maintained by the repository

Raw tables are:
- auditable
- staging-oriented
- not the final research interface

### Canonical
Canonical research-facing table:
- `price_history`

This is the table that research builders should prefer.

### Serving
Serving-only convenience table:
- `price_latest`

`price_latest` is useful for:
- latest-state inspection
- operational sanity checks
- quick UI/debug queries

It should not be the source for:
- feature generation
- label generation
- research datasets
- backtests

## Current entrypoints

### Build prices

```bash
python3 cli/core/build_prices.py --db-path /path/to/market.duckdb --mode daily --verbose
```

Modes supported by current code:
- `daily`
- `backfill`

### Build research-grade price tables

```bash
python3 cli/core/build_prices_research.py --db-path /path/to/market.duckdb --verbose
```

### Load full Stooq directory

```bash
python3 cli/raw/load_price_source_daily_raw_all_from_stooq_dir.py \
  --db-path /path/to/market.duckdb \
  --root-dir /path/to/data/raw/stooq/daily/us \
  --truncate \
  --memory-limit 24GB \
  --threads 8 \
  --temp-dir /path/to/tmp \
  --verbose
```

## Provider model visible in current code

### Daily path
- Yahoo Finance through `YfinancePriceProvider`

### Historical path
- local historical sources through `HistoricalPriceProvider`
- extracted Stooq directories can be loaded in large chunked SQL-first batches

## Current operational pattern

A practical rebuild sequence is:

1. load historical raw prices from Stooq/local sources
2. rebuild or refresh canonical prices
3. run daily Yahoo refresh for the latest window
4. rebuild `price_latest`
5. only then run research builders

## Why the chunked Stooq loader matters

The repository now contains a chunked loader for extracted Stooq trees because the one-big-glob approach was prone to:
- long silent waits
- high memory pressure
- poor operator visibility

The chunked version keeps:
- SQL-first CSV parsing inside DuckDB
- thin Python orchestration
- per-chunk progress and table stats

## Bias control

Research should consume `price_history`, not raw tables and not `price_latest`.

That keeps:
- point-in-time discipline
- cleaner rebuild semantics
- easier reproducibility across datasets and experiments
