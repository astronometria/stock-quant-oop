# stock-quant-oop

Quant research pipeline (OOP) with strict point-in-time guarantees.

## Core Principles

- price_history = canonical
- price_latest = serving only (never used in research)
- all joins are point-in-time using available_at
- no fallback to period_end_date
- dataset is reproducible and versioned

## Data Layers

### Raw
- SEC filings
- NASDAQ / symbol sources
- FINRA short interest
- price raw (stooq / yahoo)

### Normalized
- sec_filing
- sec_fact_normalized
- finra_short_interest_history
- price_history

### Derived
- fundamental_features_daily
- short_features_daily
- training_dataset_daily

## Pipelines

Core rebuild:

    python3 cli/ops/rebuild_database_from_scratch.py --db-path ~/stock-quant-oop/market.duckdb

Includes:
- symbol sources
- market universe
- SEC filings
- SEC normalized facts
- fundamentals
- price backfill (stooq)
- price daily (yahoo)
- FINRA short interest

## Research Dataset

training_dataset_daily is:

- point-in-time correct
- no future leakage
- deduplicated per (symbol, price_date)

## Guarantees

- fundamentals visible only after available_at
- short interest visible only after available_at
- no use of price_latest in research
- no implicit look-ahead bias

## Structure

- stock_quant/ → core package
- cli/core/ → main entry points
- cli/ops/ → orchestration
- cli/raw/ → raw ingestion
- tests/ → unit + integration tests
