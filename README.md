# stock-quant-oop

Quant research pipeline (OOP) with strict point-in-time guarantees.

## Core Principles

- `price_history` = canonical price table
- `price_latest` = serving only, never used for research
- all research joins are point-in-time with `available_at`
- no fallback from `available_at` to `period_end_date`
- datasets are reproducible and versionable

## Data Layers

### Raw
- SEC raw filings and XBRL facts
- symbol raw sources
- FINRA raw short-interest sources
- price raw sources (historical + daily)

### Normalized
- `symbol_reference`
- `sec_filing`
- `sec_fact_normalized`
- `price_history`
- `finra_short_interest_history`

### Derived
- `market_universe`
- `fundamental_features_daily`
- `short_features_daily`
- `training_dataset_daily`
- `dataset_versions`

## Rebuild

Canonical rebuild entry point:

    python3 cli/ops/rebuild_database_from_scratch.py --db-path ~/stock-quant-oop/market.duckdb

Rebuild includes:
- symbol staging
- market universe
- symbol reference
- SEC filings
- SEC fact normalization
- fundamentals
- historical price backfill
- daily price refresh
- FINRA short interest

## Research Guarantees

- fundamentals become visible only after `available_at`
- short interest becomes visible only after `available_at`
- research never reads `price_latest`
- datasets must be point-in-time safe and deduplicated per `(symbol, price_date)`

## Repo Structure

- `stock_quant/` : application package
- `cli/core/` : main builders
- `cli/ops/` : orchestration
- `cli/raw/` : raw ingestion / staging
- `docs/` : architecture and pipeline docs
- `tests/` : unit and integration coverage

