# stock-quant-oop

Quant research pipeline (OOP + SQL-first) with strict point-in-time guarantees.

## Core principles
- `price_history` is the canonical price table.
- `price_latest` is serving-only and never used for research.
- all research joins must respect `available_at`.
- no fallback from `available_at` to business dates.
- historical tables must avoid survivor bias.
- canonical pipelines must be idempotent and rebuildable.

## Current state
Implemented and stable:
- daily Yahoo prices with provider symbol compatibility and batching
- SEC filing normalization
- FINRA short interest canonical history and latest

Restored / in progress:
- FINRA daily short volume raw + canonical history
- short_features_daily derived from short volume + short interest

## Data layers

### Raw
- SEC raw inputs
- symbol raw sources
- FINRA short interest raw sources
- FINRA daily short volume raw sources
- price raw sources

### Normalized
- symbol_reference
- sec_filing
- sec_fact_normalized
- price_history
- finra_short_interest_history
- daily_short_volume_history

### Derived
- market_universe
- fundamental_features_daily
- short_features_daily
- training_dataset_daily
- dataset_versions

## Canonical pipeline docs
See:
- docs/pipelines/build_prices.md
- docs/pipelines/build_sec_filings.md
- docs/pipelines/build_finra_short_interest.md
- docs/pipelines/build_finra_daily_short_volume.md
- docs/pipelines/build_short_features.md
- docs/pipelines/run_daily_pipeline.md

## Rebuild from scratch
See:
- docs/operations/rebuild_from_scratch.md

## Repo walkthrough
See:
- docs/walkthrough/repo_walkthrough.md
