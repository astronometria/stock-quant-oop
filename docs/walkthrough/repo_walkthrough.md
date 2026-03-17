# Repo Walkthrough

## Structure
- cli/raw/: source ingestion and staging
- cli/core/: canonical build entry points
- cli/ops/: orchestration
- stock_quant/infrastructure/db/: schema foundation
- stock_quant/pipelines/: SQL-first canonical pipeline logic
- docs/: architecture, operations and pipeline documentation

## Data flow
1. raw source files land in data/raw/...
2. raw loaders stage source fidelity tables
3. canonical builders normalize history tables
4. derived builders create research features
5. research joins must enforce point-in-time rules

## Short-data domain
- FINRA short interest:
  - finra_short_interest_source_raw
  - finra_short_interest_history
  - finra_short_interest_latest
- FINRA daily short volume:
  - finra_daily_short_volume_source_raw
  - daily_short_volume_history
  - short_features_daily
