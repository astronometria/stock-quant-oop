# Current Repo State - 2026-03-17

## Stable
- Yahoo daily prices canonicalized into price_history
- provider symbol compatibility layer for Yahoo Finance
- batching and anti-rate-limit strategy for Yahoo
- SEC filing build pipeline
- FINRA short interest raw/history/latest pipeline

## Restored but still evolving
- FINRA daily short volume raw ingestion
- canonical daily_short_volume_history
- derived short_features_daily

## Gaps being closed
- short_data_schema.py must exist in active branch
- finra_daily_short_volume_source_raw must be created by schema foundation
- short_features_daily must become a first-class documented pipeline target

## Bias controls required
- no filtering historical short-data by current universe
- preserve available_at
- research joins remain point-in-time safe
- latest tables stay serving-only
