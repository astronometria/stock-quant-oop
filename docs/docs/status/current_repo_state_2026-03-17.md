# Current Repo State — 2026-03-18

## Summary

The repository now has a working short-data stack end-to-end.

Implemented and functioning:
- daily prices
- SEC filing normalization
- FINRA short interest canonical history + latest
- FINRA daily short volume raw ingestion + canonical history
- short feature derivation from short volume + short interest
- daily orchestrator with partial incremental skip logic

## Stable pipeline areas

### Prices
- `price_history` is the canonical research table
- `price_latest` is serving-only
- daily price refresh is working

### SEC
- `sec_filing` is building correctly
- current daily orchestrator still runs SEC each pass because runtime is already short

### FINRA short interest
- raw stage and canonical history are working
- latest serving projection is working
- pipeline cleanly returns noop when already up to date

### FINRA daily short volume
- raw loader is working against a large local historical archive
- `finra_daily_short_volume_source_raw` is populated
- `finra_daily_short_volume_sources` is populated
- `daily_short_volume_history` is populated
- daily orchestrator now skips canonical rebuild when raw/history are already aligned

### Short features
- `short_features_daily` is building successfully
- monthly batching was introduced to avoid DuckDB out-of-memory failures
- daily orchestrator now skips the rebuild when source signature is unchanged

## Current operational truth

### What is incremental today
- FINRA daily short volume canonical build
- short feature build

### What is effectively self-skip / noop today
- FINRA short interest

### What still runs every daily pass
- prices
- SEC filings

## Research integrity status

### Good
- canonical vs serving separation is explicit
- PIT-safe short feature join logic exists
- historical short-data storage is in place
- latest tables are not the canonical research source

### Must remain enforced
- no fallback from availability timestamps to business dates
- no filtering historical rows only to the current surviving universe
- no use of serving tables in research backtests

## Practical outcome

The repository is now beyond “scaffold” stage. It supports real daily refreshes plus a research-safe short-data domain.
