# build_finra_short_interest

## Purpose
Build canonical FINRA short interest history and latest snapshots.

## Input tables
- finra_short_interest_source_raw

## Output tables
- finra_short_interest_history
- finra_short_interest_latest

## Bias controls
- preserve history even for delisted symbols
- keep source metadata
- use available_at for research visibility
