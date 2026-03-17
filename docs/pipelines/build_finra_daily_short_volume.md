# build_finra_daily_short_volume

## Purpose
Build canonical FINRA daily short volume history from raw source staging.

## Input tables
- finra_daily_short_volume_source_raw

## Output tables
- daily_short_volume_history

## Grain
One canonical row per symbol, trade_date and source_file after deterministic source dedupe.

## PIT controls
- store publication_date
- store available_at
- do not infer research visibility from trade_date

## Bias controls
- do not filter history by current market universe
- retain full source history
- keep source metadata for replay and audits
