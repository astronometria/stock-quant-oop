# build_short_features

## Purpose
Build research-facing short features from canonical short volume and short interest history.

## Input tables
- daily_short_volume_history
- finra_short_interest_history

## Output tables
- short_features_daily

## PIT controls
- feature visibility must respect max_source_available_at
- do not join using future short interest
- no serving-only shortcuts
