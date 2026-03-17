# build_prices

## Purpose
Canonical Yahoo daily price refresh into price_history.

## Output tables
- price_history
- price_latest serving-only

## Guarantees
- canonical symbol stays internal
- provider symbol stays provider-specific
- batching reduces rate-limit failures
- research never reads price_latest
