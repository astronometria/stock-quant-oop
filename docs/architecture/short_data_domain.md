# Short Data Domain

## Scope
The short-data domain is composed of:
- FINRA short interest
- FINRA daily short volume
- short features derived for research

## Design goals
- SQL-first canonical transformations
- strict point-in-time visibility
- no survivor-bias filtering in historical tables
- deterministic idempotent reruns
