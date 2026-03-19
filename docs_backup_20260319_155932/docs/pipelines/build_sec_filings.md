# build_sec_filings

## Entry point

`python3 cli/core/build_sec_filings.py --db-path /path/to/market.duckdb`

## Purpose

Normalize staged SEC filing inputs into the canonical `sec_filing` table.

## Output table

- `sec_filing`

## Current status

This pipeline is working and writes the expected SEC filing rows.

## Current orchestrator behavior

The daily orchestrator still runs this pipeline every pass because:
- runtime is relatively short
- the current priority was removing waste from the heavier short-data rebuilds first

## Research usage

`sec_filing` is the normalized historical filing table intended for research and downstream feature work.
