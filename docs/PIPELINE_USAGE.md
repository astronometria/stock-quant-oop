# Pipeline Usage

This document reflects the current CLI surfaces that are visible in the repository.

## 1. Initialize the database

```bash
python3 cli/core/init_market_db.py --db-path /path/to/market.duckdb
```

Drop and recreate:

```bash
python3 cli/core/init_market_db.py --db-path /path/to/market.duckdb --drop-existing
```

## 2. Market universe and symbols

```bash
python3 cli/core/build_market_universe.py --db-path /path/to/market.duckdb --verbose
python3 cli/core/build_symbol_reference.py --db-path /path/to/market.duckdb --verbose
```

## 3. Prices

### Daily refresh from Yahoo

```bash
python3 cli/core/build_prices.py \
  --db-path /path/to/market.duckdb \
  --mode daily \
  --verbose
```

Single-date refresh:

```bash
python3 cli/core/build_prices.py \
  --db-path /path/to/market.duckdb \
  --mode daily \
  --as-of 2026-03-19 \
  --verbose
```

### Historical backfill from local source(s)

```bash
python3 cli/core/build_prices.py \
  --db-path /path/to/market.duckdb \
  --mode backfill \
  --historical-source /path/to/source.csv \
  --verbose
```

### Chunked Stooq directory load

```bash
python3 cli/raw/load_price_source_daily_raw_all_from_stooq_dir.py \
  --db-path /path/to/market.duckdb \
  --root-dir /path/to/data/raw/stooq/daily/us \
  --truncate \
  --memory-limit 24GB \
  --threads 8 \
  --temp-dir /path/to/tmp \
  --verbose
```

### Research-grade price tables

```bash
python3 cli/core/build_prices_research.py --db-path /path/to/market.duckdb --verbose
```

### Daily wrapper

```bash
python3 cli/ops/run_price_daily_refresh.py --db-path /path/to/market.duckdb --verbose
```

## 4. SEC and fundamentals

### SEC filings

```bash
python3 cli/core/build_sec_filings.py --db-path /path/to/market.duckdb --verbose
```

### Fundamentals

```bash
python3 cli/core/build_fundamentals.py --db-path /path/to/market.duckdb --verbose
```

### Daily SEC/fundamentals wrapper

```bash
python3 cli/ops/run_sec_fundamentals_daily.py \
  --db-path /path/to/market.duckdb \
  --verbose
```

If loading raw SEC filing index CSVs first:

```bash
python3 cli/ops/run_sec_fundamentals_daily.py \
  --db-path /path/to/market.duckdb \
  --sec-source /path/to/index.csv \
  --verbose
```

## 5. Research

### Build dataset

```bash
python3 cli/core/build_research_training_dataset.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --split-id <split_id> \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

### Build labels

```bash
python3 cli/core/build_research_labels.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --dataset-id <dataset_id> \
  --split-id <split_id> \
  --max-abs-return 1.0 \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

### Build backtest

```bash
python3 cli/core/build_research_backtest.py \
  --db-path /path/to/market.duckdb \
  --dataset-id <dataset_id> \
  --split-id <split_id> \
  --transaction-cost-bps 10 \
  --signal-threshold 0.5 \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

### Run full experiment

```bash
python3 cli/core/run_research_experiment.py \
  --db-path /path/to/market.duckdb \
  --snapshot-id <snapshot_id> \
  --split-id <split_id> \
  --experiment-name research_scientific_v3 \
  --transaction-cost-bps 10 \
  --signal-threshold 0.5 \
  --memory-limit 24GB \
  --threads 6 \
  --temp-dir /path/to/tmp \
  --verbose
```

### Daily research wrapper

```bash
python3 cli/ops/run_research_daily.py --db-path /path/to/market.duckdb --verbose
```

## 6. Important notes

- `price_history` is the canonical price table for research.
- `price_latest` is serving-only.
- The research runner now streams child process output live instead of hiding progress until the end.
- The label builder can sanitize extreme forward returns with `--max-abs-return`.
- The current backtest signal is still threshold-based on `short_volume_ratio`.
