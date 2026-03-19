# stock-quant-oop

Quant research pipeline centered on DuckDB, SQL-first transforms, and point-in-time-safe research artifacts.

## Current state

The repository currently supports four practical layers:

1. **Raw ingestion and staging**
   - Stooq directory backfills into `price_source_daily_raw_all`
   - Yahoo daily refresh into raw/normalized price flows
   - SEC raw filing index ingestion
   - FINRA short-interest and daily short-volume staging

2. **Canonical history**
   - `price_history`
   - `price_latest` (serving only)
   - SEC normalized filing tables
   - FINRA normalized history tables
   - `short_features_daily`

3. **Research artifacts**
   - `research_training_dataset`
   - `research_labels`
   - `research_backtest`
   - experiment manifests and split manifests

4. **Operational wrappers**
   - `cli/ops/run_price_daily_refresh.py`
   - `cli/ops/run_sec_fundamentals_daily.py`
   - `cli/ops/run_research_daily.py`
   - `cli/core/run_research_experiment.py`

## Core principles

- **SQL-first:** business-heavy transforms stay in DuckDB SQL.
- **Thin Python:** Python handles orchestration, IO, validation, progress bars, and logging.
- **Point-in-time safe:** research uses canonical history, not serving tables.
- **Reproducible research:** dataset ids, split ids, and experiment ids are explicit.
- **Memory-safe large jobs:** heavy loaders and research builders use chunking plus DuckDB temp spilling.

## Recommended command paths

### Initialize the database

```bash
python3 cli/core/init_market_db.py --db-path /path/to/market.duckdb
```

### Price pipeline

Daily refresh from Yahoo:

```bash
python3 cli/core/build_prices.py --db-path /path/to/market.duckdb --mode daily --verbose
```

Historical Stooq directory load into `price_source_daily_raw_all`:

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

Canonical / research-grade price refresh:

```bash
python3 cli/core/build_prices_research.py --db-path /path/to/market.duckdb --verbose
```

### SEC / fundamentals

```bash
python3 cli/core/build_sec_filings.py --db-path /path/to/market.duckdb --verbose
python3 cli/core/build_fundamentals.py --db-path /path/to/market.duckdb --verbose
```

### Research experiment

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

## Tables that matter most

### Prices
- `price_source_daily_raw_all`
- `price_history`
- `price_latest`

### Short data
- `daily_short_volume_history`
- `finra_short_interest_history`
- `short_features_daily`

### Research
- `research_training_dataset`
- `research_labels`
- `research_backtest`

## Important caveats

- `price_latest` is **serving only** and should not drive labels, features, datasets, or backtests.
- The current research backtest is still a **single-rule long-only threshold strategy** on `short_volume_ratio`.
- Label generation now supports **dataset-scoped sanitization** with `--max-abs-return`.
- Recent code added a **common-only** dataset build mode to exclude likely warrants, rights, units, and special/test symbols earlier in the pipeline.

## Where to read next

- `README_DEV.md`
- `docs/ARCHITECTURE.md`
- `docs/PIPELINE_USAGE.md`
- `docs/PRICE_PIPELINE.md`
- `docs/RESEARCH_PIPELINE_IMPLEMENTATION.md`
