# stock-quant-oop

A modular, SQL-first quantitative research framework built on DuckDB.

## Overview

This project implements a **point-in-time safe research pipeline** for quantitative signals.

It is designed to be:

- modular
- reproducible
- scalable
- SQL-first

## Architecture

See:

- `docs/architecture_diagram.png`
- `docs/pipeline_sequence_diagram.png`
- `docs/MODULAR_FEATURE_ARCHITECTURE.md`
- `docs/QUICKSTART.md`

## Core Flow

```text
price_history / short_features_daily
        ↓
feature_*_daily
        ↓
research_features_daily
        ↓
research_training_dataset
        ↓
research_labels
        ↓
research_backtest
```

## Modular Features

Each indicator family has its own table:

- `feature_price_momentum_daily`
- `feature_price_trend_daily`
- `feature_price_volatility_daily`
- `feature_short_daily`

## Research Artifacts

- `research_features_daily`
- `research_training_dataset`
- `research_labels`
- `research_backtest`

## Signals

Signals declare:

- required features
- parameters
- warmup
- SQL-first execution logic

Current reference signal:

- `rsi_threshold`

## Quickstart

See the full guide in `docs/QUICKSTART.md`.

Minimal flow:

```bash
python3 cli/core/build_feature_price_momentum.py --db-path market.duckdb
python3 cli/core/build_feature_price_trend.py --db-path market.duckdb
python3 cli/core/build_feature_price_volatility.py --db-path market.duckdb
python3 cli/core/build_feature_short.py --db-path market.duckdb
python3 cli/core/build_research_features_daily.py --db-path market.duckdb
python3 cli/core/build_research_training_dataset.py --db-path market.duckdb --snapshot-id <SNAPSHOT_ID> --split-id <SPLIT_ID>
python3 cli/core/build_research_labels.py --db-path market.duckdb --snapshot-id <SNAPSHOT_ID> --dataset-id <DATASET_ID> --split-id <SPLIT_ID>
python3 cli/core/build_research_backtest.py --db-path market.duckdb --dataset-id <DATASET_ID> --split-id <SPLIT_ID> --signal-name rsi_threshold --signal-params-json '{"feature_name":"rsi_14","oversold_threshold":30.0,"overbought_threshold":70.0}'
```

## Status

- RSI14 signal works end-to-end
- modular feature system implemented
- research feature composition implemented
- incremental dataset build implemented

## Philosophy

- Simple over complex
- Explicit over implicit
- SQL over Python loops
- Modular over monolithic
- Reproducible over ad hoc
