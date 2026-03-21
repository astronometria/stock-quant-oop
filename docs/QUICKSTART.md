# Quickstart

## Goal

Run the modular research pipeline end-to-end with the current architecture.

## Prerequisites

- a populated DuckDB database
- `price_history` available
- `short_features_daily` available
- a valid `snapshot_id`
- a valid `split_id`

## 1. Build modular features

```bash
python3 cli/core/build_feature_price_momentum.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose

python3 cli/core/build_feature_price_trend.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose

python3 cli/core/build_feature_price_volatility.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose

python3 cli/core/build_feature_short.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose
```

## 2. Compose research features

```bash
python3 cli/core/build_research_features_daily.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose
```

## 3. Build dataset

```bash
python3 cli/core/build_research_training_dataset.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --snapshot-id <SNAPSHOT_ID>   --split-id <SPLIT_ID>   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose
```

## 4. Find newest dataset_id

```bash
python3 <<'PY'
import duckdb

con = duckdb.connect("/home/marty/stock-quant-oop/market.duckdb", read_only=True)
try:
    row = con.execute("""
        SELECT dataset_id
        FROM research_training_dataset
        GROUP BY dataset_id
        ORDER BY MAX(created_at) DESC NULLS LAST, dataset_id DESC
        LIMIT 1
    """).fetchone()
    print(row[0] if row else None)
finally:
    con.close()
PY
```

## 5. Build labels

```bash
python3 cli/core/build_research_labels.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --snapshot-id <SNAPSHOT_ID>   --dataset-id <DATASET_ID>   --split-id <SPLIT_ID>   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose
```

## 6. Run RSI backtest

```bash
python3 cli/core/build_research_backtest.py   --db-path /home/marty/stock-quant-oop/market.duckdb   --dataset-id <DATASET_ID>   --split-id <SPLIT_ID>   --signal-name rsi_threshold   --signal-params-json '{"feature_name":"rsi_14","oversold_threshold":30.0,"overbought_threshold":70.0}'   --execution-lag-bars 1   --transaction-cost-bps 10   --memory-limit 24GB   --threads 4   --temp-dir /home/marty/stock-quant-oop/tmp   --verbose
```

## 7. Validation probes

### Dataset feature coverage

```bash
python3 <<'PY'
import duckdb

dataset_id = "<DATASET_ID>"

con = duckdb.connect("/home/marty/stock-quant-oop/market.duckdb", read_only=True)
try:
    print(con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(close) AS close_rows,
            COUNT(rsi_14) AS rsi_14_rows,
            COUNT(returns_1d) AS returns_1d_rows,
            COUNT(sma_20) AS sma_20_rows,
            COUNT(atr_14) AS atr_14_rows,
            COUNT(short_volume_ratio) AS short_volume_ratio_rows
        FROM research_training_dataset
        WHERE dataset_id = ?
    """, [dataset_id]).fetchone())
finally:
    con.close()
PY
```

### Label coverage

```bash
python3 <<'PY'
import duckdb

dataset_id = "<DATASET_ID>"

con = duckdb.connect("/home/marty/stock-quant-oop/market.duckdb", read_only=True)
try:
    print(con.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(fwd_return_1d) AS fwd_return_1d_rows,
            COUNT(fwd_return_5d) AS fwd_return_5d_rows,
            COUNT(fwd_return_20d) AS fwd_return_20d_rows
        FROM research_labels
        WHERE dataset_id = ?
    """, [dataset_id]).fetchone())
finally:
    con.close()
PY
```

## Expected Current Reference Flow

```text
price_history / short_features_daily
  -> feature_*_daily
  -> research_features_daily
  -> research_training_dataset
  -> research_labels
  -> research_backtest
```
