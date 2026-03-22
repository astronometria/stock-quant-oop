#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Train an aligned 1D LightGBM ranker
#
# Why this script exists:
# - The public repo currently centers labels around 5d / 10d / 20d horizons.
# - Our validated backtest now uses realized next-day returns.
# - We therefore need a trainer that is aligned with that next-day execution
#   horizon, without depending on any fragile intermediate local script state.
#
# Design choices:
# - SQL-first data extraction from DuckDB
# - canonical prices from price_bars_adjusted.adj_close
# - reuse existing train/val/test calendar from research_split_dataset
# - execution-aligned liquidity filters
# - heavy comments for maintainability
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb
import joblib
import lightgbm as lgb
import pandas as pd
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    parser.add_argument(
        "--model-path",
        default="/home/marty/stock-quant-oop/models/lgbm_ranker.pkl",
        help="Output path for the trained LightGBM model.",
    )
    parser.add_argument(
        "--metrics-path",
        default="/home/marty/stock-quant-oop/models/lgbm_ranker_1d_metrics.json",
        help="Output path for training metrics JSON.",
    )
    parser.add_argument(
        "--min-price",
        type=float,
        default=5.0,
        help="Minimum adjusted close used in train/validation rows.",
    )
    parser.add_argument(
        "--min-dollar-volume",
        type=float,
        default=1_000_000.0,
        help="Minimum adjusted dollar volume used in train/validation rows.",
    )
    parser.add_argument(
        "--exclude-symbol",
        action="append",
        default=["ZVZZT"],
        help="Symbol to exclude from the aligned 1D training set. Can be repeated.",
    )
    parser.add_argument(
        "--n-estimators",
        type=int,
        default=1000,
        help="Maximum number of boosting rounds.",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=0.05,
        help="Learning rate for LightGBM.",
    )
    return parser.parse_args()


def load_aligned_training_frame(
    con: duckdb.DuckDBPyConnection,
    min_price: float,
    min_dollar_volume: float,
    excluded_symbols: list[str],
) -> pd.DataFrame:
    """
    Build an aligned 1D training frame directly from canonical adjusted prices.

    Key points:
    - target_return_1d uses next-day adjusted close
    - relevance_1d is a cross-sectional 5-bin relevance score per date
    - split assignment is reused from the existing split calendar by date
    - the same execution filters are applied as in the cleaned backtest
    """
    excluded_sql = ""
    if excluded_symbols:
        quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in sorted(set(excluded_symbols)))
        excluded_sql = f"AND f.symbol NOT IN ({quoted})"

    sql = f"""
    WITH split_calendar AS (
        SELECT DISTINCT
            as_of_date,
            dataset_split
        FROM research_split_dataset
    ),
    next_day_prices AS (
        SELECT
            symbol,
            bar_date,
            adj_close,
            volume,
            LEAD(adj_close, 1) OVER (
                PARTITION BY symbol
                ORDER BY bar_date
            ) AS adj_close_next
        FROM price_bars_adjusted
        WHERE adj_close IS NOT NULL
          AND adj_close > 0
    ),
    labeled_base AS (
        SELECT
            f.*,
            sc.dataset_split,
            p.adj_close AS exec_close,
            p.volume AS exec_volume,
            p.adj_close * p.volume AS exec_dollar_volume,
            CASE
                WHEN p.adj_close_next IS NULL OR p.adj_close <= 0 OR p.adj_close_next <= 0 THEN NULL
                ELSE (p.adj_close_next / p.adj_close) - 1
            END AS target_return_1d
        FROM research_features_daily f
        INNER JOIN split_calendar sc
            ON f.as_of_date = sc.as_of_date
        INNER JOIN next_day_prices p
            ON f.symbol = p.symbol
           AND f.as_of_date = p.bar_date
        WHERE p.adj_close >= {float(min_price)}
          AND (p.adj_close * p.volume) >= {float(min_dollar_volume)}
          {excluded_sql}
    ),
    filtered AS (
        SELECT *
        FROM labeled_base
        WHERE target_return_1d IS NOT NULL
          AND dataset_split IN ('train', 'val', 'test')
    ),
    ranked AS (
        SELECT
            *,
            NTILE(5) OVER (
                PARTITION BY as_of_date
                ORDER BY target_return_1d
            ) - 1 AS relevance_1d
        FROM filtered
    )
    SELECT *
    FROM ranked
    ORDER BY as_of_date, symbol
    """
    return con.execute(sql).fetchdf()


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    """
    LightGBM ranker expects group sizes, one group per query/date.
    """
    grouped = (
        df.groupby("as_of_date", sort=True)
          .size()
          .astype(int)
          .tolist()
    )
    return grouped


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    model_path = Path(args.model_path).expanduser().resolve()
    metrics_path = Path(args.metrics_path).expanduser().resolve()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)

    progress = tqdm(total=5, desc="train_lgbm_ranker_1d", leave=True)

    # -------------------------------------------------------------------------
    # Step 1: connect and extract the aligned 1D frame.
    # -------------------------------------------------------------------------
    con = duckdb.connect(str(db_path))
    try:
        df = load_aligned_training_frame(
            con=con,
            min_price=args.min_price,
            min_dollar_volume=args.min_dollar_volume,
            excluded_symbols=args.exclude_symbol,
        )
    finally:
        con.close()
    progress.update(1)

    if df.empty:
        raise RuntimeError("Aligned 1D training frame is empty after filters.")

    # -------------------------------------------------------------------------
    # Step 2: split train / val / test.
    # We train on train, early-stop on val, and keep test for downstream audit.
    # -------------------------------------------------------------------------
    train_df = df[df["dataset_split"] == "train"].copy()
    val_df = df[df["dataset_split"] == "val"].copy()
    test_df = df[df["dataset_split"] == "test"].copy()
    progress.update(1)

    if train_df.empty or val_df.empty:
        raise RuntimeError("Train or validation split is empty for aligned 1D training.")

    # -------------------------------------------------------------------------
    # Step 3: build feature matrices.
    # We remove identity, split, target, and execution-only columns.
    # -------------------------------------------------------------------------
    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "dataset_split",
        "exec_close",
        "exec_volume",
        "exec_dollar_volume",
        "target_return_1d",
        "relevance_1d",
    ]

    feature_cols = [c for c in train_df.columns if c not in drop_cols]

    X_train = train_df[feature_cols].copy()
    y_train = train_df["relevance_1d"].astype(int)

    X_val = val_df[feature_cols].copy()
    y_val = val_df["relevance_1d"].astype(int)

    train_group = build_group_sizes(train_df)
    val_group = build_group_sizes(val_df)
    progress.update(1)

    # -------------------------------------------------------------------------
    # Step 4: train a LightGBM ranker.
    # We use lambdarank because the downstream portfolio consumes a ranking.
    # -------------------------------------------------------------------------
    model = lgb.LGBMRanker(
        objective="lambdarank",
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=63,
        min_child_samples=100,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=0.1,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(
        X_train,
        y_train,
        group=train_group,
        eval_set=[(X_val, y_val)],
        eval_group=[val_group],
        eval_at=[10, 25, 50, 100],
        callbacks=[
            lgb.early_stopping(stopping_rounds=100),
            lgb.log_evaluation(period=50),
        ],
    )
    progress.update(1)

    # -------------------------------------------------------------------------
    # Step 5: save model and metrics.
    # -------------------------------------------------------------------------
    joblib.dump(model, model_path)

    metrics = {
        "trainer": "train_lgbm_ranker_1d_aligned.py",
        "db_path": str(db_path),
        "model_path": str(model_path),
        "rows_total": int(len(df)),
        "rows_train": int(len(train_df)),
        "rows_val": int(len(val_df)),
        "rows_test": int(len(test_df)),
        "features": int(len(feature_cols)),
        "feature_names": feature_cols,
        "train_dates_min": str(train_df["as_of_date"].min()),
        "train_dates_max": str(train_df["as_of_date"].max()),
        "val_dates_min": str(val_df["as_of_date"].min()),
        "val_dates_max": str(val_df["as_of_date"].max()),
        "test_dates_min": str(test_df["as_of_date"].min()) if not test_df.empty else None,
        "test_dates_max": str(test_df["as_of_date"].max()) if not test_df.empty else None,
        "train_groups": int(len(train_group)),
        "val_groups": int(len(val_group)),
        "min_price": float(args.min_price),
        "min_dollar_volume": float(args.min_dollar_volume),
        "excluded_symbols": sorted(set(args.exclude_symbol)),
        "best_iteration_": int(model.best_iteration_) if model.best_iteration_ is not None else None,
        "best_score_": model.best_score_,
    }

    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    progress.update(1)
    progress.close()

    print("===== TRAIN LGBM RANKER 1D ALIGNED =====")
    print(json.dumps({
        "model_path": str(model_path),
        "metrics_path": str(metrics_path),
        "rows_train": metrics["rows_train"],
        "rows_val": metrics["rows_val"],
        "rows_test": metrics["rows_test"],
        "features": metrics["features"],
        "best_iteration_": metrics["best_iteration_"],
        "excluded_symbols": metrics["excluded_symbols"],
    }, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
