#!/usr/bin/env python3
from __future__ import annotations

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
    parser.add_argument("--db-path", required=True)
    parser.add_argument(
        "--model-path",
        default="~/stock-quant-oop-runtime/models/lgbm_ranker_20d_quality.pkl",
    )
    parser.add_argument(
        "--metrics-path",
        default="~/stock-quant-oop-runtime/models/lgbm_ranker_20d_quality_metrics.json",
    )
    parser.add_argument("--n-estimators", type=int, default=1000)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    return parser.parse_args()


def build_group_sizes(df: pd.DataFrame) -> list[int]:
    return df.groupby("as_of_date", sort=True).size().astype(int).tolist()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    model_path = Path(args.model_path).expanduser().resolve()
    metrics_path = Path(args.metrics_path).expanduser().resolve()
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)

    progress = tqdm(total=5, desc="train_lgbm_ranker_20d_quality", leave=True)

    con = duckdb.connect(str(db_path))
    try:
        # ---------------------------------------------------------------------
        # PIT universe:
        # - research_universe_whitelist_20d_pit is date-aware
        # - must join on symbol + as_of_date
        # ---------------------------------------------------------------------
        df = con.execute("""
        WITH split_calendar AS (
            SELECT DISTINCT
                as_of_date,
                dataset_split
            FROM research_split_dataset
        ),
        future_prices AS (
            SELECT
                symbol,
                bar_date,
                adj_close,
                volume,
                LEAD(adj_close, 20) OVER (
                    PARTITION BY symbol
                    ORDER BY bar_date
                ) AS adj_close_fwd_20
            FROM price_bars_adjusted
            WHERE adj_close IS NOT NULL
              AND adj_close > 0
              AND volume IS NOT NULL
              AND volume > 0
        ),
        labeled AS (
            SELECT
                f.*,
                sc.dataset_split,
                p.adj_close AS exec_close,
                p.volume AS exec_volume,
                (p.adj_close * p.volume) AS exec_dollar_volume,
                CASE
                    WHEN p.adj_close_fwd_20 IS NULL OR p.adj_close <= 0 OR p.adj_close_fwd_20 <= 0 THEN NULL
                    ELSE (p.adj_close_fwd_20 / p.adj_close) - 1
                END AS target_return_20d
            FROM research_features_daily f
            INNER JOIN research_universe_whitelist_20d_pit q
                ON f.symbol = q.symbol
               AND f.as_of_date = q.as_of_date
            INNER JOIN split_calendar sc
                ON f.as_of_date = sc.as_of_date
            INNER JOIN future_prices p
                ON f.symbol = p.symbol
               AND f.as_of_date = p.bar_date
            WHERE sc.dataset_split IN ('train','val','test')
        ),
        ranked AS (
            SELECT
                *,
                NTILE(5) OVER (
                    PARTITION BY as_of_date
                    ORDER BY target_return_20d
                ) - 1 AS relevance_20d
            FROM labeled
            WHERE target_return_20d IS NOT NULL
        )
        SELECT *
        FROM ranked
        ORDER BY as_of_date, symbol
        """).fetchdf()
    finally:
        con.close()
    progress.update(1)

    if df.empty:
        raise RuntimeError("Training frame is empty after PIT whitelist filtering.")

    train_df = df[df["dataset_split"] == "train"].copy()
    val_df = df[df["dataset_split"] == "val"].copy()
    test_df = df[df["dataset_split"] == "test"].copy()
    progress.update(1)

    if train_df.empty or val_df.empty:
        raise RuntimeError("Train or validation split is empty.")

    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "dataset_split",
        "exec_close",
        "exec_volume",
        "exec_dollar_volume",
        "target_return_20d",
        "relevance_20d",
    ]
    feature_cols = [c for c in train_df.columns if c not in drop_cols]

    X_train = train_df[feature_cols].copy()
    y_train = train_df["relevance_20d"].astype(int)

    X_val = val_df[feature_cols].copy()
    y_val = val_df["relevance_20d"].astype(int)

    train_group = build_group_sizes(train_df)
    val_group = build_group_sizes(val_df)
    progress.update(1)

    model = lgb.LGBMRanker(
        objective="lambdarank",
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        num_leaves=31,
        min_child_samples=200,
        subsample=0.8,
        subsample_freq=1,
        colsample_bytree=0.8,
        reg_alpha=0.5,
        reg_lambda=0.5,
        random_state=42,
        n_jobs=-1,
    )

    model.fit(
        X_train,
        y_train,
        group=train_group,
        eval_set=[(X_val, y_val)],
        eval_group=[val_group],
        eval_at=[10, 25, 50],
        callbacks=[
            lgb.early_stopping(stopping_rounds=100),
            lgb.log_evaluation(period=50),
        ],
    )
    progress.update(1)

    joblib.dump(model, model_path)

    metrics = {
        "trainer": "train_lgbm_ranker_20d_quality.py",
        "universe_table": "research_universe_whitelist_20d_pit",
        "rows_train": int(len(train_df)),
        "rows_val": int(len(val_df)),
        "rows_test": int(len(test_df)),
        "features": int(len(feature_cols)),
        "feature_names": feature_cols,
        "best_iteration_": int(model.best_iteration_) if model.best_iteration_ is not None else None,
        "best_score_": model.best_score_,
        "model_path": str(model_path),
    }
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    progress.update(1)
    progress.close()

    print("===== TRAIN LGBM RANKER 20D QUALITY =====")
    print(json.dumps({
        "model_path": str(model_path),
        "metrics_path": str(metrics_path),
        "universe_table": metrics["universe_table"],
        "rows_train": metrics["rows_train"],
        "rows_val": metrics["rows_val"],
        "rows_test": metrics["rows_test"],
        "features": metrics["features"],
        "best_iteration_": metrics["best_iteration_"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
