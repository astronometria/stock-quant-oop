#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
import lightgbm as lgb
import joblib
import json

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"


def add_relevance_by_date(df: pd.DataFrame, n_bins: int = 5) -> pd.DataFrame:
    """
    Convertit target_return en labels entiers de ranking par date.
    0 = pire bucket, n_bins-1 = meilleur bucket.
    """
    out = df.copy()

    def _per_day(group: pd.DataFrame) -> pd.DataFrame:
        g = group.copy()

        # rang stable de target_return dans la coupe du jour
        # pct=True -> [0,1], puis bucketisation en bins entiers
        pct_rank = g["target_return"].rank(method="first", pct=True)

        relevance = ((pct_rank * n_bins).clip(upper=n_bins) - 1).astype(int)
        relevance = relevance.clip(lower=0, upper=n_bins - 1)

        g["relevance"] = relevance
        return g

    out = out.groupby("as_of_date", group_keys=False).apply(_per_day)
    return out


def run():
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split IN ('train', 'val')
          AND target_return IS NOT NULL
    """).fetchdf()

    con.close()

    # ordre temporel impératif pour grouper proprement
    df = df.sort_values(["as_of_date", "symbol"]).reset_index(drop=True)

    # cible ranking = relevance entière par date
    df = add_relevance_by_date(df, n_bins=5)

    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split",
        "relevance",
    ]

    X = df.drop(columns=drop_cols)
    y = df["relevance"].astype(int)

    group = df.groupby("as_of_date").size().to_list()

    model = lgb.LGBMRanker(
        objective="lambdarank",
        metric="ndcg",
        n_estimators=300,
        learning_rate=0.05,
        num_leaves=64,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        force_row_wise=True,
        random_state=42,
    )

    model.fit(
        X,
        y,
        group=group,
        eval_at=[10, 25, 50],
        callbacks=[lgb.log_evaluation(50)]
    )

    importance = pd.Series(
        model.feature_importances_,
        index=X.columns
    ).sort_values(ascending=False).head(20)

    print("===== TOP FEATURES =====")
    print(importance)

    joblib.dump(model, "models/lgbm_ranker.pkl")

    summary = {
        "rows": int(len(df)),
        "groups": int(len(group)),
        "features": int(X.shape[1]),
        "relevance_min": int(y.min()),
        "relevance_max": int(y.max()),
        "model_path": "models/lgbm_ranker.pkl",
    }
    print("===== RANKER SUMMARY =====")
    print(json.dumps(summary, indent=2))
    print("===== RANKER MODEL SAVED =====")


if __name__ == "__main__":
    run()
