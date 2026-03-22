#!/usr/bin/env python3
from __future__ import annotations

import duckdb
import pandas as pd
import lightgbm as lgb
import json

DB_PATH = "~/stock-quant-oop-runtime/db/market.duckdb"


def run():
    con = duckdb.connect(DB_PATH)

    df = con.execute("""
        SELECT *
        FROM research_split_dataset
        WHERE dataset_split IN ('train', 'val')
    """).fetchdf()

    con.close()

    y = df["target_class"]

    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split"
    ]

    X = df.drop(columns=drop_cols)

    train_mask = df["dataset_split"] == "train"

    X_train = X[train_mask]
    y_train = y[train_mask]

    X_val = X[~train_mask]
    y_val = y[~train_mask]

    model = lgb.LGBMClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=64,
        subsample=0.8,
        colsample_bytree=0.8
    )

    model.fit(
        X_train,
        y_train,
        eval_set=[(X_val, y_val)],
        eval_metric="auc",
        callbacks=[
            lgb.log_evaluation(50)
        ]
    )

    importance = pd.Series(
        model.feature_importances_,
        index=X.columns
    ).sort_values(ascending=False).head(20)

    print("===== TOP FEATURES =====")
    print(importance)

    import joblib
    joblib.dump(model, "models/lgbm_model.pkl")

    print("===== MODEL SAVED =====")


if __name__ == "__main__":
    run()
