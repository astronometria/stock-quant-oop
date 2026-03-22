#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Portfolio Engine V3
#
# IMPORTANT:
# - The model is trained on forward labels (target_return), but target_return is
#   NOT a tradable realized PnL series.
# - We therefore use target_return only for ranking / scoring.
# - Actual portfolio PnL is computed from next-day adjusted returns built from
#   the canonical price table price_bars_adjusted.
#
# This removes the major evaluation bug where the engine was effectively trading
# the future label directly.
# =============================================================================

import json
import duckdb
import joblib
import pandas as pd

DB_PATH = "/home/marty/stock-quant-oop/market.duckdb"
MODEL_PATH = "/home/marty/stock-quant-oop/models/lgbm_ranker.pkl"

AUM = 1_000_000
BUCKET_PCT = 0.05
COST_BPS = 20
MAX_WEIGHT = 0.02


def zscore(s: pd.Series) -> pd.Series:
    """Cross-sectional z-score with safe zero-std handling."""
    std = s.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def main() -> None:
    # -------------------------------------------------------------------------
    # Load model and database connection.
    # -------------------------------------------------------------------------
    model = joblib.load(MODEL_PATH)
    con = duckdb.connect(DB_PATH)

    # -------------------------------------------------------------------------
    # Build the test universe with:
    # - model features from research_split_dataset
    # - liquidity filter inputs from price_bars_adjusted
    # - realized next-day adjusted return for actual PnL
    #
    # realized_return_1d is computed as:
    #   LEAD(adj_close, 1) / adj_close - 1
    #
    # This is the tradable realized return proxy used by the backtest.
    # -------------------------------------------------------------------------
    df = con.execute("""
    WITH next_day_prices AS (
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
    )
    SELECT
        r.*,
        p.volume,
        p.adj_close AS close,
        p.adj_close * p.volume AS dollar_volume,
        CASE
            WHEN p.adj_close_next IS NULL OR p.adj_close <= 0 OR p.adj_close_next <= 0 THEN NULL
            ELSE (p.adj_close_next / p.adj_close) - 1
        END AS realized_return_1d
    FROM research_split_dataset r
    LEFT JOIN next_day_prices p
        ON r.symbol = p.symbol
       AND r.as_of_date = p.bar_date
    WHERE r.dataset_split = 'test'
      AND r.target_return IS NOT NULL
    ORDER BY r.as_of_date
    """).fetchdf()

    # -------------------------------------------------------------------------
    # Basic execution universe filters.
    # These are execution filters, not model-training filters.
    # -------------------------------------------------------------------------
    df = df[
        (df["close"] >= 5) &
        (df["dollar_volume"] >= 1_000_000) &
        (df["realized_return_1d"].notna())
    ].copy()

    # -------------------------------------------------------------------------
    # Prepare model feature matrix.
    # Any non-feature columns are removed before prediction.
    # -------------------------------------------------------------------------
    drop_cols = [
        "symbol",
        "instrument_id",
        "company_id",
        "as_of_date",
        "target_return",
        "target_class",
        "dataset_split",
        "volume",
        "dollar_volume",
        "realized_return_1d",
    ]

    X = df.drop(columns=drop_cols, errors="ignore")
    model_features = model.booster_.feature_name()
    X = X.reindex(columns=model_features)

    # -------------------------------------------------------------------------
    # Score the universe.
    # The model score ranks names cross-sectionally.
    # -------------------------------------------------------------------------
    df["score"] = -model.predict(X)

    # -------------------------------------------------------------------------
    # Simulate a daily rebalanced long/short portfolio.
    #
    # IMPORTANT:
    # - ranking signal uses model score
    # - realized PnL uses realized_return_1d
    # -------------------------------------------------------------------------
    capital = AUM
    prev_weights: dict[str, float] = {}
    equity_curve: list[tuple] = []

    for date, g in df.groupby("as_of_date"):
        if len(g) < 50:
            continue

        g = g.copy()
        g["score_z"] = zscore(g["score"])
        g = g.sort_values("score_z")

        n = len(g)
        k = max(int(n * BUCKET_PCT), 1)

        long = g.tail(k).copy()
        short = g.head(k).copy()

        # ---------------------------------------------------------------------
        # Build score-proportional long weights from positive z-scores.
        # ---------------------------------------------------------------------
        long_w = long["score_z"].clip(lower=0)
        long_w = long_w / (long_w.abs().sum() + 1e-9)

        # ---------------------------------------------------------------------
        # Build score-proportional short weights from negative z-scores.
        # ---------------------------------------------------------------------
        short_w = short["score_z"].clip(upper=0)
        short_w = -short_w.abs() / (short_w.abs().sum() + 1e-9)

        long["weight"] = long_w
        short["weight"] = short_w

        book = pd.concat([long, short], ignore_index=True)

        # ---------------------------------------------------------------------
        # Cap single-name weight, then renormalize gross exposure to 1.0.
        # ---------------------------------------------------------------------
        book["weight"] = book["weight"].clip(-MAX_WEIGHT, MAX_WEIGHT)
        gross_abs = book["weight"].abs().sum()

        if gross_abs <= 0:
            continue

        book["weight"] = book["weight"] / gross_abs

        # ---------------------------------------------------------------------
        # Realized next-day PnL:
        # long positions gain when realized_return_1d is positive
        # short positions gain when realized_return_1d is negative
        # because the weight already has the correct sign
        # ---------------------------------------------------------------------
        book["ret"] = book["realized_return_1d"]
        book["pnl"] = book["weight"] * book["ret"]

        gross = float(book["pnl"].sum())

        # ---------------------------------------------------------------------
        # Turnover and linear transaction cost model.
        # ---------------------------------------------------------------------
        current_weights = dict(zip(book["symbol"], book["weight"]))
        all_symbols = set(current_weights) | set(prev_weights)

        turnover = float(
            sum(abs(current_weights.get(s, 0.0) - prev_weights.get(s, 0.0)) for s in all_symbols)
        )

        cost = float(turnover * (COST_BPS / 10000.0))
        net = float(gross - cost)

        capital *= (1.0 + net)

        equity_curve.append(
            (
                date,
                capital,
                gross,
                cost,
                net,
                turnover,
            )
        )

        prev_weights = current_weights

    df_eq = pd.DataFrame(
        equity_curve,
        columns=["date", "capital", "gross", "cost", "return", "turnover"],
    )

    if df_eq.empty:
        raise RuntimeError("Backtest produced no rows after filters.")

    mean_daily = float(df_eq["return"].mean())
    std_daily = float(df_eq["return"].std())
    sharpe = mean_daily / (std_daily + 1e-9)

    result = {
        "final_capital": float(df_eq["capital"].iloc[-1]),
        "total_return": float(df_eq["capital"].iloc[-1] / AUM - 1.0),
        "mean_daily": mean_daily,
        "sharpe": sharpe,
        "avg_turnover": float(df_eq["turnover"].mean()),
        "avg_gross_before_cost": float(df_eq["gross"].mean()),
        "avg_cost": float(df_eq["cost"].mean()),
        "days": int(len(df_eq)),
    }

    print("===== PORTFOLIO V3 (REALIZED NEXT-DAY PNL) =====")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
