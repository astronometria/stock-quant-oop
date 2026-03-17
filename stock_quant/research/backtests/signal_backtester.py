from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import sqrt
from typing import Any

import pandas as pd


@dataclass(slots=True)
class CrossSectionalBacktestOutput:
    """
    Résultat complet du backtest cross-sectional v1.

    Notes
    -----
    - positions : sélection journalière des titres retenus
    - daily     : métriques agrégées journalières
    - summary   : résumé global simple
    """
    positions: list[dict[str, Any]]
    daily: list[dict[str, Any]]
    summary: dict[str, Any]


class SignalBacktester:
    """
    Moteur de backtest cross-sectional v1.

    Hypothèses v1
    -------------
    - long-only
    - equal-weight
    - rebalance daily
    - top_n titres retenus chaque jour
    - performance = moyenne simple du label forward sélectionné
    """

    def run_cross_sectional_backtest(
        self,
        dataset_rows: list[dict[str, Any]],
        *,
        signal_column: str,
        label_column: str,
        top_n: int,
        experiment_name: str,
        backtest_name: str,
        dataset_name: str,
        dataset_version: str,
    ) -> CrossSectionalBacktestOutput:
        frame = pd.DataFrame(dataset_rows)

        # --------------------------------------------------------------
        # Cas vide : on retourne un résultat vide mais structuré.
        # --------------------------------------------------------------
        if frame.empty:
            return CrossSectionalBacktestOutput(
                positions=[],
                daily=[],
                summary={
                    "backtest_name": backtest_name,
                    "dataset_name": dataset_name,
                    "dataset_version": dataset_version,
                    "signal_column": signal_column,
                    "label_column": label_column,
                    "top_n": top_n,
                    "observations": 0,
                    "selected_observations": 0,
                    "trading_days": 0,
                    "mean_daily_return": None,
                    "volatility_daily": None,
                    "sharpe_like": None,
                    "hit_rate": None,
                    "cumulative_return_proxy": None,
                    "created_at": datetime.utcnow(),
                },
            )

        required_columns = {
            "instrument_id",
            "company_id",
            "symbol",
            "as_of_date",
            signal_column,
            label_column,
        }
        missing = sorted(col for col in required_columns if col not in frame.columns)
        if missing:
            raise ValueError(f"dataset_rows missing required columns: {missing}")

        # --------------------------------------------------------------
        # On force les colonnes quantitatives en numérique.
        # --------------------------------------------------------------
        frame["signal_value"] = pd.to_numeric(frame[signal_column], errors="coerce")
        frame["realized_return"] = pd.to_numeric(frame[label_column], errors="coerce")

        # --------------------------------------------------------------
        # On garde seulement les lignes exploitables.
        # - signal connu
        # - label connu
        # --------------------------------------------------------------
        usable = frame[
            frame["signal_value"].notna() & frame["realized_return"].notna()
        ].copy()

        if usable.empty:
            return CrossSectionalBacktestOutput(
                positions=[],
                daily=[],
                summary={
                    "backtest_name": backtest_name,
                    "dataset_name": dataset_name,
                    "dataset_version": dataset_version,
                    "signal_column": signal_column,
                    "label_column": label_column,
                    "top_n": top_n,
                    "observations": int(len(frame)),
                    "selected_observations": 0,
                    "trading_days": 0,
                    "mean_daily_return": None,
                    "volatility_daily": None,
                    "sharpe_like": None,
                    "hit_rate": None,
                    "cumulative_return_proxy": None,
                    "created_at": datetime.utcnow(),
                },
            )

        usable["as_of_date"] = pd.to_datetime(usable["as_of_date"]).dt.date

        positions: list[dict[str, Any]] = []
        daily_rows: list[dict[str, Any]] = []

        # --------------------------------------------------------------
        # Ranking cross-sectional par jour.
        # --------------------------------------------------------------
        for as_of_date, day_frame in usable.groupby("as_of_date", sort=True):
            ranked = day_frame.sort_values(
                by=["signal_value", "symbol"],
                ascending=[False, True],
            ).head(max(int(top_n), 1)).copy()

            if ranked.empty:
                continue

            weight = 1.0 / float(len(ranked))
            ranked["weight"] = weight
            ranked["rank"] = range(1, len(ranked) + 1)
            ranked["position_return"] = ranked["realized_return"] * ranked["weight"]

            for row in ranked.itertuples(index=False):
                positions.append(
                    {
                        "experiment_name": experiment_name,
                        "backtest_name": backtest_name,
                        "dataset_name": dataset_name,
                        "dataset_version": dataset_version,
                        "instrument_id": row.instrument_id,
                        "company_id": row.company_id,
                        "symbol": row.symbol,
                        "as_of_date": row.as_of_date,
                        "signal_column": signal_column,
                        "label_column": label_column,
                        "signal_value": float(row.signal_value),
                        "realized_return": float(row.realized_return),
                        "rank_position": int(row.rank),
                        "weight": float(row.weight),
                        "created_at": datetime.utcnow(),
                    }
                )

            gross_return = float(ranked["realized_return"].mean())
            selected_count = int(len(ranked))
            hit_rate = float((ranked["realized_return"] > 0).mean())

            daily_rows.append(
                {
                    "backtest_name": backtest_name,
                    "dataset_name": dataset_name,
                    "dataset_version": dataset_version,
                    "as_of_date": as_of_date,
                    "signal_column": signal_column,
                    "label_column": label_column,
                    "selected_count": selected_count,
                    "gross_return": gross_return,
                    "hit_rate": hit_rate,
                    "created_at": datetime.utcnow(),
                }
            )

        daily_frame = pd.DataFrame(daily_rows)

        if daily_frame.empty:
            summary = {
                "backtest_name": backtest_name,
                "dataset_name": dataset_name,
                "dataset_version": dataset_version,
                "signal_column": signal_column,
                "label_column": label_column,
                "top_n": top_n,
                "observations": int(len(frame)),
                "selected_observations": 0,
                "trading_days": 0,
                "mean_daily_return": None,
                "volatility_daily": None,
                "sharpe_like": None,
                "hit_rate": None,
                "cumulative_return_proxy": None,
                "created_at": datetime.utcnow(),
            }
            return CrossSectionalBacktestOutput(
                positions=positions,
                daily=daily_rows,
                summary=summary,
            )

        mean_daily_return = float(daily_frame["gross_return"].mean())
        volatility_daily = (
            float(daily_frame["gross_return"].std(ddof=0))
            if len(daily_frame) > 1
            else 0.0
        )
        sharpe_like = None
        if volatility_daily and volatility_daily > 0:
            sharpe_like = float(mean_daily_return / volatility_daily * sqrt(252.0))

        hit_rate_total = float((daily_frame["gross_return"] > 0).mean())

        cumulative_return_proxy = float((1.0 + daily_frame["gross_return"]).prod() - 1.0)

        summary = {
            "backtest_name": backtest_name,
            "dataset_name": dataset_name,
            "dataset_version": dataset_version,
            "signal_column": signal_column,
            "label_column": label_column,
            "top_n": int(top_n),
            "observations": int(len(frame)),
            "selected_observations": int(len(positions)),
            "trading_days": int(len(daily_frame)),
            "mean_daily_return": mean_daily_return,
            "volatility_daily": volatility_daily,
            "sharpe_like": sharpe_like,
            "hit_rate": hit_rate_total,
            "cumulative_return_proxy": cumulative_return_proxy,
            "created_at": datetime.utcnow(),
        }

        return CrossSectionalBacktestOutput(
            positions=positions,
            daily=daily_rows,
            summary=summary,
        )
