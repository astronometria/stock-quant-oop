#!/usr/bin/env python3

"""
Premier test de stratégie de recherche "price-only" sur DuckDB.

Objectif:
- Ne dépendre que de la table price_history réellement présente dans la DB.
- Éviter le lookahead évident avec un lag d'exécution de 1 barre.
- Produire des sorties faciles à auditer dans logs/.

Logique du test:
1. Construire un signal de momentum 20 jours à la date t:
      mom_20d(t) = close(t) / close(t-20) - 1
2. Ne PAS trader sur le close de t.
3. Appliquer un lag d'exécution d'une barre complète:
      retour exécuté = close(t+2) / close(t+1) - 1
   Donc:
   - le signal est observé à t
   - on "entre" après une barre complète, sur t+1
   - on mesure le retour entre t+1 et t+2
4. Chaque jour:
   - top N momentum -> portefeuille long
   - bottom N momentum -> portefeuille short de recherche
   - univers équipondéré -> benchmark simple

Important:
- Ceci est un test de recherche initial, pas encore un moteur institutionnel complet.
- On n'intègre pas ici:
  - slippage réaliste
  - spread
  - ADV participation
  - borrow cost
  - contraintes sectorielles
  - univers PIT strict via universe_membership_history
- Mais on évite déjà un biais de timing trop naïf.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from tqdm import tqdm


def to_json_safe(value: Any) -> Any:
    """
    Convertit récursivement les objets pandas / dates / timestamps / nombres numpy
    en types JSON-sérialisables natifs Python.

    Cette fonction est volontairement défensive pour éviter qu'un seul type exotique
    casse l'écriture des artefacts de recherche en fin de run.
    """
    if value is None:
        return None

    # Timestamp / datetime / date pandas
    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    # Valeurs manquantes pandas/numpy
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    # Dictionnaires
    if isinstance(value, dict):
        return {str(k): to_json_safe(v) for k, v in value.items()}

    # Listes / tuples
    if isinstance(value, (list, tuple)):
        return [to_json_safe(v) for v in value]

    # Types numériques / dates "duckdb/python" convertissables via string si besoin
    if hasattr(value, "item"):
        try:
            return to_json_safe(value.item())
        except Exception:
            pass

    # Objets dates / datetimes Python
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    # Types Python déjà sérialisables
    if isinstance(value, (str, int, float, bool)):
        return value

    # Fallback ultra défensif
    return str(value)


def compute_metrics(daily_returns: pd.Series) -> dict:
    """
    Calcule des métriques simples de recherche à partir d'une série de retours journaliers.

    Hypothèse:
    - fréquence quotidienne de trading ~ 252 jours par an
    """
    series = daily_returns.dropna().astype(float)
    if series.empty:
        return {
            "days": 0,
            "total_return": None,
            "cagr": None,
            "mean_daily": None,
            "vol_daily": None,
            "sharpe_annualized": None,
            "max_drawdown": None,
            "win_rate": None,
        }

    equity = (1.0 + series).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)

    num_days = int(len(series))
    years = num_days / 252.0 if num_days > 0 else 0.0

    if years > 0 and equity.iloc[-1] > 0:
        cagr = float(equity.iloc[-1] ** (1.0 / years) - 1.0)
    else:
        cagr = None

    mean_daily = float(series.mean())
    vol_daily = float(series.std(ddof=1)) if len(series) > 1 else 0.0

    if vol_daily and not math.isclose(vol_daily, 0.0):
        sharpe_annualized = float((mean_daily / vol_daily) * math.sqrt(252.0))
    else:
        sharpe_annualized = None

    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    max_drawdown = float(drawdown.min())

    win_rate = float((series > 0).mean())

    return {
        "days": num_days,
        "total_return": total_return,
        "cagr": cagr,
        "mean_daily": mean_daily,
        "vol_daily": vol_daily,
        "sharpe_annualized": sharpe_annualized,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a price-only research strategy on DuckDB price_history.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path.home() / "stock-quant-oop-runtime" / "db" / "market.duckdb",
        help="Path to DuckDB database.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of names in long and short baskets.",
    )
    parser.add_argument(
        "--min-close",
        type=float,
        default=5.0,
        help="Minimum close price filter.",
    )
    parser.add_argument(
        "--min-volume",
        type=float,
        default=100000.0,
        help="Minimum raw daily volume filter.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Optional lower bound on price_date, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Optional upper bound on price_date, format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="research_strategy_price_only",
        help="Prefix for output files inside logs/.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[2]
    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    summary_path = logs_dir / f"{args.output_prefix}_summary.json"
    daily_path = logs_dir / f"{args.output_prefix}_daily.csv"

    steps = tqdm(total=5, desc="research_strategy", unit="step")

    # -------------------------------------------------------------------------
    # Étape 1: ouvrir la DB et vérifier la table / colonnes réellement présentes
    # -------------------------------------------------------------------------
    con = duckdb.connect(str(args.db_path))
    steps.set_description("research_strategy:open_db")

    tables = con.execute("SHOW TABLES").fetchdf()
    if "price_history" not in set(tables["name"].tolist()):
        raise RuntimeError("Table price_history introuvable dans la DB.")

    schema_df = con.execute("DESCRIBE price_history").fetchdf()
    required_columns = {"symbol", "price_date", "close", "volume"}
    present_columns = set(schema_df["column_name"].tolist())

    missing = sorted(required_columns - present_columns)
    if missing:
        raise RuntimeError(f"Colonnes manquantes dans price_history: {missing}")

    steps.update(1)

    # -------------------------------------------------------------------------
    # Étape 2: probe de couverture
    # -------------------------------------------------------------------------
    steps.set_description("research_strategy:probe")
    coverage_sql = """
    SELECT
        COUNT(*) AS rows_total,
        COUNT(DISTINCT symbol) AS symbols_total,
        MIN(price_date) AS min_date,
        MAX(price_date) AS max_date
    FROM price_history
    """
    coverage = con.execute(coverage_sql).fetchdf().iloc[0].to_dict()
    coverage = to_json_safe(coverage)
    steps.update(1)

    # -------------------------------------------------------------------------
    # Étape 3: construire le dataset de recherche en SQL-first
    # -------------------------------------------------------------------------
    steps.set_description("research_strategy:build_dataset")

    date_filters = []
    if args.start_date:
        date_filters.append(f"price_date >= DATE '{args.start_date}'")
    if args.end_date:
        date_filters.append(f"price_date <= DATE '{args.end_date}'")

    where_clause = ""
    if date_filters:
        where_clause = "WHERE " + " AND ".join(date_filters)

    query = f"""
    WITH base AS (
        SELECT
            symbol,
            price_date,
            CAST(close AS DOUBLE) AS close,
            CAST(volume AS DOUBLE) AS volume,
            LAG(close, 20) OVER (
                PARTITION BY symbol
                ORDER BY price_date
            ) AS close_lag_20,
            LEAD(close, 1) OVER (
                PARTITION BY symbol
                ORDER BY price_date
            ) AS close_t1,
            LEAD(close, 2) OVER (
                PARTITION BY symbol
                ORDER BY price_date
            ) AS close_t2
        FROM price_history
        {where_clause}
    ),
    signals AS (
        SELECT
            symbol,
            price_date,
            close,
            volume,
            (close / NULLIF(close_lag_20, 0)) - 1.0 AS mom_20d,
            (close_t2 / NULLIF(close_t1, 0)) - 1.0 AS exec_ret_1d_lag1
        FROM base
        WHERE
            close_lag_20 IS NOT NULL
            AND close_t1 IS NOT NULL
            AND close_t2 IS NOT NULL
            AND close IS NOT NULL
            AND volume IS NOT NULL
            AND close >= ?
            AND volume >= ?
    ),
    ranked AS (
        SELECT
            symbol,
            price_date,
            mom_20d,
            exec_ret_1d_lag1,
            ROW_NUMBER() OVER (
                PARTITION BY price_date
                ORDER BY mom_20d DESC, symbol
            ) AS rn_long,
            ROW_NUMBER() OVER (
                PARTITION BY price_date
                ORDER BY mom_20d ASC, symbol
            ) AS rn_short,
            COUNT(*) OVER (
                PARTITION BY price_date
            ) AS day_count
        FROM signals
    ),
    portfolio AS (
        SELECT
            price_date,
            AVG(CASE WHEN rn_long <= ? THEN exec_ret_1d_lag1 END) AS long_ret,
            AVG(CASE WHEN rn_short <= ? THEN exec_ret_1d_lag1 END) AS short_ret,
            AVG(exec_ret_1d_lag1) AS universe_ret,
            SUM(CASE WHEN rn_long <= ? THEN 1 ELSE 0 END) AS long_names,
            SUM(CASE WHEN rn_short <= ? THEN 1 ELSE 0 END) AS short_names,
            MAX(day_count) AS universe_names
        FROM ranked
        GROUP BY price_date
        HAVING
            long_names = ?
            AND short_names = ?
        ORDER BY price_date
    )
    SELECT
        price_date,
        long_ret,
        short_ret,
        universe_ret,
        (long_ret - short_ret) AS long_short_spread_ret,
        long_names,
        short_names,
        universe_names
    FROM portfolio
    ORDER BY price_date
    """

    params = [
        args.min_close,
        args.min_volume,
        args.top_n,
        args.top_n,
        args.top_n,
        args.top_n,
        args.top_n,
        args.top_n,
    ]

    daily_df = con.execute(query, params).fetchdf()
    if daily_df.empty:
        raise RuntimeError(
            "Le dataset de stratégie est vide. Essaie un min_volume plus faible, "
            "un top-n plus petit, ou sonde davantage la base."
        )

    daily_df["price_date"] = pd.to_datetime(daily_df["price_date"])
    steps.update(1)

    # -------------------------------------------------------------------------
    # Étape 4: calcul des métriques
    # -------------------------------------------------------------------------
    steps.set_description("research_strategy:metrics")

    long_metrics = compute_metrics(daily_df["long_ret"])
    spread_metrics = compute_metrics(daily_df["long_short_spread_ret"])
    universe_metrics = compute_metrics(daily_df["universe_ret"])

    summary = {
        "db_path": str(args.db_path),
        "parameters": {
            "top_n": args.top_n,
            "min_close": args.min_close,
            "min_volume": args.min_volume,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "signal": "20d momentum",
            "execution_lag": "1 full bar",
            "realized_return_window": "t+1 to t+2",
        },
        "coverage": coverage,
        "sample": {
            "rows": int(len(daily_df)),
            "min_date": str(daily_df["price_date"].min().date()),
            "max_date": str(daily_df["price_date"].max().date()),
            "avg_long_names": float(daily_df["long_names"].mean()),
            "avg_short_names": float(daily_df["short_names"].mean()),
            "avg_universe_names": float(daily_df["universe_names"].mean()),
        },
        "results": {
            "long_only_top_n": long_metrics,
            "long_minus_short_spread": spread_metrics,
            "equal_weight_universe": universe_metrics,
        },
    }

    summary = to_json_safe(summary)
    steps.update(1)

    # -------------------------------------------------------------------------
    # Étape 5: écrire les artefacts
    # -------------------------------------------------------------------------
    steps.set_description("research_strategy:write_outputs")

    daily_df_out = daily_df.copy()
    daily_df_out["price_date"] = daily_df_out["price_date"].dt.strftime("%Y-%m-%d")
    daily_df_out.to_csv(daily_path, index=False)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print("\n===== RESEARCH STRATEGY SUMMARY =====")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print("\nSaved daily CSV to:", daily_path)
    print("Saved summary JSON to:", summary_path)

    steps.update(1)
    steps.close()
    con.close()


if __name__ == "__main__":
    main()
