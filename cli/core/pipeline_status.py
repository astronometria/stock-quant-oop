#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# pipeline_status.py
#
# But:
#   - Donner un état compact et lisible de la base DuckDB du pipeline.
#   - Afficher des comptes de tables clés.
#   - Afficher quelques samples bornés pour éviter des payloads énormes.
#
# Règles importantes:
#   - Ce script ne modifie rien.
#   - Il doit être sûr en prod.
#   - Il ne doit pas exploser si une table n'existe pas encore.
#   - Les samples doivent toujours être limités explicitement.
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes:
    - On garde un path par défaut pour la base prod locale.
    - sample-limit est borné au niveau SQL plus tard.
    """
    parser = argparse.ArgumentParser(
        description="Report compact pipeline status for the DuckDB database."
    )
    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop/market.duckdb",
        help="DuckDB path.",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Maximum number of symbols/pairs to include in each sample string.",
    )
    return parser.parse_args()


def safe_scalar(con: duckdb.DuckDBPyConnection, sql: str):
    """
    Exécute une requête scalaire de façon tolérante.

    Pourquoi:
    - En phase d'initialisation, certaines tables peuvent être absentes.
    - On veut un status robuste plutôt qu'une erreur fatale.

    Retour:
    - La valeur scalaire si la requête réussit.
    - None sinon.
    """
    try:
        return con.execute(sql).fetchone()[0]
    except Exception:
        return None


def sample_string(con: duckdb.DuckDBPyConnection, inner_sql: str) -> str:
    """
    Exécute une petite requête de sample et retourne une chaîne compacte.

    Important:
    - inner_sql DOIT déjà contenir son LIMIT.
    - On stringify seulement la première colonne.
    - On évite volontairement tout payload massif dans le status.

    Exemple de sortie:
    - "AAPL, MSFT, NVDA"
    """
    try:
        rows = con.execute(inner_sql).fetchall()
        if not rows:
            return ""
        return ", ".join("" if row[0] is None else str(row[0]) for row in rows)
    except Exception:
        return ""


def main() -> int:
    """
    Point d'entrée principal.

    Étapes:
    1. Résoudre le path de la DB.
    2. Ouvrir la connexion DuckDB.
    3. Calculer les comptes de tables utiles.
    4. Calculer quelques samples bornés.
    5. Imprimer le JSON final.
    """
    args = parse_args()

    # On normalise le chemin pour éviter toute ambiguïté dans les logs/status.
    db_path = str(Path(args.db_path).expanduser().resolve())

    # Ouverture en lecture/écriture standard DuckDB.
    # Ici on ne modifie rien, mais le connect habituel suffit.
    con = duckdb.connect(db_path)

    # Défense simple contre des limites invalides.
    # On force au moins 1 pour éviter un SQL incohérent.
    limit = max(1, int(args.sample_limit))

    # -------------------------------------------------------------------------
    # Comptes de tables
    #
    # Objectif:
    # - Voir rapidement si le pipeline charge du vrai volume ou des fixtures.
    # - Diagnostiquer les trous de pipeline (raw présent mais curated vide, etc.).
    # -------------------------------------------------------------------------
    status = {
        "db_path": db_path,
        "tables": {
            "symbol_reference_source_raw": safe_scalar(
                con, "SELECT COUNT(*) FROM symbol_reference_source_raw"
            ),
            "market_universe": safe_scalar(
                con, "SELECT COUNT(*) FROM market_universe"
            ),
            "market_universe_included": safe_scalar(
                con,
                "SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE",
            ),
            "market_universe_conflicts": safe_scalar(
                con, "SELECT COUNT(*) FROM market_universe_conflicts"
            ),
            "symbol_reference": safe_scalar(
                con, "SELECT COUNT(*) FROM symbol_reference"
            ),
            "price_source_daily_raw_all": safe_scalar(
                con, "SELECT COUNT(*) FROM price_source_daily_raw_all"
            ),
            "price_source_daily_raw_yahoo": safe_scalar(
                con, "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
            ),
            "price_source_daily_raw": safe_scalar(
                con, "SELECT COUNT(*) FROM price_source_daily_raw"
            ),
            "price_history": safe_scalar(
                con, "SELECT COUNT(*) FROM price_history"
            ),
            "price_latest": safe_scalar(
                con, "SELECT COUNT(*) FROM price_latest"
            ),
            "finra_short_interest_source_raw": safe_scalar(
                con, "SELECT COUNT(*) FROM finra_short_interest_source_raw"
            ),
            "finra_short_interest_history": safe_scalar(
                con, "SELECT COUNT(*) FROM finra_short_interest_history"
            ),
            "finra_short_interest_latest": safe_scalar(
                con, "SELECT COUNT(*) FROM finra_short_interest_latest"
            ),
            "finra_short_interest_sources": safe_scalar(
                con, "SELECT COUNT(*) FROM finra_short_interest_sources"
            ),
            "news_source_raw": safe_scalar(
                con, "SELECT COUNT(*) FROM news_source_raw"
            ),
            "news_articles_raw": safe_scalar(
                con, "SELECT COUNT(*) FROM news_articles_raw"
            ),
            "news_symbol_candidates": safe_scalar(
                con, "SELECT COUNT(*) FROM news_symbol_candidates"
            ),
        },
        # ---------------------------------------------------------------------
        # Samples bornés
        #
        # Objectif:
        # - Donner une idée de ce qui est présent sans spammer la console.
        # - Permettre de détecter immédiatement la présence de fixtures.
        # ---------------------------------------------------------------------
        "samples": {
            "market_universe_included_symbols": sample_string(
                con,
                f"""
                SELECT symbol
                FROM market_universe
                WHERE include_in_universe = TRUE
                ORDER BY symbol
                LIMIT {limit}
                """,
            ),
            "price_latest_symbols": sample_string(
                con,
                f"""
                SELECT symbol
                FROM price_latest
                ORDER BY symbol
                LIMIT {limit}
                """,
            ),
            "bronze_price_symbols": sample_string(
                con,
                f"""
                SELECT DISTINCT symbol
                FROM price_source_daily_raw_all
                ORDER BY symbol
                LIMIT {limit}
                """,
            ),
            "yahoo_price_symbols": sample_string(
                con,
                f"""
                SELECT DISTINCT symbol
                FROM price_source_daily_raw_yahoo
                ORDER BY symbol
                LIMIT {limit}
                """,
            ),
            "finra_latest_symbols": sample_string(
                con,
                f"""
                SELECT symbol
                FROM finra_short_interest_latest
                ORDER BY symbol
                LIMIT {limit}
                """,
            ),
            "news_candidate_pairs": sample_string(
                con,
                f"""
                SELECT CAST(raw_id AS VARCHAR) || ':' || symbol
                FROM news_symbol_candidates
                ORDER BY raw_id, symbol
                LIMIT {limit}
                """,
            ),
        },
    }

    # Fermeture explicite propre.
    con.close()

    # Sortie JSON stable et lisible.
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
