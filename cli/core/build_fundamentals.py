#!/usr/bin/env python3
from __future__ import annotations

"""
SQL-first builder for fundamental_ttm from sec_fact_normalized.

Objectif
--------
Construire une table `fundamental_ttm` robuste, PIT-safe, et cohérente avec
le reste du codebase.

Principes de design
-------------------
- SQL-first pour toute la logique lourde de transformation SEC.
- Python mince uniquement pour:
  - l'orchestration,
  - les probes qualité,
  - le fallback ciblé sur yfinance quand SEC ne fournit aucun fondamentaux.
- aucun wrapper legacy,
- aucun faux "service" additionnel juste pour Yahoo,
- nomenclature cohérente avec le repo actuel,
- beaucoup de commentaires pour aider les autres développeurs.

Règles métier importantes
-------------------------
1. SEC reste la source prioritaire.
2. Le fallback yfinance est utilisé UNIQUEMENT pour les CIK présents dans
   `sec_filing` mais absents de `fundamental_ttm`.
3. Si Yahoo ne fournit rien, le pipeline ne doit PAS échouer.
4. On accepte des lignes partielles en fallback, plutôt que d'inventer
   des fondamentaux.
5. `source_name` permet d'identifier explicitement la provenance:
   - 'sec'
   - 'yfinance'
"""

import argparse
import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.fundamentals_schema import FundamentalsSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


# =============================================================================
# Helpers SQL / DuckDB
# =============================================================================
def _quote_sql_string(value: str) -> str:
    """
    Escape simple pour injecter une string dans un PRAGMA SQL.
    """
    return str(value).replace("'", "''")


def _table_exists(con: Any, table_name: str) -> bool:
    """
    Vérifie l'existence d'une table dans DuckDB.
    """
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _fetch_one_dict(con: Any, sql: str, params: list[Any] | None = None) -> dict[str, Any]:
    """
    Retourne une seule ligne sous forme de dictionnaire.

    Très utile pour les probes de qualité et les résumés JSON.
    """
    cursor = con.execute(sql, params or [])
    columns = [str(desc[0]) for desc in cursor.description]
    row = cursor.fetchone()
    if row is None:
        return {col: None for col in columns}
    return {columns[i]: row[i] for i in range(len(columns))}


def _fetch_all_dicts(con: Any, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    """
    Retourne toutes les lignes d'une requête sous forme de liste de dicts.
    """
    cursor = con.execute(sql, params or [])
    columns = [str(desc[0]) for desc in cursor.description]
    rows = cursor.fetchall()
    return [{columns[i]: row[i] for i in range(len(columns))} for row in rows]


# =============================================================================
# Helpers datetime / normalisation légère
# =============================================================================
def _coerce_python_date(value: Any) -> date | None:
    """
    Convertit une valeur arbitraire en `date` Python.

    Supporte:
    - datetime/date Python
    - pandas.Timestamp (via .to_pydatetime())
    - chaînes ISO simples
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    # pandas.Timestamp ou objets similaires
    to_pydatetime = getattr(value, "to_pydatetime", None)
    if callable(to_pydatetime):
        try:
            return to_pydatetime().date()
        except Exception:
            pass

    text = str(value).strip()
    if not text:
        return None

    # Garde seulement la partie date si une heure est présente.
    text = text[:10]
    try:
        return date.fromisoformat(text)
    except Exception:
        return None


def _coerce_float(value: Any) -> float | None:
    """
    Convertit une valeur numérique potentielle en float Python.
    """
    if value is None:
        return None

    try:
        # Évite de transformer les chaînes vides en 0
        text = str(value).strip()
        if text == "" or text.lower() == "nan":
            return None
        result = float(value)
    except Exception:
        return None

    # Les NaN Python/NumPy ne doivent pas être propagés.
    if result != result:
        return None
    return result


def _safe_int(value: Any) -> int:
    """
    Conversion robuste vers int.
    """
    try:
        return int(value)
    except Exception:
        return 0


# =============================================================================
# Fallback yfinance
# =============================================================================
def _pick_first_non_null(mapping: dict[str, Any], candidate_labels: list[str]) -> float | None:
    """
    Retourne la première valeur numérique non nulle trouvée pour une liste de labels.

    `mapping` représente typiquement une colonne d'état financier Yahoo:
    index -> valeur
    """
    for label in candidate_labels:
        if label in mapping:
            numeric_value = _coerce_float(mapping.get(label))
            if numeric_value is not None:
                return numeric_value
    return None


def _series_dict_from_dataframe_column(df: Any, column: Any) -> dict[str, Any]:
    """
    Extrait une colonne d'un DataFrame pandas en dictionnaire {index_label: value}.

    La fonction reste très défensive pour ne pas faire échouer le pipeline si
    Yahoo change légèrement son format.
    """
    if df is None:
        return {}

    try:
        if getattr(df, "empty", True):
            return {}
    except Exception:
        return {}

    try:
        series = df[column]
    except Exception:
        return {}

    result: dict[str, Any] = {}
    try:
        for idx, value in series.items():
            result[str(idx)] = value
    except Exception:
        return {}
    return result


def _extract_yfinance_statement_rows(
    *,
    symbol: str,
    cik: str,
    ticker: Any,
) -> list[tuple[Any, ...]]:
    """
    Transforme les statements Yahoo en lignes compatibles avec `fundamental_ttm`.

    Design volontairement simple:
    - une ligne par colonne de statement (donc généralement annuelle ici),
    - `period_type='TTM'` pour rester compatible avec le schéma actuel,
    - `available_at=CURRENT_TIMESTAMP` côté Python au moment du fetch,
    - `operating_cash_flow` reste NULL si Yahoo ne fournit rien.

    Pourquoi ce compromis:
    - le schéma actuel ne contient qu'une seule table cible `fundamental_ttm`
    - l'objectif du fallback est de combler des trous, pas de reproduire une
      modélisation comptable parfaite
    - on préfère une ligne partielle mais utile à une absence complète
    """
    now_ts = datetime.utcnow()

    # -------------------------------------------------------------------------
    # Récupération défensive des dataframes Yahoo.
    # -------------------------------------------------------------------------
    try:
        balance_sheet_df = ticker.balance_sheet
    except Exception:
        balance_sheet_df = None

    try:
        income_stmt_df = ticker.income_stmt
    except Exception:
        income_stmt_df = None

    try:
        cashflow_df = ticker.cashflow
    except Exception:
        cashflow_df = None

    # Union des colonnes disponibles entre les états.
    statement_columns: list[Any] = []
    for df in [balance_sheet_df, income_stmt_df, cashflow_df]:
        try:
            if df is not None and not df.empty:
                for col in list(df.columns):
                    if col not in statement_columns:
                        statement_columns.append(col)
        except Exception:
            continue

    if not statement_columns:
        return []

    # -------------------------------------------------------------------------
    # Infos rapides Yahoo utiles pour shares_outstanding de fallback.
    # -------------------------------------------------------------------------
    fast_info_shares = None
    try:
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info is not None:
            fast_info_shares = _coerce_float(fast_info.get("shares"))
    except Exception:
        fast_info_shares = None

    info_shares = None
    try:
        info = getattr(ticker, "info", None)
        if isinstance(info, dict):
            info_shares = _coerce_float(info.get("sharesOutstanding"))
    except Exception:
        info_shares = None

    rows: list[tuple[Any, ...]] = []

    for column in statement_columns:
        period_end_date = _coerce_python_date(column)
        if period_end_date is None:
            # Si Yahoo renvoie une colonne non-datable, on l'ignore.
            continue

        balance_map = _series_dict_from_dataframe_column(balance_sheet_df, column)
        income_map = _series_dict_from_dataframe_column(income_stmt_df, column)
        cashflow_map = _series_dict_from_dataframe_column(cashflow_df, column)

        assets = _pick_first_non_null(
            balance_map,
            [
                "Total Assets",
            ],
        )

        liabilities = _pick_first_non_null(
            balance_map,
            [
                "Total Liabilities Net Minority Interest",
            ],
        )

        equity = _pick_first_non_null(
            balance_map,
            [
                "Stockholders Equity",
                "Common Stock Equity",
                "Total Equity Gross Minority Interest",
            ],
        )

        # Si liabilities manque mais assets/equity existent, on applique le
        # même principe que le build SEC: liabilities = assets - equity.
        if liabilities is None and assets is not None and equity is not None:
            liabilities = assets - equity

        net_income = _pick_first_non_null(
            income_map,
            [
                "Net Income",
                "Net Income Common Stockholders",
                "Net Income From Continuing Operation Net Minority Interest",
                "Net Income Continuous Operations",
                "Normalized Income",
            ],
        )

        revenue = _pick_first_non_null(
            income_map,
            [
                "Total Revenue",
                "Operating Revenue",
                "Net Interest Income",
                "Interest Income",
            ],
        )

        operating_cash_flow = _pick_first_non_null(
            cashflow_map,
            [
                "Operating Cash Flow",
                "Cash Flow From Continuing Operating Activities",
                "Net Cash Provided By Operating Activities",
                "Net Cash From Operating Activities",
            ],
        )

        shares_outstanding = _pick_first_non_null(
            balance_map,
            [
                "Ordinary Shares Number",
                "Share Issued",
            ],
        )
        if shares_outstanding is None:
            shares_outstanding = fast_info_shares
        if shares_outstanding is None:
            shares_outstanding = info_shares

        # Si absolument aucune mesure utile n'est disponible pour cette date,
        # on ne crée pas de ligne vide.
        if (
            assets is None
            and liabilities is None
            and equity is None
            and net_income is None
            and revenue is None
            and operating_cash_flow is None
            and shares_outstanding is None
        ):
            continue

        rows.append(
            (
                cik,                    # company_id
                cik,                    # cik
                "TTM",                  # period_type
                period_end_date,        # period_end_date
                now_ts,                 # available_at
                revenue,                # revenue
                net_income,             # net_income
                assets,                 # assets
                liabilities,            # liabilities
                equity,                 # equity
                operating_cash_flow,    # operating_cash_flow
                shares_outstanding,     # shares_outstanding
                "yfinance",             # source_name
                now_ts,                 # created_at
            )
        )

    return rows


def _select_missing_fundamental_symbols(con: Any) -> list[dict[str, Any]]:
    """
    Sélectionne les CIK présents dans `sec_filing` mais absents de `fundamental_ttm`,
    puis leur associe un symbole depuis `symbol_reference`.

    La table `symbol_reference` du user ne contient pas `is_active`, donc on reste
    compatible avec son schéma réel.

    Heuristique de sélection du symbole:
    - priorité aux lignes `symbol_match_enabled = TRUE`
    - puis `name_match_enabled = TRUE`
    - puis symboles sans suffixe de classe (`-PD`, `-PE`, etc.)
    - puis ordre alphabétique stable
    """
    return _fetch_all_dicts(
        con,
        """
        WITH filing_ciks AS (
            SELECT DISTINCT
                TRIM(CAST(cik AS VARCHAR)) AS cik
            FROM sec_filing
            WHERE cik IS NOT NULL
              AND TRIM(CAST(cik AS VARCHAR)) <> ''
        ),
        covered_ciks AS (
            SELECT DISTINCT
                TRIM(CAST(cik AS VARCHAR)) AS cik
            FROM fundamental_ttm
            WHERE cik IS NOT NULL
              AND TRIM(CAST(cik AS VARCHAR)) <> ''
        ),
        missing_ciks AS (
            SELECT f.cik
            FROM filing_ciks f
            LEFT JOIN covered_ciks c
              ON c.cik = f.cik
            WHERE c.cik IS NULL
        ),
        symbol_candidates AS (
            SELECT
                TRIM(CAST(cik AS VARCHAR)) AS cik,
                TRIM(CAST(symbol AS VARCHAR)) AS symbol,
                exchange,
                symbol_match_enabled,
                name_match_enabled,
                ROW_NUMBER() OVER (
                    PARTITION BY TRIM(CAST(cik AS VARCHAR))
                    ORDER BY
                        CASE WHEN COALESCE(symbol_match_enabled, FALSE) THEN 0 ELSE 1 END,
                        CASE WHEN COALESCE(name_match_enabled, FALSE) THEN 0 ELSE 1 END,
                        CASE WHEN POSITION('-' IN TRIM(CAST(symbol AS VARCHAR))) = 0 THEN 0 ELSE 1 END,
                        TRIM(CAST(symbol AS VARCHAR))
                ) AS rn
            FROM symbol_reference
            WHERE cik IS NOT NULL
              AND TRIM(CAST(cik AS VARCHAR)) <> ''
              AND symbol IS NOT NULL
              AND TRIM(CAST(symbol AS VARCHAR)) <> ''
        )
        SELECT
            m.cik,
            s.symbol,
            s.exchange
        FROM missing_ciks m
        LEFT JOIN symbol_candidates s
          ON s.cik = m.cik
         AND s.rn = 1
        ORDER BY m.cik
        """,
    )


def _insert_yfinance_fallback_rows(con: Any, rows: list[tuple[Any, ...]]) -> int:
    """
    Insère les lignes fallback Yahoo dans `fundamental_ttm`.

    L'insertion reste volontairement simple:
    - pas d'upsert complexe,
    - on n'appelle cette étape que pour les CIK absents de `fundamental_ttm`,
      donc le risque de doublon logique est déjà très réduit.
    """
    if not rows:
        return 0

    con.executemany(
        """
        INSERT INTO fundamental_ttm (
            company_id,
            cik,
            period_type,
            period_end_date,
            available_at,
            revenue,
            net_income,
            assets,
            liabilities,
            equity,
            operating_cash_flow,
            shares_outstanding,
            source_name,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    return len(rows)


def _load_yfinance_fallback_rows(
    con: Any,
    *,
    verbose: bool,
) -> tuple[int, dict[str, Any]]:
    """
    Construit et insère les lignes fallback yfinance pour les CIK manquants.

    Retour:
    - rows_written
    - quality dict
    """
    missing_symbols = _select_missing_fundamental_symbols(con)

    quality: dict[str, Any] = {
        "candidate_symbols": missing_symbols,
        "symbols_attempted": 0,
        "symbols_succeeded": 0,
        "symbols_empty": 0,
        "symbols_missing_mapping": 0,
        "rows_written": 0,
        "empty_symbols": [],
        "successful_symbols": [],
        "missing_mapping_ciks": [],
    }

    if not missing_symbols:
        return 0, quality

    try:
        import yfinance as yf
    except Exception as exc:
        quality["yfinance_import_error"] = str(exc)
        return 0, quality

    all_rows: list[tuple[Any, ...]] = []

    for item in missing_symbols:
        cik = str(item.get("cik") or "").strip()
        symbol = str(item.get("symbol") or "").strip()

        if not cik:
            continue

        if not symbol:
            quality["symbols_missing_mapping"] = _safe_int(quality["symbols_missing_mapping"]) + 1
            quality["missing_mapping_ciks"].append(cik)
            continue

        quality["symbols_attempted"] = _safe_int(quality["symbols_attempted"]) + 1

        try:
            ticker = yf.Ticker(symbol)
            symbol_rows = _extract_yfinance_statement_rows(symbol=symbol, cik=cik, ticker=ticker)
        except Exception as exc:
            symbol_rows = []
            if verbose:
                print(
                    f"[build_fundamentals] yfinance_error="
                    f"{json.dumps({'cik': cik, 'symbol': symbol, 'error': str(exc)}, ensure_ascii=False, default=str, sort_keys=True)}",
                    flush=True,
                )

        if symbol_rows:
            all_rows.extend(symbol_rows)
            quality["symbols_succeeded"] = _safe_int(quality["symbols_succeeded"]) + 1
            quality["successful_symbols"].append({"cik": cik, "symbol": symbol, "rows": len(symbol_rows)})
        else:
            quality["symbols_empty"] = _safe_int(quality["symbols_empty"]) + 1
            quality["empty_symbols"].append({"cik": cik, "symbol": symbol})

    rows_written = _insert_yfinance_fallback_rows(con, all_rows)
    quality["rows_written"] = rows_written
    return rows_written, quality


# =============================================================================
# Arguments CLI
# =============================================================================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build fundamental_ttm from sec_fact_normalized using SQL-first transformations."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--memory-limit",
        default="24GB",
        help="DuckDB memory_limit pragma, e.g. 8GB, 24GB.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=6,
        help="DuckDB worker threads.",
    )
    parser.add_argument(
        "--temp-dir",
        default="/home/marty/stock-quant-oop/tmp",
        help="DuckDB temp directory for disk spill.",
    )
    parser.add_argument(
        "--min-available-date",
        default="1990-01-01",
        help="Lower bound for available_at::date kept in the build.",
    )
    parser.add_argument(
        "--max-available-date",
        default="2100-12-31",
        help="Upper bound for available_at::date kept in the build.",
    )
    parser.add_argument(
        "--min-period-end-date",
        default="1990-01-01",
        help="Lower bound for period_end_date kept in the build.",
    )
    parser.add_argument(
        "--max-period-end-date",
        default="2100-12-31",
        help="Upper bound for period_end_date kept in the build.",
    )
    parser.add_argument(
        "--quarter-lookback-days",
        type=int,
        default=370,
        help=(
            "Window length used to aggregate trailing 12 months from quarterly facts. "
            "A slightly loose bound helps absorb calendar irregularities."
        ),
    )
    parser.add_argument(
        "--skip-yfinance-fallback",
        action="store_true",
        help="Disable targeted yfinance fallback for CIKs still missing fundamentals after SEC build.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


# =============================================================================
# Main
# =============================================================================
def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    temp_dir_path = Path(args.temp_dir).expanduser().resolve()
    temp_dir_path.mkdir(parents=True, exist_ok=True)

    if args.verbose:
        print(f"[build_fundamentals] project_root={config.project_root}", flush=True)
        print(f"[build_fundamentals] db_path={config.db_path}", flush=True)
        print(f"[build_fundamentals] memory_limit={args.memory_limit}", flush=True)
        print(f"[build_fundamentals] threads={args.threads}", flush=True)
        print(f"[build_fundamentals] temp_dir={temp_dir_path}", flush=True)
        print(
            f"[build_fundamentals] available_date_range="
            f"{args.min_available_date} -> {args.max_available_date}",
            flush=True,
        )
        print(
            f"[build_fundamentals] period_end_date_range="
            f"{args.min_period_end_date} -> {args.max_period_end_date}",
            flush=True,
        )
        print(
            f"[build_fundamentals] quarter_lookback_days={args.quarter_lookback_days}",
            flush=True,
        )
        print(
            f"[build_fundamentals] skip_yfinance_fallback={bool(args.skip_yfinance_fallback)}",
            flush=True,
        )

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        FundamentalsSchemaManager(uow).initialize()

        if uow.connection is None:
            raise RuntimeError("missing active DB connection")

        con = uow.connection

        # ---------------------------------------------------------------------
        # Runtime DuckDB
        # ---------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{_quote_sql_string(args.memory_limit)}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{_quote_sql_string(str(temp_dir_path))}'")

        if not _table_exists(con, "sec_fact_normalized"):
            raise RuntimeError("required table sec_fact_normalized does not exist")

        if not _table_exists(con, "fundamental_ttm"):
            raise RuntimeError("required table fundamental_ttm does not exist after schema init")

        sec_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT company_id) AS companies,
                COUNT(DISTINCT cik) AS ciks,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date,
                MIN(CAST(available_at AS DATE)) AS min_available_date,
                MAX(CAST(available_at AS DATE)) AS max_available_date
            FROM sec_fact_normalized
            """,
        )

        if args.verbose:
            print(
                f"[build_fundamentals] sec_probe="
                f"{json.dumps(sec_probe, default=str, sort_keys=True)}",
                flush=True,
            )

        # ---------------------------------------------------------------------
        # Rebuild déterministe
        # ---------------------------------------------------------------------
        con.execute("DELETE FROM fundamental_ttm")
        if args.verbose:
            print("[build_fundamentals] cleared fundamental_ttm", flush=True)

        # ---------------------------------------------------------------------
        # Scope de travail SEC
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_sec_fact_scope")
        con.execute(
            """
            CREATE TEMP TABLE tmp_sec_fact_scope AS
            SELECT
                company_id,
                cik,
                LOWER(TRIM(CAST(taxonomy AS VARCHAR))) AS taxonomy,
                TRIM(CAST(concept AS VARCHAR)) AS concept,
                period_end_date,
                available_at,
                value_numeric,
                value_text,
                source_name
            FROM sec_fact_normalized
            WHERE company_id IS NOT NULL
              AND TRIM(CAST(company_id AS VARCHAR)) <> ''
              AND cik IS NOT NULL
              AND TRIM(CAST(cik AS VARCHAR)) <> ''
              AND concept IS NOT NULL
              AND TRIM(CAST(concept AS VARCHAR)) <> ''
              AND period_end_date IS NOT NULL
              AND period_end_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
              AND available_at IS NOT NULL
              AND CAST(available_at AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
            """,
            [
                args.min_period_end_date,
                args.max_period_end_date,
                args.min_available_date,
                args.max_available_date,
            ],
        )

        # ---------------------------------------------------------------------
        # Faits numériques utiles pour les fondamentaux.
        # On reste volontairement explicite sur les concepts retenus.
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_sec_fact_numeric")
        con.execute(
            """
            CREATE TEMP TABLE tmp_sec_fact_numeric AS
            SELECT
                company_id,
                cik,
                taxonomy,
                concept,
                period_end_date,
                available_at,
                value_numeric,
                source_name
            FROM tmp_sec_fact_scope
            WHERE value_numeric IS NOT NULL
              AND concept IN (
                    'Revenues',
                    'RevenueFromContractWithCustomerExcludingAssessedTax',
                    'SalesRevenueNet',
                    'RevenueFromContractWithCustomerIncludingAssessedTax',
                    'TotalRevenue',
                    'OperatingRevenue',

                    'NetIncomeLoss',
                    'ProfitLoss',

                    'Assets',
                    'AssetsCurrent',

                    'Liabilities',
                    'LiabilitiesCurrent',

                    'StockholdersEquity',
                    'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                    'Equity',

                    'NetCashProvidedByUsedInOperatingActivities',
                    'NetCashFlowsProvidedByUsedInOperatingActivities',
                    'NetCashProvidedByUsedInContinuingOperations',

                    'EntityCommonStockSharesOutstanding',
                    'CommonStockSharesOutstanding',
                    'WeightedAverageNumberOfSharesOutstandingBasic',
                    'WeightedAverageNumberOfDilutedSharesOutstanding'
              )
            """
        )

        # ---------------------------------------------------------------------
        # Base par (company_id, cik, period_end_date, available_at)
        #
        # Important:
        # - on NE mappe PAS liabilities sur LiabilitiesAndStockholdersEquity
        #   car cela crée des biais majeurs
        # - un fallback plus sûr est assets - equity quand assets/equity existent
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_fundamental_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_base AS
            SELECT
                company_id,
                cik,
                period_end_date,
                available_at,
                MAX(source_name) AS source_name,

                MAX(
                    CASE
                        WHEN concept IN (
                            'Revenues',
                            'RevenueFromContractWithCustomerExcludingAssessedTax',
                            'SalesRevenueNet',
                            'RevenueFromContractWithCustomerIncludingAssessedTax',
                            'TotalRevenue',
                            'OperatingRevenue'
                        )
                        THEN value_numeric
                    END
                ) AS revenue,

                MAX(
                    CASE
                        WHEN concept IN ('NetIncomeLoss', 'ProfitLoss')
                        THEN value_numeric
                    END
                ) AS net_income,

                MAX(
                    CASE
                        WHEN concept IN ('Assets', 'AssetsCurrent')
                        THEN value_numeric
                    END
                ) AS assets,

                MAX(
                    CASE
                        WHEN concept IN ('Liabilities', 'LiabilitiesCurrent')
                        THEN value_numeric
                    END
                ) AS liabilities_raw,

                MAX(
                    CASE
                        WHEN concept IN (
                            'StockholdersEquity',
                            'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
                            'Equity'
                        )
                        THEN value_numeric
                    END
                ) AS equity,

                MAX(
                    CASE
                        WHEN concept IN (
                            'NetCashProvidedByUsedInOperatingActivities',
                            'NetCashFlowsProvidedByUsedInOperatingActivities',
                            'NetCashProvidedByUsedInContinuingOperations'
                        )
                        THEN value_numeric
                    END
                ) AS operating_cash_flow,

                MAX(
                    CASE
                        WHEN concept IN (
                            'EntityCommonStockSharesOutstanding',
                            'CommonStockSharesOutstanding',
                            'WeightedAverageNumberOfSharesOutstandingBasic',
                            'WeightedAverageNumberOfDilutedSharesOutstanding'
                        )
                        THEN value_numeric
                    END
                ) AS shares_outstanding

            FROM tmp_sec_fact_numeric
            GROUP BY
                company_id,
                cik,
                period_end_date,
                available_at
            """
        )

        # ---------------------------------------------------------------------
        # Fallback liabilities = assets - equity quand la valeur brute de liabilities
        # n'existe pas mais qu'assets et equity existent.
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_fundamental_base_fixed")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_base_fixed AS
            SELECT
                company_id,
                cik,
                period_end_date,
                available_at,
                source_name,
                revenue,
                net_income,
                assets,
                CASE
                    WHEN liabilities_raw IS NOT NULL THEN liabilities_raw
                    WHEN liabilities_raw IS NULL
                         AND assets IS NOT NULL
                         AND equity IS NOT NULL
                    THEN assets - equity
                    ELSE NULL
                END AS liabilities,
                equity,
                operating_cash_flow,
                shares_outstanding,
                CASE
                    WHEN liabilities_raw IS NULL
                         AND assets IS NOT NULL
                         AND equity IS NOT NULL
                    THEN 1
                    ELSE 0
                END AS used_assets_minus_equity_fallback
            FROM tmp_fundamental_base
            """
        )

        # ---------------------------------------------------------------------
        # Dédup légère.
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_fundamental_base_dedup")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_base_dedup AS
            SELECT
                company_id,
                cik,
                period_end_date,
                available_at,
                revenue,
                net_income,
                assets,
                liabilities,
                equity,
                operating_cash_flow,
                shares_outstanding,
                source_name,
                used_assets_minus_equity_fallback
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY company_id, cik, period_end_date, available_at
                        ORDER BY source_name DESC NULLS LAST
                    ) AS rn
                FROM tmp_fundamental_base_fixed
            ) x
            WHERE rn = 1
            """
        )

        # ---------------------------------------------------------------------
        # Construction TTM.
        #
        # Le nom du CTE reste ttm_window pour éviter le problème de parser déjà vu
        # avec certains alias.
        # ---------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_fundamental_ttm")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_fundamental_ttm AS
            WITH ttm_window AS (
                SELECT
                    b.company_id,
                    b.cik,
                    b.period_end_date,
                    b.available_at,
                    b.source_name AS base_source_name,

                    t.period_end_date AS trailing_period_end_date,
                    t.available_at AS trailing_available_at,

                    t.revenue,
                    t.net_income,
                    t.assets,
                    t.liabilities,
                    t.equity,
                    t.operating_cash_flow,
                    t.shares_outstanding,
                    t.used_assets_minus_equity_fallback,

                    ROW_NUMBER() OVER (
                        PARTITION BY b.company_id, b.cik, b.period_end_date, b.available_at
                        ORDER BY t.period_end_date DESC, t.available_at DESC
                    ) AS rn_desc
                FROM tmp_fundamental_base_dedup b
                JOIN tmp_fundamental_base_dedup t
                  ON t.company_id = b.company_id
                 AND t.cik = b.cik
                 AND t.available_at <= b.available_at
                 AND t.period_end_date <= b.period_end_date
                 AND t.period_end_date > b.period_end_date - INTERVAL '{int(args.quarter_lookback_days)} days'
            )
            SELECT
                company_id,
                cik,
                'TTM' AS period_type,
                period_end_date,
                available_at,

                SUM(revenue) AS revenue,
                SUM(net_income) AS net_income,

                MAX(CASE WHEN rn_desc = 1 THEN assets END) AS assets,
                MAX(CASE WHEN rn_desc = 1 THEN liabilities END) AS liabilities,
                MAX(CASE WHEN rn_desc = 1 THEN equity END) AS equity,

                SUM(operating_cash_flow) AS operating_cash_flow,
                MAX(CASE WHEN rn_desc = 1 THEN shares_outstanding END) AS shares_outstanding,

                MAX(base_source_name) AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ttm_window
            GROUP BY
                company_id,
                cik,
                period_end_date,
                available_at
            """
        )

        # ---------------------------------------------------------------------
        # Chargement SEC -> fundamental_ttm
        # ---------------------------------------------------------------------
        inserted_sec_rows = con.execute(
            """
            INSERT INTO fundamental_ttm (
                company_id,
                cik,
                period_type,
                period_end_date,
                available_at,
                revenue,
                net_income,
                assets,
                liabilities,
                equity,
                operating_cash_flow,
                shares_outstanding,
                source_name,
                created_at
            )
            SELECT
                company_id,
                cik,
                period_type,
                period_end_date,
                available_at,
                revenue,
                net_income,
                assets,
                liabilities,
                equity,
                operating_cash_flow,
                shares_outstanding,
                source_name,
                created_at
            FROM tmp_fundamental_ttm
            """
        ).rowcount

        # ---------------------------------------------------------------------
        # Probes après chargement SEC
        # ---------------------------------------------------------------------
        base_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT company_id) AS companies,
                COUNT(DISTINCT cik) AS ciks,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date,
                MIN(CAST(available_at AS DATE)) AS min_available_date,
                MAX(CAST(available_at AS DATE)) AS max_available_date
            FROM tmp_fundamental_base_dedup
            """
        )

        liabilities_fallback_probe = _fetch_one_dict(
            con,
            """
            SELECT
                SUM(used_assets_minus_equity_fallback) AS rows_using_assets_minus_equity_fallback
            FROM tmp_fundamental_base_dedup
            """
        )

        # ---------------------------------------------------------------------
        # Fallback Yahoo ciblé
        # ---------------------------------------------------------------------
        yfinance_rows_written = 0
        yfinance_quality: dict[str, Any] = {
            "candidate_symbols": [],
            "symbols_attempted": 0,
            "symbols_succeeded": 0,
            "symbols_empty": 0,
            "symbols_missing_mapping": 0,
            "rows_written": 0,
            "empty_symbols": [],
            "successful_symbols": [],
            "missing_mapping_ciks": [],
        }

        if not args.skip_yfinance_fallback:
            yfinance_rows_written, yfinance_quality = _load_yfinance_fallback_rows(
                con,
                verbose=args.verbose,
            )
            if args.verbose:
                print(
                    f"[build_fundamentals] yfinance_fallback_summary="
                    f"{json.dumps(yfinance_quality, default=str, sort_keys=True)}",
                    flush=True,
                )

        # ---------------------------------------------------------------------
        # Probes finales
        # ---------------------------------------------------------------------
        ttm_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT company_id) AS companies,
                COUNT(DISTINCT cik) AS ciks,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date,
                MIN(CAST(available_at AS DATE)) AS min_available_date,
                MAX(CAST(available_at AS DATE)) AS max_available_date
            FROM fundamental_ttm
            """
        )

        null_profile = _fetch_one_dict(
            con,
            """
            SELECT
                SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS revenue_null_rows,
                SUM(CASE WHEN net_income IS NULL THEN 1 ELSE 0 END) AS net_income_null_rows,
                SUM(CASE WHEN assets IS NULL THEN 1 ELSE 0 END) AS assets_null_rows,
                SUM(CASE WHEN liabilities IS NULL THEN 1 ELSE 0 END) AS liabilities_null_rows,
                SUM(CASE WHEN equity IS NULL THEN 1 ELSE 0 END) AS equity_null_rows,
                SUM(CASE WHEN operating_cash_flow IS NULL THEN 1 ELSE 0 END) AS operating_cash_flow_null_rows,
                SUM(CASE WHEN shares_outstanding IS NULL THEN 1 ELSE 0 END) AS shares_outstanding_null_rows
            FROM fundamental_ttm
            """
        )

        suspicious_balance_probe = _fetch_one_dict(
            con,
            """
            SELECT
                SUM(
                    CASE
                        WHEN assets IS NOT NULL
                         AND liabilities IS NOT NULL
                         AND assets = liabilities
                        THEN 1 ELSE 0
                    END
                ) AS suspicious_assets_equal_liabilities_rows
            FROM fundamental_ttm
            """
        )

        top_companies = con.execute(
            """
            SELECT
                company_id,
                COUNT(*) AS rows
            FROM fundamental_ttm
            GROUP BY company_id
            ORDER BY rows DESC, company_id
            LIMIT 10
            """
        ).fetchall()

        no_fundamentals_available_symbols = _fetch_all_dicts(
            con,
            """
            WITH filing_ciks AS (
                SELECT DISTINCT
                    TRIM(CAST(cik AS VARCHAR)) AS cik
                FROM sec_filing
                WHERE cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
            ),
            covered_ciks AS (
                SELECT DISTINCT
                    TRIM(CAST(cik AS VARCHAR)) AS cik
                FROM fundamental_ttm
                WHERE cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
            ),
            missing_ciks AS (
                SELECT f.cik
                FROM filing_ciks f
                LEFT JOIN covered_ciks c
                  ON c.cik = f.cik
                WHERE c.cik IS NULL
            ),
            symbol_candidates AS (
                SELECT
                    TRIM(CAST(cik AS VARCHAR)) AS cik,
                    TRIM(CAST(symbol AS VARCHAR)) AS symbol,
                    ROW_NUMBER() OVER (
                        PARTITION BY TRIM(CAST(cik AS VARCHAR))
                        ORDER BY
                            CASE WHEN COALESCE(symbol_match_enabled, FALSE) THEN 0 ELSE 1 END,
                            CASE WHEN COALESCE(name_match_enabled, FALSE) THEN 0 ELSE 1 END,
                            CASE WHEN POSITION('-' IN TRIM(CAST(symbol AS VARCHAR))) = 0 THEN 0 ELSE 1 END,
                            TRIM(CAST(symbol AS VARCHAR))
                    ) AS rn
                FROM symbol_reference
                WHERE cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
                  AND symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            )
            SELECT
                m.cik,
                s.symbol
            FROM missing_ciks m
            LEFT JOIN symbol_candidates s
              ON s.cik = m.cik
             AND s.rn = 1
            ORDER BY m.cik
            """
        )

        quality = {
            "sec_probe": sec_probe,
            "base_probe": base_probe,
            "ttm_probe": ttm_probe,
            "null_profile": null_profile,
            "liabilities_fallback_probe": liabilities_fallback_probe,
            "suspicious_balance_probe": suspicious_balance_probe,
            "top_companies": [
                {"company_id": row[0], "rows": int(row[1])}
                for row in top_companies
            ],
            "yfinance_fallback_probe": yfinance_quality,
            "no_fundamentals_available_symbols": no_fundamentals_available_symbols,
        }

        if args.verbose:
            print(
                f"[build_fundamentals] quality_summary="
                f"{json.dumps(quality, default=str, sort_keys=True)}",
                flush=True,
            )

        sec_rows_written_int = (
            int(inserted_sec_rows)
            if inserted_sec_rows is not None and inserted_sec_rows >= 0
            else int(base_probe["rows"] or 0)
        )

        output = {
            "status": "SUCCESS",
            "rows_written": int(ttm_probe["rows"] or 0),
            "metrics": {
                "sec_fact_normalized_rows": int(sec_probe["rows"] or 0),
                "base_rows": int(base_probe["rows"] or 0),
                "fundamental_ttm_rows": int(ttm_probe["rows"] or 0),
                "companies": int(ttm_probe["companies"] or 0),
                "ciks": int(ttm_probe["ciks"] or 0),
                "sec_rows_written": sec_rows_written_int,
                "yfinance_fallback_rows_written": int(yfinance_rows_written or 0),
                "yfinance_fallback_symbols_attempted": int(yfinance_quality.get("symbols_attempted") or 0),
                "yfinance_fallback_symbols_succeeded": int(yfinance_quality.get("symbols_succeeded") or 0),
                "yfinance_fallback_symbols_empty": int(yfinance_quality.get("symbols_empty") or 0),
                "yfinance_fallback_symbols_missing_mapping": int(yfinance_quality.get("symbols_missing_mapping") or 0),
                "rows_using_assets_minus_equity_fallback": int(
                    liabilities_fallback_probe.get("rows_using_assets_minus_equity_fallback") or 0
                ),
                "suspicious_assets_equal_liabilities_rows": int(
                    suspicious_balance_probe.get("suspicious_assets_equal_liabilities_rows") or 0
                ),
                "no_fundamentals_available_symbols": len(no_fundamentals_available_symbols),
            },
            "quality": quality,
        }

        print(json.dumps(output, indent=2, sort_keys=True, default=str), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
