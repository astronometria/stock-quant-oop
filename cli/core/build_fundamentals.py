#!/usr/bin/env python3
from __future__ import annotations

"""
SQL-first builder for fundamental_ttm from sec_fact_normalized.

Objectif
--------
Construire une table fundamental_ttm robuste, PIT-safe et exploitable par les
features quotidiennes, à partir de sec_fact_normalized.

Pourquoi cette réécriture
-------------------------
La version précédente:
- passait par une couche service/repository très mince
- n'était pas alignée avec le schéma réel
- n'était pas SQL-first
- n'appliquait pas les réglages runtime lourds de DuckDB

Cette version:
- reste SQL-first
- applique PRAGMA memory_limit / threads / temp_directory
- produit un rebuild complet déterministe
- agrège les faits SEC en TTM par (company_id, cik, period_end_date, available_at)
- reste strictement point-in-time safe grâce à available_at

Important
---------
- On ne crée PAS de colonne symbol ici car le schéma réel fundamental_ttm n'en a pas.
- On ne remplit QUE les colonnes réellement présentes dans fundamental_ttm.
- On privilégie la robustesse et l'explicabilité sur la sophistication.
"""

import argparse
import json
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.fundamentals_schema import FundamentalsSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def _quote_sql_string(value: str) -> str:
    """Escape simple pour injecter une string dans un PRAGMA SQL."""
    return str(value).replace("'", "''")


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _fetch_one_dict(con, sql: str, params: list | None = None) -> dict[str, object]:
    """
    Retourne une seule ligne sous forme dict.
    Très pratique pour les probes et les résumés JSON.
    """
    cursor = con.execute(sql, params or [])
    columns = [str(desc[0]) for desc in cursor.description]
    row = cursor.fetchone()
    if row is None:
        return {col: None for col in columns}
    return {columns[i]: row[i] for i in range(len(columns))}


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
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


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

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        FundamentalsSchemaManager(uow).initialize()

        if uow.connection is None:
            raise RuntimeError("missing active DB connection")

        con = uow.connection

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
            """
        )

        if args.verbose:
            print(
                f"[build_fundamentals] sec_probe="
                f"{json.dumps(sec_probe, default=str, sort_keys=True)}",
                flush=True,
            )

        con.execute("DELETE FROM fundamental_ttm")

        if args.verbose:
            print("[build_fundamentals] cleared fundamental_ttm", flush=True)

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
              AND (
                    concept IN (
                        'Revenues',
                        'RevenueFromContractWithCustomerExcludingAssessedTax',
                        'SalesRevenueNet',
                        'RevenueFromContractWithCustomerIncludingAssessedTax',

                        'NetIncomeLoss',
                        'ProfitLoss',

                        'Assets',
                        'AssetsCurrent',

                        'Liabilities',
                        'LiabilitiesCurrent',
                        'LiabilitiesAndStockholdersEquity',

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
                  )
            """
        )

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
                            'RevenueFromContractWithCustomerIncludingAssessedTax'
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
                        WHEN concept IN (
                            'Liabilities',
                            'LiabilitiesCurrent',
                            'LiabilitiesAndStockholdersEquity'
                        )
                        THEN value_numeric
                    END
                ) AS liabilities,

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
                source_name
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY company_id, cik, period_end_date, available_at
                        ORDER BY source_name DESC NULLS LAST
                    ) AS rn
                FROM tmp_fundamental_base
            ) x
            WHERE rn = 1
            """
        )

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
                    t.source_name AS trailing_source_name,

                    ROW_NUMBER() OVER (
                        PARTITION BY
                            b.company_id,
                            b.cik,
                            b.period_end_date,
                            b.available_at
                        ORDER BY
                            t.period_end_date DESC,
                            t.available_at DESC
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

        inserted_rows = con.execute(
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

        quality = {
            "sec_probe": sec_probe,
            "base_probe": base_probe,
            "ttm_probe": ttm_probe,
            "null_profile": null_profile,
            "top_companies": [
                {"company_id": row[0], "rows": int(row[1])}
                for row in top_companies
            ],
        }

        if args.verbose:
            print(
                f"[build_fundamentals] quality_summary="
                f"{json.dumps(quality, default=str, sort_keys=True)}",
                flush=True,
            )

        output = {
            "status": "SUCCESS",
            "rows_written": int(
                inserted_rows
                if inserted_rows is not None and inserted_rows >= 0
                else int(ttm_probe["rows"] or 0)
            ),
            "metrics": {
                "sec_fact_normalized_rows": int(sec_probe["rows"] or 0),
                "base_rows": int(base_probe["rows"] or 0),
                "fundamental_ttm_rows": int(ttm_probe["rows"] or 0),
                "companies": int(ttm_probe["companies"] or 0),
                "ciks": int(ttm_probe["ciks"] or 0),
            },
            "quality": quality,
        }

        print(json.dumps(output, indent=2, sort_keys=True, default=str), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
