#!/usr/bin/env python3
from __future__ import annotations

"""
SQL-first builder for sec_fact_normalized with visible chunk progress.

Objectif
--------
Construire `sec_fact_normalized` à partir de `sec_xbrl_fact_raw` en gardant:
- une logique SQL-first
- un full refresh déterministe
- une progression visible dans le terminal
- un découpage par mois de `period_end_date`

Important
---------
Ce script est volontairement aligné sur le schéma réel local de
`sec_fact_normalized`, qui contient actuellement seulement:

- filing_id
- company_id
- cik
- taxonomy
- concept
- period_end_date
- unit
- value_text
- value_numeric
- available_at
- source_name
- created_at

Donc:
- on NE tente PAS d'insérer de `fact_id`
- on NE tente PAS d'insérer accession_number / frame / fiscal_year / etc.
- on garde un full refresh déterministe, mais chunké par mois pour voir la progression

Notes PIT
---------
- `available_at` prioritaire depuis `sec_filing.available_at` si disponible
- sinon fallback technique sur `ingested_at` du raw
- on n'utilise jamais `period_end_date` comme date de publication
"""

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


# =============================================================================
# Helpers
# =============================================================================
def _quote_sql_string(value: str) -> str:
    return str(value).replace("'", "''")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build sec_fact_normalized from sec_xbrl_fact_raw with chunked monthly progress."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--memory-limit", default="24GB", help="DuckDB memory_limit pragma, e.g. 8GB, 24GB.")
    parser.add_argument("--threads", type=int, default=6, help="DuckDB worker threads.")
    parser.add_argument(
        "--temp-dir",
        default="/home/marty/stock-quant-oop/tmp",
        help="DuckDB temp directory for disk spill.",
    )
    parser.add_argument(
        "--min-period-end-date",
        default="1990-01-01",
        help="Lower bound for accepted period_end_date values.",
    )
    parser.add_argument(
        "--max-period-end-date",
        default="2100-12-31",
        help="Upper bound for accepted period_end_date values.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Conservé pour compatibilité. Le build reste un full refresh déterministe.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _table_exists(con: Any, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _column_exists(con: Any, table_name: str, column_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE lower(table_name) = lower(?)
          AND lower(column_name) = lower(?)
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _fetch_one_dict(con: Any, sql: str, params: list[Any] | None = None) -> dict[str, object]:
    cursor = con.execute(sql, params or [])
    columns = [str(desc[0]) for desc in cursor.description]
    row = cursor.fetchone()
    if row is None:
        return {col: None for col in columns}
    return {columns[i]: row[i] for i in range(len(columns))}


def _fetch_month_starts(con: Any, min_date: str, max_date: str) -> list[Any]:
    rows = con.execute(
        """
        SELECT DISTINCT date_trunc('month', period_end_date)::DATE AS month_start
        FROM sec_xbrl_fact_raw
        WHERE period_end_date IS NOT NULL
          AND period_end_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
        ORDER BY month_start
        """,
        [min_date, max_date],
    ).fetchall()
    return [row[0] for row in rows]


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
        print(f"[build_sec_fact_normalized] project_root={config.project_root}", flush=True)
        print(f"[build_sec_fact_normalized] db_path={config.db_path}", flush=True)
        print(f"[build_sec_fact_normalized] memory_limit={args.memory_limit}", flush=True)
        print(f"[build_sec_fact_normalized] threads={args.threads}", flush=True)
        print(f"[build_sec_fact_normalized] temp_dir={temp_dir_path}", flush=True)
        print(
            f"[build_sec_fact_normalized] period_end_date_range="
            f"{args.min_period_end_date} -> {args.max_period_end_date}",
            flush=True,
        )

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

        if uow.connection is None:
            raise RuntimeError("missing active DB connection")
        con = uow.connection

        # ------------------------------------------------------------------
        # Runtime PRAGMA
        # ------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{_quote_sql_string(args.memory_limit)}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{_quote_sql_string(str(temp_dir_path))}'")

        # ------------------------------------------------------------------
        # Préflight
        # ------------------------------------------------------------------
        if not _table_exists(con, "sec_xbrl_fact_raw"):
            raise RuntimeError("required table sec_xbrl_fact_raw does not exist")
        if not _table_exists(con, "sec_fact_normalized"):
            raise RuntimeError("required table sec_fact_normalized does not exist after schema init")
        if not _table_exists(con, "sec_filing"):
            raise RuntimeError("required table sec_filing does not exist")

        raw_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT cik) AS ciks,
                COUNT(DISTINCT taxonomy || '|' || concept) AS concepts,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date
            FROM sec_xbrl_fact_raw
            """,
        )
        raw_bad_probe = _fetch_one_dict(
            con,
            """
            SELECT
                SUM(CASE WHEN period_end_date IS NULL THEN 1 ELSE 0 END) AS null_period_end_date_rows,
                SUM(CASE WHEN period_end_date < CAST(? AS DATE) THEN 1 ELSE 0 END) AS too_old_period_end_date_rows,
                SUM(CASE WHEN period_end_date > CAST(? AS DATE) THEN 1 ELSE 0 END) AS too_future_period_end_date_rows,
                SUM(
                    CASE
                        WHEN value_numeric IS NULL
                         AND (value_text IS NULL OR TRIM(CAST(value_text AS VARCHAR)) = '')
                        THEN 1
                        ELSE 0
                    END
                ) AS no_value_rows
            FROM sec_xbrl_fact_raw
            """,
            [args.min_period_end_date, args.max_period_end_date],
        )

        if args.verbose:
            print(
                f"[build_sec_fact_normalized] raw_probe={json.dumps(raw_probe, default=str, sort_keys=True)}",
                flush=True,
            )
            print(
                f"[build_sec_fact_normalized] raw_bad_probe={json.dumps(raw_bad_probe, default=str, sort_keys=True)}",
                flush=True,
            )

        # ------------------------------------------------------------------
        # Full refresh déterministe, avec insert chunké.
        # ------------------------------------------------------------------
        con.execute("DELETE FROM sec_fact_normalized")
        if args.verbose:
            print("[build_sec_fact_normalized] cleared sec_fact_normalized", flush=True)

        # ------------------------------------------------------------------
        # Map CIK -> company_id effectif
        # - si symbol_reference expose company_id, on l'utilise
        # - sinon fallback robuste company_id = cik normalisé
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_sec_company_map")

        if _table_exists(con, "symbol_reference") and _column_exists(con, "symbol_reference", "cik"):
            has_company_id = _column_exists(con, "symbol_reference", "company_id")

            if has_company_id:
                con.execute(
                    """
                    CREATE TEMP TABLE tmp_sec_company_map AS
                    SELECT
                        LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik,
                        TRIM(CAST(company_id AS VARCHAR)) AS company_id
                    FROM (
                        SELECT
                            cik,
                            company_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0')
                                ORDER BY TRIM(CAST(company_id AS VARCHAR))
                            ) AS rn
                        FROM symbol_reference
                        WHERE cik IS NOT NULL
                          AND TRIM(CAST(cik AS VARCHAR)) <> ''
                          AND company_id IS NOT NULL
                          AND TRIM(CAST(company_id AS VARCHAR)) <> ''
                    ) x
                    WHERE rn = 1
                    """
                )
            else:
                con.execute(
                    """
                    CREATE TEMP TABLE tmp_sec_company_map AS
                    SELECT DISTINCT
                        LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik,
                        LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS company_id
                    FROM symbol_reference
                    WHERE cik IS NOT NULL
                      AND TRIM(CAST(cik AS VARCHAR)) <> ''
                    """
                )
        else:
            con.execute(
                """
                CREATE TEMP TABLE tmp_sec_company_map AS
                SELECT DISTINCT
                    LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik,
                    LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS company_id
                FROM sec_xbrl_fact_raw
                WHERE cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
                """
            )

        if args.verbose:
            company_map_probe = _fetch_one_dict(
                con,
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT cik) AS ciks,
                    COUNT(DISTINCT company_id) AS company_ids
                FROM tmp_sec_company_map
                """,
            )
            print(
                f"[build_sec_fact_normalized] company_map_probe={json.dumps(company_map_probe, default=str, sort_keys=True)}",
                flush=True,
            )

        # ------------------------------------------------------------------
        # Meilleur filing par accession_number
        # Ici il peut y avoir 0 row si sec_filing est vide.
        # Ce n'est pas bloquant.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_sec_filing_best")
        con.execute(
            """
            CREATE TEMP TABLE tmp_sec_filing_best AS
            SELECT
                filing_id,
                company_id,
                LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik,
                accession_number,
                available_at,
                accepted_at,
                filing_date,
                source_name
            FROM (
                SELECT
                    filing_id,
                    company_id,
                    cik,
                    accession_number,
                    available_at,
                    accepted_at,
                    filing_date,
                    source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY accession_number
                        ORDER BY
                            available_at DESC NULLS LAST,
                            accepted_at DESC NULLS LAST,
                            filing_date DESC NULLS LAST,
                            filing_id
                    ) AS rn
                FROM sec_filing
                WHERE accession_number IS NOT NULL
                  AND TRIM(accession_number) <> ''
            ) x
            WHERE rn = 1
            """
        )

        if args.verbose:
            filing_best_probe = _fetch_one_dict(
                con,
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT accession_number) AS accession_numbers
                FROM tmp_sec_filing_best
                """,
            )
            print(
                f"[build_sec_fact_normalized] filing_best_probe={json.dumps(filing_best_probe, default=str, sort_keys=True)}",
                flush=True,
            )

        # ------------------------------------------------------------------
        # Liste des chunks mensuels
        # ------------------------------------------------------------------
        month_starts = _fetch_month_starts(
            con,
            args.min_period_end_date,
            args.max_period_end_date,
        )

        if not month_starts:
            summary = {
                "table_name": "sec_fact_normalized",
                "rows": 0,
                "month_chunks": 0,
                "inserted_rows": 0,
                "message": "no eligible monthly chunks found in sec_xbrl_fact_raw",
            }
            print(json.dumps(summary, indent=2, default=str), flush=True)
            return 0

        if args.verbose:
            print(f"[build_sec_fact_normalized] month_chunks={len(month_starts)}", flush=True)
            print(
                f"[build_sec_fact_normalized] first_month={month_starts[0]} last_month={month_starts[-1]}",
                flush=True,
            )

        inserted_total = 0

        # ------------------------------------------------------------------
        # Traitement chunké par mois
        # ------------------------------------------------------------------
        for idx, month_start in enumerate(
            tqdm(month_starts, desc="sec_fact_normalized_months", unit="month"),
            start=1,
        ):
            month_end = con.execute(
                "SELECT (date_trunc('month', CAST(? AS DATE)) + INTERVAL '1 month' - INTERVAL '1 day')::DATE",
                [month_start],
            ).fetchone()[0]

            if args.verbose or idx == 1 or idx == len(month_starts) or idx % 12 == 0:
                print(
                    f"[build_sec_fact_normalized] month_chunk {idx}/{len(month_starts)}: "
                    f"{month_start} -> {month_end}",
                    flush=True,
                )

            # Scope mensuel nettoyé.
            con.execute("DROP TABLE IF EXISTS tmp_sec_fact_scope")
            con.execute(
                """
                CREATE TEMP TABLE tmp_sec_fact_scope AS
                SELECT
                    accession_number,
                    LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik,
                    LOWER(TRIM(CAST(taxonomy AS VARCHAR))) AS taxonomy,
                    TRIM(CAST(concept AS VARCHAR)) AS concept,
                    NULLIF(TRIM(CAST(unit AS VARCHAR)), '') AS unit,
                    period_end_date,
                    CASE
                        WHEN value_text IS NULL THEN NULL
                        WHEN TRIM(CAST(value_text AS VARCHAR)) = '' THEN NULL
                        ELSE CAST(value_text AS VARCHAR)
                    END AS value_text,
                    value_numeric,
                    source_name,
                    source_file,
                    ingested_at
                FROM sec_xbrl_fact_raw
                WHERE accession_number IS NOT NULL
                  AND TRIM(accession_number) <> ''
                  AND cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
                  AND taxonomy IS NOT NULL
                  AND TRIM(CAST(taxonomy AS VARCHAR)) <> ''
                  AND concept IS NOT NULL
                  AND TRIM(CAST(concept AS VARCHAR)) <> ''
                  AND period_end_date IS NOT NULL
                  AND period_end_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
                  AND (
                        value_numeric IS NOT NULL
                     OR (value_text IS NOT NULL AND TRIM(CAST(value_text AS VARCHAR)) <> '')
                  )
                """,
                [month_start, month_end],
            )

            # Dédup logique au grain utile pour la table cible réelle.
            con.execute("DROP TABLE IF EXISTS tmp_sec_fact_dedup")
            con.execute(
                """
                CREATE TEMP TABLE tmp_sec_fact_dedup AS
                SELECT
                    accession_number,
                    cik,
                    taxonomy,
                    concept,
                    unit,
                    period_end_date,
                    value_text,
                    value_numeric,
                    source_name,
                    source_file,
                    ingested_at
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                accession_number,
                                cik,
                                taxonomy,
                                concept,
                                period_end_date,
                                COALESCE(unit, ''),
                                COALESCE(value_text, ''),
                                value_numeric
                            ORDER BY
                                ingested_at DESC NULLS LAST,
                                source_file DESC NULLS LAST,
                                source_name DESC NULLS LAST
                        ) AS rn
                    FROM tmp_sec_fact_scope
                ) x
                WHERE rn = 1
                """
            )

            month_scope_probe = _fetch_one_dict(
                con,
                """
                SELECT
                    COUNT(*) AS scoped_rows,
                    COUNT(DISTINCT cik) AS scoped_ciks,
                    COUNT(DISTINCT taxonomy || '|' || concept) AS scoped_concepts
                FROM tmp_sec_fact_dedup
                """,
            )

            inserted_rows = con.execute(
                """
                INSERT INTO sec_fact_normalized (
                    filing_id,
                    company_id,
                    cik,
                    taxonomy,
                    concept,
                    period_end_date,
                    unit,
                    value_text,
                    value_numeric,
                    available_at,
                    source_name,
                    created_at
                )
                SELECT
                    b.filing_id,
                    COALESCE(
                        NULLIF(TRIM(CAST(m.company_id AS VARCHAR)), ''),
                        NULLIF(TRIM(CAST(b.company_id AS VARCHAR)), ''),
                        f.cik
                    ) AS company_id,
                    f.cik,
                    f.taxonomy,
                    f.concept,
                    f.period_end_date,
                    f.unit,
                    f.value_text,
                    f.value_numeric,
                    COALESCE(
                        b.available_at,
                        b.accepted_at,
                        CAST(b.filing_date AS TIMESTAMP),
                        f.ingested_at
                    ) AS available_at,
                    COALESCE(b.source_name, f.source_name) AS source_name,
                    CURRENT_TIMESTAMP AS created_at
                FROM tmp_sec_fact_dedup f
                LEFT JOIN tmp_sec_company_map m
                  ON m.cik = f.cik
                LEFT JOIN tmp_sec_filing_best b
                  ON b.accession_number = f.accession_number
                """
            ).fetchone()[0]

            inserted_total += int(inserted_rows)

            if args.verbose or idx == 1 or idx == len(month_starts) or idx % 12 == 0:
                payload = {
                    "month_start": str(month_start),
                    "month_end": str(month_end),
                    "scoped_rows": int(month_scope_probe["scoped_rows"] or 0),
                    "scoped_ciks": int(month_scope_probe["scoped_ciks"] or 0),
                    "scoped_concepts": int(month_scope_probe["scoped_concepts"] or 0),
                    "inserted_rows": int(inserted_rows),
                    "inserted_rows_cumulative": int(inserted_total),
                }
                print(
                    f"[build_sec_fact_normalized] month_chunk_summary={json.dumps(payload, sort_keys=True)}",
                    flush=True,
                )

        # ------------------------------------------------------------------
        # Final probes
        # ------------------------------------------------------------------
        final_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT company_id) AS company_ids,
                COUNT(DISTINCT cik) AS ciks,
                COUNT(DISTINCT taxonomy || '|' || concept) AS concepts,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date
            FROM sec_fact_normalized
            """,
        )
        join_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(CASE WHEN filing_id IS NOT NULL THEN 1 END) AS rows_with_filing_id,
                COUNT(CASE WHEN company_id IS NOT NULL AND TRIM(CAST(company_id AS VARCHAR)) <> '' THEN 1 END) AS rows_with_company_id,
                COUNT(CASE WHEN company_id = cik THEN 1 END) AS rows_using_cik_fallback_company_id
            FROM sec_fact_normalized
            """,
        )

        result = {
            "table_name": "sec_fact_normalized",
            "rows": int(final_probe["rows"] or 0),
            "company_ids": int(final_probe["company_ids"] or 0),
            "ciks": int(final_probe["ciks"] or 0),
            "concepts": int(final_probe["concepts"] or 0),
            "min_period_end_date": str(final_probe["min_period_end_date"]) if final_probe["min_period_end_date"] is not None else None,
            "max_period_end_date": str(final_probe["max_period_end_date"]) if final_probe["max_period_end_date"] is not None else None,
            "month_chunks": int(len(month_starts)),
            "inserted_rows": int(inserted_total),
            "rows_with_filing_id": int(join_probe["rows_with_filing_id"] or 0),
            "rows_with_company_id": int(join_probe["rows_with_company_id"] or 0),
            "rows_using_cik_fallback_company_id": int(join_probe["rows_using_cik_fallback_company_id"] or 0),
        }

        print(json.dumps(result, indent=2, default=str), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
