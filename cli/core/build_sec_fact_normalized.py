#!/usr/bin/env python3
from __future__ import annotations

"""
SQL-first builder for sec_fact_normalized.

Objectif
--------
Construire la couche normalized SEC à partir de sec_xbrl_fact_raw,
sans charger des millions de lignes en mémoire Python.

Pourquoi cette réécriture
-------------------------
La version précédente du flux SEC avait plusieurs fragilités :

1) elle traitait trop le join filings <-> facts comme une dépendance dure
2) elle pouvait perdre des facts valides quand un accession_number issu de
   companyfacts ne retrouvait pas un filing dans sec_filing
3) elle n'était pas assez homogène avec les autres scripts lourds du repo
   concernant :
   - PRAGMA DuckDB
   - logs de probes
   - contrôle SQL-first
   - structure de sortie JSON

Cette version :
- reste SQL-first
- applique les PRAGMA runtime comme les autres scripts lourds
- produit des métriques de qualité utiles
- enrichit les facts avec filing_id / company_id / available_at
- n'élimine PAS les facts simplement parce que le join filing échoue
- garde une logique PIT explicite :
  - available_at prioritaire depuis sec_filing.available_at
  - sinon accepted_at
  - sinon filing_date cast timestamp
  - sinon fallback technique sur ingested_at du raw

Important
---------
On n'utilise PAS period_end_date comme substitut de visibilité marché.
period_end_date décrit la période comptable, pas la date de publication.

Contexte important du SEC
-------------------------
Les companyfacts SEC peuvent référencer des accession numbers déposés par des
filing agents. Donc :
- le fact peut être valide
- l'accession peut être présent dans companyfacts
- mais ne pas se retrouver dans notre sec_filing si le raw filings n'est pas
  chargé au même niveau d'historique

Conclusion :
- sec_xbrl_fact_raw = vérité source des facts
- sec_filing = enrichissement quand disponible
"""

import argparse
import json
from pathlib import Path
from typing import Any

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


# =============================================================================
# Helpers simples
# =============================================================================


def _quote_sql_string(value: str) -> str:
    """
    Escape minimal pour injecter une string dans un PRAGMA SQL.
    """
    return str(value).replace("'", "''")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build sec_fact_normalized from sec_xbrl_fact_raw using SQL-first transformations."
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
        help="Flag conservé pour clarté d'interface. Le build fait un full refresh de toute façon.",
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
    """
    Retourne une seule ligne sous forme dict, pratique pour les probes.
    """
    cursor = con.execute(sql, params or [])
    columns = [str(desc[0]) for desc in cursor.description]
    row = cursor.fetchone()

    if row is None:
        return {col: None for col in columns}

    return {columns[i]: row[i] for i in range(len(columns))}


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

        # ---------------------------------------------------------------------
        # Runtime PRAGMA
        # ---------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{_quote_sql_string(args.memory_limit)}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{_quote_sql_string(str(temp_dir_path))}'")

        # ---------------------------------------------------------------------
        # Préflight minimal
        # ---------------------------------------------------------------------
        if not _table_exists(con, "sec_xbrl_fact_raw"):
            raise RuntimeError("required table sec_xbrl_fact_raw does not exist")

        if not _table_exists(con, "sec_fact_normalized"):
            raise RuntimeError("required table sec_fact_normalized does not exist after schema init")

        if not _table_exists(con, "sec_filing"):
            raise RuntimeError("required table sec_filing does not exist")

        # ---------------------------------------------------------------------
        # Probes raw
        # ---------------------------------------------------------------------
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

        # ---------------------------------------------------------------------
        # Full refresh déterministe
        # ---------------------------------------------------------------------
        con.execute("DELETE FROM sec_fact_normalized")

        if args.verbose:
            print("[build_sec_fact_normalized] cleared sec_fact_normalized", flush=True)

        # ---------------------------------------------------------------------
        # Map CIK -> company_id
        #
        # Objectif:
        # - si symbol_reference expose company_id, on l'utilise
        # - sinon fallback robuste company_id = cik normalisé
        #
        # Pourquoi:
        # - certaines versions du schéma n'ont pas company_id
        # - certaines chaînes historiques SEC doivent quand même produire
        #   un identifiant stable
        # ---------------------------------------------------------------------
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

        # ---------------------------------------------------------------------
        # Meilleur filing par accession_number
        #
        # Important:
        # - on ne suppose pas un mapping parfait accession -> filing historique
        # - cette table sert d'enrichissement uniquement
        # - si le filing n'existe pas, le fact doit survivre
        # ---------------------------------------------------------------------
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

        # ---------------------------------------------------------------------
        # Scope filtré / nettoyé
        #
        # On enlève ici:
        # - période hors bornes
        # - identité incomplète
        # - rows sans valeur numérique ni texte
        # ---------------------------------------------------------------------
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
                period_start_date,
                fiscal_year,
                NULLIF(TRIM(CAST(fiscal_period AS VARCHAR)), '') AS fiscal_period,
                NULLIF(TRIM(CAST(frame AS VARCHAR)), '') AS frame,
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
            [args.min_period_end_date, args.max_period_end_date],
        )

        # ---------------------------------------------------------------------
        # Dédup logique
        #
        # On déduplique à granularité du fact.
        # On garde la version la plus récente selon ingested_at/source_file/source_name.
        # ---------------------------------------------------------------------
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
                period_start_date,
                fiscal_year,
                fiscal_period,
                frame,
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

        # ---------------------------------------------------------------------
        # Insert normalized
        #
        # Règle clé:
        # - le filing enrichit si trouvé
        # - sinon on garde quand même le fact
        #
        # available_at:
        # - priorité à sec_filing.available_at
        # - puis accepted_at
        # - puis filing_date
        # - puis ingested_at du raw
        #
        # On n'utilise jamais period_end_date comme date de publication.
        # ---------------------------------------------------------------------
        con.execute(
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
                f.filing_id AS filing_id,
                COALESCE(
                    NULLIF(TRIM(CAST(f.company_id AS VARCHAR)), ''),
                    NULLIF(TRIM(CAST(m.company_id AS VARCHAR)), ''),
                    d.cik
                ) AS company_id,
                d.cik,
                d.taxonomy,
                d.concept,
                d.period_end_date,
                d.unit,
                d.value_text,
                d.value_numeric,
                COALESCE(
                    f.available_at,
                    f.accepted_at,
                    CAST(f.filing_date AS TIMESTAMP),
                    d.ingested_at
                ) AS available_at,
                COALESCE(f.source_name, d.source_name, 'sec') AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_sec_fact_dedup d
            LEFT JOIN tmp_sec_filing_best f
              ON f.accession_number = d.accession_number
            LEFT JOIN tmp_sec_company_map m
              ON m.cik = d.cik
            """
        )

        # ---------------------------------------------------------------------
        # Probes qualité finales
        # ---------------------------------------------------------------------
        scope_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS scoped_rows,
                COUNT(DISTINCT cik) AS scoped_ciks,
                COUNT(DISTINCT taxonomy || '|' || concept) AS scoped_concepts,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date
            FROM tmp_sec_fact_dedup
            """,
        )

        normalized_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT cik) AS ciks,
                COUNT(DISTINCT taxonomy || '|' || concept) AS concepts,
                COUNT(CASE WHEN filing_id IS NOT NULL THEN 1 END) AS rows_with_filing_id,
                COUNT(CASE WHEN company_id IS NOT NULL AND TRIM(company_id) <> '' THEN 1 END) AS rows_with_company_id,
                COUNT(CASE WHEN available_at IS NOT NULL THEN 1 END) AS rows_with_available_at,
                MIN(period_end_date) AS min_period_end_date,
                MAX(period_end_date) AS max_period_end_date
            FROM sec_fact_normalized
            """,
        )

        join_probe = _fetch_one_dict(
            con,
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(CASE WHEN filing_id IS NOT NULL THEN 1 END) AS rows_joined_to_filing,
                COUNT(CASE WHEN filing_id IS NULL THEN 1 END) AS rows_without_filing_join,
                COUNT(CASE WHEN company_id = cik THEN 1 END) AS rows_using_cik_fallback_company_id
            FROM sec_fact_normalized
            """,
        )

        top_taxonomy = con.execute(
            """
            SELECT
                taxonomy,
                COUNT(*) AS rows
            FROM sec_fact_normalized
            GROUP BY taxonomy
            ORDER BY rows DESC, taxonomy
            LIMIT 10
            """
        ).fetchall()

        top_concepts = con.execute(
            """
            SELECT
                taxonomy,
                concept,
                COUNT(*) AS rows
            FROM sec_fact_normalized
            GROUP BY taxonomy, concept
            ORDER BY rows DESC, taxonomy, concept
            LIMIT 10
            """
        ).fetchall()

        quality = {
            "raw_probe": raw_probe,
            "raw_bad_probe": raw_bad_probe,
            "scope_probe": scope_probe,
            "normalized_probe": normalized_probe,
            "join_probe": join_probe,
            "top_taxonomy": [
                {
                    "taxonomy": row[0],
                    "rows": int(row[1]),
                }
                for row in top_taxonomy
            ],
            "top_concepts": [
                {
                    "taxonomy": row[0],
                    "concept": row[1],
                    "rows": int(row[2]),
                }
                for row in top_concepts
            ],
        }

        if args.verbose:
            print(
                f"[build_sec_fact_normalized] quality_summary="
                f"{json.dumps(quality, default=str, sort_keys=True)}",
                flush=True,
            )

        output = {
            "status": "SUCCESS",
            "rows_written": int(normalized_probe["rows"] or 0),
            "metrics": {
                "raw_rows": int(raw_probe["rows"] or 0),
                "scoped_rows": int(scope_probe["scoped_rows"] or 0),
                "normalized_rows": int(normalized_probe["rows"] or 0),
                "rows_joined_to_filing": int(join_probe["rows_joined_to_filing"] or 0),
                "rows_without_filing_join": int(join_probe["rows_without_filing_join"] or 0),
                "rows_using_cik_fallback_company_id": int(
                    join_probe["rows_using_cik_fallback_company_id"] or 0
                ),
            },
            "quality": quality,
        }

        print(json.dumps(output, indent=2, sort_keys=True, default=str), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
