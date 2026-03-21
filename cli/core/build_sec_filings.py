#!/usr/bin/env python3
from __future__ import annotations

"""
Incremental + chunked SEC filing builder with visible progress.

Objectif
--------
Promouvoir `sec_filing_raw_index` vers `sec_filing` de façon:
- incrémentale
- visible (tqdm)
- robuste sur les reruns
- compatible avec le schéma local réel

Stratégie
---------
1) Maintenir un watermark de pipeline dans `sec_pipeline_state`
2) Détecter les lignes raw candidates:
   - ingested_at > watermark
   - OU filing_date dans une petite fenêtre récente de recouvrement
3) Chunker par mois de filing_date
4) Builder les `SecFiling` avec le service existant
5) Upsert chunk par chunk
6) Mettre à jour le watermark seulement en cas de succès complet

Pourquoi garder un recouvrement
-------------------------------
Même si le raw SEC est généralement append-only, un petit overlap récent aide à:
- rejouer des mois fraîchement touchés
- absorber les re-runs partiels
- garder l'upsert idempotent
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

from stock_quant.app.services.sec_filings_service import SecFilingsService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.shared.exceptions import RepositoryError


PIPELINE_NAME = "build_sec_filings_incremental"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build sec_filing incrementally from sec_filing_raw_index with chunked progress."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--reprocess-recent-months",
        type=int,
        default=2,
        help="Small overlap window to reprocess recent filing months on every run.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _require_active_connection(uow: DuckDbUnitOfWork):
    con = uow.connection
    if con is None:
        raise RepositoryError("DuckDbUnitOfWork has no active connection")
    return con


def _table_count(con: Any, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _ensure_pipeline_state_table(con: Any) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sec_pipeline_state (
            pipeline_name VARCHAR,
            last_raw_ingested_at TIMESTAMP,
            updated_at TIMESTAMP,
            notes VARCHAR
        )
        """
    )


def _get_watermark(con: Any, pipeline_name: str) -> Any:
    row = con.execute(
        """
        SELECT last_raw_ingested_at
        FROM sec_pipeline_state
        WHERE pipeline_name = ?
        ORDER BY updated_at DESC NULLS LAST
        LIMIT 1
        """,
        [pipeline_name],
    ).fetchone()
    return row[0] if row else None


def _set_watermark(con: Any, pipeline_name: str, watermark: Any, notes: str) -> None:
    con.execute(
        """
        DELETE FROM sec_pipeline_state
        WHERE pipeline_name = ?
        """,
        [pipeline_name],
    )
    con.execute(
        """
        INSERT INTO sec_pipeline_state (
            pipeline_name,
            last_raw_ingested_at,
            updated_at,
            notes
        )
        VALUES (?, ?, CURRENT_TIMESTAMP, ?)
        """,
        [pipeline_name, watermark, notes],
    )


def _get_raw_max_ingested_at(con: Any) -> Any:
    return con.execute(
        """
        SELECT MAX(ingested_at)
        FROM sec_filing_raw_index
        """
    ).fetchone()[0]


def _fetch_candidate_month_chunks(con: Any, watermark: Any, reprocess_recent_months: int) -> list[str | None]:
    """
    Retourne les mois à traiter.

    Critères:
    - toute ligne avec ingested_at > watermark
    - + petite fenêtre récente de filing_date pour overlap
    - + chunk spécial NULL_DATE si nécessaire
    """
    if watermark is None:
        rows = con.execute(
            """
            SELECT DISTINCT date_trunc('month', filing_date)::DATE AS month_start
            FROM sec_filing_raw_index
            WHERE filing_date IS NOT NULL
            ORDER BY month_start
            """
        ).fetchall()
        chunks: list[str | None] = [str(row[0]) for row in rows]

        null_rows = con.execute(
            """
            SELECT COUNT(*)
            FROM sec_filing_raw_index
            WHERE filing_date IS NULL
            """
        ).fetchone()[0]
        if int(null_rows or 0) > 0:
            chunks.append(None)
        return chunks

    rows = con.execute(
        f"""
        WITH overlap_floor AS (
            SELECT
                CAST(
                    date_trunc('month', MAX(filing_date)) - INTERVAL '{int(reprocess_recent_months)} month'
                    AS DATE
                ) AS floor_date
            FROM sec_filing_raw_index
        )
        SELECT DISTINCT date_trunc('month', filing_date)::DATE AS month_start
        FROM sec_filing_raw_index, overlap_floor
        WHERE filing_date IS NOT NULL
          AND (
                ingested_at > CAST(? AS TIMESTAMP)
             OR filing_date >= overlap_floor.floor_date
          )
        ORDER BY month_start
        """,
        [watermark],
    ).fetchall()

    chunks = [str(row[0]) for row in rows]

    null_rows = con.execute(
        """
        SELECT COUNT(*)
        FROM sec_filing_raw_index
        WHERE filing_date IS NULL
          AND ingested_at > CAST(? AS TIMESTAMP)
        """,
        [watermark],
    ).fetchone()[0]
    if int(null_rows or 0) > 0:
        chunks.append(None)

    return chunks


def _load_raw_index_rows_for_chunk(
    con: Any,
    month_start: str | None,
    watermark: Any,
    reprocess_recent_months: int,
) -> list[dict[str, Any]]:
    """
    Charge les raw rows d'un chunk.

    Pour un run incrémental:
    - on prend les nouveaux raw rows
    - on reprend aussi les mois récents d'overlap
    """
    if month_start is None:
        if watermark is None:
            rows = con.execute(
                """
                SELECT
                    cik,
                    company_name,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    primary_document,
                    filing_url,
                    source_name,
                    source_file,
                    ingested_at
                FROM sec_filing_raw_index
                WHERE filing_date IS NULL
                ORDER BY
                    COALESCE(accepted_at, ingested_at) DESC NULLS LAST,
                    accession_number
                """
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT
                    cik,
                    company_name,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    primary_document,
                    filing_url,
                    source_name,
                    source_file,
                    ingested_at
                FROM sec_filing_raw_index
                WHERE filing_date IS NULL
                  AND ingested_at > CAST(? AS TIMESTAMP)
                ORDER BY
                    COALESCE(accepted_at, ingested_at) DESC NULLS LAST,
                    accession_number
                """,
                [watermark],
            ).fetchall()
    else:
        if watermark is None:
            rows = con.execute(
                """
                SELECT
                    cik,
                    company_name,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    primary_document,
                    filing_url,
                    source_name,
                    source_file,
                    ingested_at
                FROM sec_filing_raw_index
                WHERE filing_date >= CAST(? AS DATE)
                  AND filing_date < (CAST(? AS DATE) + INTERVAL '1 month')
                ORDER BY
                    COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP), ingested_at) DESC NULLS LAST,
                    accession_number
                """,
                [month_start, month_start],
            ).fetchall()
        else:
            rows = con.execute(
                f"""
                WITH overlap_floor AS (
                    SELECT
                        CAST(
                            date_trunc('month', MAX(filing_date)) - INTERVAL '{int(reprocess_recent_months)} month'
                            AS DATE
                        ) AS floor_date
                    FROM sec_filing_raw_index
                )
                SELECT
                    cik,
                    company_name,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    primary_document,
                    filing_url,
                    source_name,
                    source_file,
                    ingested_at
                FROM sec_filing_raw_index, overlap_floor
                WHERE filing_date >= CAST(? AS DATE)
                  AND filing_date < (CAST(? AS DATE) + INTERVAL '1 month')
                  AND (
                        ingested_at > CAST(? AS TIMESTAMP)
                     OR filing_date >= overlap_floor.floor_date
                  )
                ORDER BY
                    COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP), ingested_at) DESC NULLS LAST,
                    accession_number
                """,
                [month_start, month_start, watermark],
            ).fetchall()

    return [
        {
            "cik": row[0],
            "company_name": row[1],
            "form_type": row[2],
            "filing_date": row[3],
            "accepted_at": row[4],
            "accession_number": row[5],
            "primary_document": row[6],
            "filing_url": row[7],
            "source_name": row[8],
            "source_file": row[9],
            "ingested_at": row[10],
        }
        for row in rows
    ]


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_sec_filings] project_root={config.project_root}", flush=True)
        print(f"[build_sec_filings] db_path={config.db_path}", flush=True)
        print(f"[build_sec_filings] reprocess_recent_months={args.reprocess_recent_months}", flush=True)

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

    with DuckDbUnitOfWork(session_factory) as uow:
        con = _require_active_connection(uow)
        repository = DuckDbSecRepository(con)
        service = SecFilingsService()

        _ensure_pipeline_state_table(con)

        raw_total_before = _table_count(con, "sec_filing_raw_index")
        sec_filing_before = _table_count(con, "sec_filing")
        watermark_before = _get_watermark(con, PIPELINE_NAME)
        raw_max_ingested_at = _get_raw_max_ingested_at(con)
        cik_company_map = repository.load_cik_company_map()
        month_chunks = _fetch_candidate_month_chunks(
            con,
            watermark=watermark_before,
            reprocess_recent_months=args.reprocess_recent_months,
        )

        if args.verbose:
            print(f"[build_sec_filings] raw_total_before={raw_total_before}", flush=True)
            print(f"[build_sec_filings] sec_filing_before={sec_filing_before}", flush=True)
            print(f"[build_sec_filings] watermark_before={watermark_before}", flush=True)
            print(f"[build_sec_filings] raw_max_ingested_at={raw_max_ingested_at}", flush=True)
            print(f"[build_sec_filings] cik_company_map_size={len(cik_company_map)}", flush=True)
            print(f"[build_sec_filings] month_chunks={len(month_chunks)}", flush=True)
            if month_chunks:
                print(
                    f"[build_sec_filings] first_chunk={month_chunks[0]} last_chunk={month_chunks[-1]}",
                    flush=True,
                )

        if raw_total_before == 0:
            payload = {
                "pipeline_name": "build_sec_filings",
                "status": "SUCCESS",
                "rows_read": 0,
                "rows_written": 0,
                "metrics": {
                    "raw_index_rows": 0,
                    "sec_filing_rows_before": sec_filing_before,
                    "sec_filing_rows_after": sec_filing_before,
                    "month_chunks": 0,
                    "watermark_before": str(watermark_before) if watermark_before is not None else None,
                    "watermark_after": str(watermark_before) if watermark_before is not None else None,
                },
            }
            print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
            return 0

        if not month_chunks:
            _set_watermark(
                con,
                PIPELINE_NAME,
                raw_max_ingested_at,
                notes="noop_no_candidate_months",
            )
            payload = {
                "pipeline_name": "build_sec_filings",
                "status": "SUCCESS",
                "rows_read": 0,
                "rows_written": 0,
                "metrics": {
                    "raw_index_rows": raw_total_before,
                    "sec_filing_rows_before": sec_filing_before,
                    "sec_filing_rows_after": sec_filing_before,
                    "month_chunks": 0,
                    "watermark_before": str(watermark_before) if watermark_before is not None else None,
                    "watermark_after": str(raw_max_ingested_at) if raw_max_ingested_at is not None else None,
                    "message": "no candidate months to process",
                },
            }
            print(json.dumps(payload, indent=2, sort_keys=True), flush=True)
            return 0

        total_rows_read = 0
        total_rows_built = 0
        total_rows_written = 0
        matched_company_ids_total = 0
        skipped_missing_keys_total = 0

        for idx, month_start in enumerate(
            tqdm(month_chunks, desc="sec_filing_months", unit="month"),
            start=1,
        ):
            raw_index_rows = _load_raw_index_rows_for_chunk(
                con=con,
                month_start=month_start,
                watermark=watermark_before,
                reprocess_recent_months=args.reprocess_recent_months,
            )
            chunk_rows = len(raw_index_rows)
            total_rows_read += chunk_rows

            filings, metrics = service.build_sec_filings(
                raw_index_rows=raw_index_rows,
                cik_company_map=cik_company_map,
            )
            built_rows = len(filings)
            written_rows = repository.upsert_sec_filings(filings)

            total_rows_built += built_rows
            total_rows_written += written_rows
            matched_company_ids_total += int(metrics.get("matched_company_ids", 0))
            skipped_missing_keys_total += int(metrics.get("skipped_missing_keys", 0))

            if args.verbose or idx == 1 or idx == len(month_chunks) or idx % 12 == 0:
                payload = {
                    "chunk_index": idx,
                    "chunk_total": len(month_chunks),
                    "month_start": month_start if month_start is not None else "NULL_DATE",
                    "raw_index_rows": chunk_rows,
                    "built_rows": built_rows,
                    "written_rows": written_rows,
                    "rows_read_cumulative": total_rows_read,
                    "rows_built_cumulative": total_rows_built,
                    "rows_written_cumulative": total_rows_written,
                    "matched_company_ids_cumulative": matched_company_ids_total,
                    "skipped_missing_keys_cumulative": skipped_missing_keys_total,
                }
                print(
                    f"[build_sec_filings] chunk_summary={json.dumps(payload, sort_keys=True)}",
                    flush=True,
                )

        sec_filing_after = _table_count(con, "sec_filing")

        _set_watermark(
            con,
            PIPELINE_NAME,
            raw_max_ingested_at,
            notes=(
                f"success rows_read={total_rows_read} rows_written={total_rows_written} "
                f"chunks={len(month_chunks)}"
            ),
        )

        result = {
            "pipeline_name": "build_sec_filings",
            "status": "SUCCESS",
            "rows_read": total_rows_read,
            "rows_written": total_rows_written,
            "metrics": {
                "raw_index_rows": raw_total_before,
                "rows_read_cumulative": total_rows_read,
                "rows_built_cumulative": total_rows_built,
                "rows_written_cumulative": total_rows_written,
                "matched_company_ids": matched_company_ids_total,
                "skipped_missing_keys": skipped_missing_keys_total,
                "month_chunks": len(month_chunks),
                "sec_filing_rows_before": sec_filing_before,
                "sec_filing_rows_after": sec_filing_after,
                "watermark_before": str(watermark_before) if watermark_before is not None else None,
                "watermark_after": str(raw_max_ingested_at) if raw_max_ingested_at is not None else None,
            },
        }

        print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
