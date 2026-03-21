#!/usr/bin/env python3
from __future__ import annotations

"""
Chunked SEC filing builder with visible progress.

Objectif
--------
Promouvoir `sec_filing_raw_index` vers `sec_filing` avec:
- progression visible
- traitement chunké par mois de filing_date
- logs cumulés lisibles
- compatibilité avec le repository/service existants

Pourquoi cette version
----------------------
La version précédente déléguait tout au pipeline monolithique:
- peu ou pas de visibilité
- difficile de savoir si ça avance
- difficile de diagnostiquer un blocage

Cette version garde les briques métier existantes:
- DuckDbSecRepository
- SecFilingsService

Mais orchestre en Python mince:
- découverte des chunks mensuels
- extraction SQL chunkée
- tqdm visible
- upsert chunk par chunk

Notes
-----
- chunk = 1 mois de filing_date
- si filing_date est NULL, on crée un chunk spécial "NULL_DATE"
- le mapping CIK -> company_id_effectif reste délégué au repository
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build normalized SEC filing tables incrementally with chunked progress."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _require_active_connection(uow: DuckDbUnitOfWork):
    con = uow.connection
    if con is None:
        raise RepositoryError("DuckDbUnitOfWork has no active connection")
    return con


def _fetch_month_chunks(con: Any) -> list[str | None]:
    """
    Retourne les mois distincts présents dans sec_filing_raw_index.

    Format:
    - 'YYYY-MM-01' pour les dates non nulles
    - None pour le chunk filing_date IS NULL
    """
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


def _load_raw_index_rows_for_chunk(con: Any, month_start: str | None) -> list[dict[str, Any]]:
    """
    Charge les lignes raw d'un seul chunk mensuel.

    Si month_start est None:
    - on charge filing_date IS NULL
    Sinon:
    - on charge filing_date dans le mois correspondant
    """
    if month_start is None:
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
            WHERE filing_date >= CAST(? AS DATE)
              AND filing_date < (CAST(? AS DATE) + INTERVAL '1 month')
            ORDER BY
                COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP), ingested_at) DESC NULLS LAST,
                accession_number
            """,
            [month_start, month_start],
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


def _table_count(con: Any, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_sec_filings] project_root={config.project_root}", flush=True)
        print(f"[build_sec_filings] db_path={config.db_path}", flush=True)

    session_factory = DuckDbSessionFactory(config.db_path)

    # ------------------------------------------------------------------
    # Pass 1a: master data schema
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    # ------------------------------------------------------------------
    # Pass 1b: SEC schema
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

    # ------------------------------------------------------------------
    # Pass 2: chunked build
    # ------------------------------------------------------------------
    with DuckDbUnitOfWork(session_factory) as uow:
        con = _require_active_connection(uow)
        repository = DuckDbSecRepository(con)
        service = SecFilingsService()

        raw_total_before = _table_count(con, "sec_filing_raw_index")
        sec_filing_before = _table_count(con, "sec_filing")
        cik_company_map = repository.load_cik_company_map()
        month_chunks = _fetch_month_chunks(con)

        if args.verbose:
            print(f"[build_sec_filings] raw_total_before={raw_total_before}", flush=True)
            print(f"[build_sec_filings] sec_filing_before={sec_filing_before}", flush=True)
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
            raw_index_rows = _load_raw_index_rows_for_chunk(con, month_start)
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

            # Log périodique propre
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
            },
        }

        print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
