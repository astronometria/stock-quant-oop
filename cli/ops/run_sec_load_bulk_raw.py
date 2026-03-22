#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_sec_load_bulk_raw.py
# -----------------------------------------------------------------------------
# Charge les archives bulk SEC déjà extraites vers les tables raw DuckDB.
#
# Tables cibles:
# - sec_filing_raw_index
# - sec_xbrl_fact_raw
#
# Cette étape ne fait que:
# - bulk disque -> raw DB
#
# Les étapes suivantes feront:
# - raw -> sec_filing
# - raw -> sec_fact_normalized
# - normalized -> fundamentals snapshots/features
# =============================================================================

import argparse
import json
from pathlib import Path

from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.sec.sec_bulk_disk_loader import SecBulkDiskLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load extracted SEC bulk files into raw DuckDB tables.")
    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop-runtime/db/market.duckdb",
        help="DuckDB path.",
    )
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop/data",
        help="Project data root.",
    )
    parser.add_argument(
        "--truncate-before-load",
        action="store_true",
        help="Delete current SEC raw tables before loading.",
    )
    parser.add_argument(
        "--limit-ciks",
        type=int,
        default=None,
        help="Optional limit for a dry-run / test.",
    )
    parser.add_argument(
        "--only-submissions",
        action="store_true",
        help="Load only sec_filing_raw_index.",
    )
    parser.add_argument(
        "--only-companyfacts",
        action="store_true",
        help="Load only sec_xbrl_fact_raw.",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=5000,
        help="DuckDB executemany batch size.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser()
    data_root = Path(args.data_root).expanduser()

    load_submissions = not args.only_companyfacts
    load_companyfacts = not args.only_submissions

    print(f"[run_sec_load_bulk_raw] db_path={db_path}")
    print(f"[run_sec_load_bulk_raw] data_root={data_root}")
    print(f"[run_sec_load_bulk_raw] load_submissions={load_submissions}")
    print(f"[run_sec_load_bulk_raw] load_companyfacts={load_companyfacts}")
    print(f"[run_sec_load_bulk_raw] truncate_before_load={bool(args.truncate_before_load)}")
    print(f"[run_sec_load_bulk_raw] limit_ciks={args.limit_ciks}")
    print(f"[run_sec_load_bulk_raw] insert_batch_size={args.insert_batch_size}")

    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        if uow.connection is None:
            raise RuntimeError("active DB connection is required")

        loader = SecBulkDiskLoader(
            con=uow.connection,
            data_root=data_root,
            insert_batch_size=args.insert_batch_size,
        )

        metrics = loader.load_raw_from_disk(
            load_submissions=load_submissions,
            load_companyfacts=load_companyfacts,
            truncate_before_load=bool(args.truncate_before_load),
            limit_ciks=args.limit_ciks,
        )

        print(
            "[run_sec_load_bulk_raw] metrics="
            + json.dumps(
                {
                    "submission_files_seen": metrics.submission_files_seen,
                    "submission_rows_written": metrics.submission_rows_written,
                    "companyfacts_files_seen": metrics.companyfacts_files_seen,
                    "xbrl_fact_rows_written": metrics.xbrl_fact_rows_written,
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        # Petit probe DB final
        counts = {}
        for table_name in ["sec_filing_raw_index", "sec_xbrl_fact_raw"]:
            row = uow.connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            counts[table_name] = int(row[0]) if row else 0

        print("[run_sec_load_bulk_raw] table_counts=" + json.dumps(counts, ensure_ascii=False, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
