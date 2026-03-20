#!/usr/bin/env python3
from __future__ import annotations

"""
Charge des fichiers SEC raw filing index dans `sec_filing_raw_index`.

Pourquoi cette réécriture
-------------------------
Le script précédent appelait une méthode de repository absente:
`replace_sec_filing_raw_index(...)`.

Cette version:
- reste Python mince
- conserve la logique de parsing / orchestration au niveau CLI
- délègue l'écriture au repository avec les méthodes réellement disponibles
- supporte plusieurs sources `--source`
- garde une sortie observable et robuste
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from stock_quant.domain.entities.sec_filing import SecFilingRawIndexEntry
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.infrastructure.providers.sec.sec_raw_index_loader import SecRawIndexLoader


def _utc_now() -> datetime:
    """Retourne un timestamp UTC timezone-aware pour les objets raw."""
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load SEC filing raw index files into sec_filing_raw_index."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB database path.",
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="Path to a SEC raw index file. Repeat for multiple files.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing sec_filing_raw_index rows before insert.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def _resolve_sources(raw_sources: Iterable[str]) -> list[Path]:
    """
    Résout, normalise et filtre la liste des fichiers source.

    On:
    - expanduser
    - resolve
    - déduplique
    - garde seulement les chemins existants
    """
    seen: set[str] = set()
    resolved: list[Path] = []

    for raw in raw_sources:
        path = Path(raw).expanduser().resolve()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists() and path.is_file():
            resolved.append(path)

    return resolved


def _repository_write(
    repository: DuckDbSecRepository,
    rows: list[SecFilingRawIndexEntry],
    truncate: bool,
) -> int:
    """
    Écrit les lignes via les méthodes réellement exposées par le repository.

    Ordre de fallback:
    1) replace_sec_filing_raw_index
    2) insert_sec_filing_raw_index_entries avec DELETE explicite si truncate
    """
    # ------------------------------------------------------------------
    # Cas 1: certaines versions futures/anciennes du repo peuvent
    # exposer une méthode "replace_*". On la supporte si elle existe.
    # ------------------------------------------------------------------
    if hasattr(repository, "replace_sec_filing_raw_index"):
        replace_fn = getattr(repository, "replace_sec_filing_raw_index")
        return int(replace_fn(rows))

    # ------------------------------------------------------------------
    # Cas 2: méthode canonique actuellement observée dans le codebase.
    # ------------------------------------------------------------------
    if truncate and hasattr(repository, "con"):
        repository.con.execute("DELETE FROM sec_filing_raw_index")

    if hasattr(repository, "insert_sec_filing_raw_index_entries"):
        insert_fn = getattr(repository, "insert_sec_filing_raw_index_entries")
        return int(insert_fn(rows))

    raise RuntimeError(
        "DuckDbSecRepository does not expose a supported raw index write method. "
        "Expected one of: replace_sec_filing_raw_index, insert_sec_filing_raw_index_entries."
    )


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    resolved_sources = _resolve_sources(args.source)

    if args.verbose:
        print(f"[load_sec_filing_raw_index] project_root={config.project_root}", flush=True)
        print(f"[load_sec_filing_raw_index] db_path={config.db_path}", flush=True)
        print(
            f"[load_sec_filing_raw_index] sources={json.dumps([str(p) for p in resolved_sources], indent=2)}",
            flush=True,
        )
        print(f"[load_sec_filing_raw_index] truncate={args.truncate}", flush=True)

    if not resolved_sources:
        raise SystemExit("no valid --source files provided")

    loader = SecRawIndexLoader()

    session_factory = DuckDbSessionFactory(config.db_path)

    total_rows_read = 0
    total_rows_written = 0
    per_file: list[dict[str, object]] = []

    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

        if uow.connection is None:
            raise RuntimeError("missing active DB connection")

        repository = DuckDbSecRepository(uow.connection)

        # --------------------------------------------------------------
        # On ne truncate qu'une seule fois avant le premier batch.
        # --------------------------------------------------------------
        first_batch = True

        for source_path in resolved_sources:
            parsed_rows = loader.load(source_path)

            # ----------------------------------------------------------
            # On homogénéise les objets vers SecFilingRawIndexEntry.
            # Le loader retourne déjà normalement le bon type, mais on
            # garde une conversion défensive.
            # ----------------------------------------------------------
            batch: list[SecFilingRawIndexEntry] = []
            for row in parsed_rows:
                if isinstance(row, SecFilingRawIndexEntry):
                    batch.append(row)
                    continue

                batch.append(
                    SecFilingRawIndexEntry(
                        cik=str(getattr(row, "cik", "") or ""),
                        company_name=getattr(row, "company_name", None),
                        form_type=getattr(row, "form_type", None),
                        filing_date=getattr(row, "filing_date", None),
                        accepted_at=getattr(row, "accepted_at", None),
                        accession_number=getattr(row, "accession_number", None),
                        primary_document=getattr(row, "primary_document", None),
                        filing_url=getattr(row, "filing_url", None),
                        source_name=getattr(row, "source_name", "sec"),
                        source_file=str(source_path),
                        ingested_at=getattr(row, "ingested_at", _utc_now()),
                    )
                )

            batch_written = _repository_write(
                repository=repository,
                rows=batch,
                truncate=(args.truncate and first_batch),
            )
            first_batch = False

            total_rows_read += len(batch)
            total_rows_written += int(batch_written)

            per_file.append(
                {
                    "source": str(source_path),
                    "rows_read": int(len(batch)),
                    "rows_written": int(batch_written),
                }
            )

            if args.verbose:
                print(
                    f"[load_sec_filing_raw_index] source={source_path} rows_read={len(batch)} rows_written={batch_written}",
                    flush=True,
                )

    print(
        json.dumps(
            {
                "status": "SUCCESS",
                "sources": len(resolved_sources),
                "rows_read": int(total_rows_read),
                "rows_written": int(total_rows_written),
                "truncate": bool(args.truncate),
                "files": per_file,
            },
            indent=2,
            sort_keys=True,
            default=str,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
