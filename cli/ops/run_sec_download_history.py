#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_sec_download_history.py
# -----------------------------------------------------------------------------
# Runner CLI pour télécharger l'historique brut SEC sur disque.
#
# Ce runner:
# - lit les CIKs depuis la DB si possible
# - ou permet de les passer via un fichier texte
# - télécharge submissions + companyfacts pour tous les CIKs
# - écrit sous:
#     data/sec/submissions/
#     data/sec/companyfacts/
#     data/sec/manifests/
#
# Important:
# - cette étape NE charge pas encore la DB
# - elle constitue la couche "raw durable sur disque"
# - l'étape suivante branchera le load raw -> normalized -> fundamentals
# =============================================================================

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.infrastructure.providers.sec.sec_bulk_history_provider import (
    SecBulkHistoryProvider,
    SecDownloadTarget,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download historical SEC submissions/companyfacts to disk.")
    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop-runtime/db/market.duckdb",
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop/data",
        help="Project data root.",
    )
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compliant User-Agent, e.g. 'stock-quant-oop you@example.com'",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for testing.",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-download files even if they already exist on disk.",
    )
    parser.add_argument(
        "--cik-file",
        default=None,
        help="Optional text file with one CIK per line. If omitted, targets are read from DuckDB.",
    )
    parser.add_argument(
        "--only-submissions",
        action="store_true",
        help="Download only submissions JSON.",
    )
    parser.add_argument(
        "--only-companyfacts",
        action="store_true",
        help="Download only companyfacts JSON.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.20,
        help="Sleep between SEC requests.",
    )
    return parser.parse_args()


def _load_targets_from_cik_file(path: str | Path) -> list[SecDownloadTarget]:
    """
    Charge les CIKs depuis un fichier texte.

    Format:
    - une ligne = un CIK
    - lignes vides ignorées
    - commentaires '#' ignorés
    """
    file_path = Path(path).expanduser()
    targets: list[SecDownloadTarget] = []

    for line in file_path.read_text(encoding="utf-8").splitlines():
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        targets.append(SecDownloadTarget(cik=value))

    return targets


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _column_names(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return [str(row[0]) for row in rows]


def _load_targets_from_db(db_path: str | Path, limit: int | None) -> list[SecDownloadTarget]:
    """
    Charge les cibles CIK depuis la DB.

    Stratégie:
    - privilégier symbol_reference si elle contient CIK + company_id + symbol
    - fallback sur sec_filing si de la donnée SEC existe déjà
    - rester défensif car les schémas peuvent évoluer
    """
    con = duckdb.connect(str(Path(db_path).expanduser()))
    try:
        if _table_exists(con, "symbol_reference"):
            cols = set(_column_names(con, "symbol_reference"))

            cik_col = "cik" if "cik" in cols else None
            company_col = "company_id" if "company_id" in cols else None
            symbol_col = "symbol" if "symbol" in cols else None

            if cik_col is not None:
                select_parts = [f"{cik_col} AS cik"]
                select_parts.append(f"{company_col} AS company_id" if company_col else "NULL AS company_id")
                select_parts.append(f"{symbol_col} AS symbol" if symbol_col else "NULL AS symbol")

                sql = f"""
                    SELECT DISTINCT
                        {", ".join(select_parts)}
                    FROM symbol_reference
                    WHERE {cik_col} IS NOT NULL
                      AND TRIM(CAST({cik_col} AS VARCHAR)) <> ''
                    ORDER BY cik
                """
                if limit is not None:
                    sql += f" LIMIT {int(limit)}"

                rows = con.execute(sql).fetchall()
                return [
                    SecDownloadTarget(
                        cik=str(row[0]).strip(),
                        company_id=None if row[1] is None else str(row[1]).strip(),
                        symbol=None if row[2] is None else str(row[2]).strip(),
                    )
                    for row in rows
                ]

        if _table_exists(con, "sec_filing"):
            rows = con.execute(
                """
                SELECT DISTINCT
                    cik,
                    company_id,
                    NULL AS symbol
                FROM sec_filing
                WHERE cik IS NOT NULL
                  AND TRIM(CAST(cik AS VARCHAR)) <> ''
                ORDER BY cik
                """
                + (f" LIMIT {int(limit)}" if limit is not None else "")
            ).fetchall()

            return [
                SecDownloadTarget(
                    cik=str(row[0]).strip(),
                    company_id=None if row[1] is None else str(row[1]).strip(),
                    symbol=None,
                )
                for row in rows
            ]

        return []
    finally:
        con.close()


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser()
    data_root = Path(args.data_root).expanduser()

    include_submissions = not args.only_companyfacts
    include_companyfacts = not args.only_submissions

    if args.cik_file:
        targets = _load_targets_from_cik_file(args.cik_file)
    else:
        targets = _load_targets_from_db(db_path=db_path, limit=args.limit)

    if args.limit is not None and args.cik_file:
        targets = targets[: int(args.limit)]

    print(f"[run_sec_download_history] db_path={db_path}")
    print(f"[run_sec_download_history] data_root={data_root}")
    print(f"[run_sec_download_history] targets={len(targets)}")
    print(f"[run_sec_download_history] include_submissions={include_submissions}")
    print(f"[run_sec_download_history] include_companyfacts={include_companyfacts}")
    print(f"[run_sec_download_history] refresh_existing={bool(args.refresh_existing)}")
    print(f"[run_sec_download_history] sleep_seconds={args.sleep_seconds}")

    if not targets:
        print("[run_sec_download_history] no targets found")
        return 0

    provider = SecBulkHistoryProvider(
        data_root=data_root,
        user_agent=args.user_agent,
        sleep_seconds=args.sleep_seconds,
    )

    metrics = provider.download_history_for_targets(
        targets=targets,
        include_submissions=include_submissions,
        include_companyfacts=include_companyfacts,
        refresh_existing=bool(args.refresh_existing),
        show_progress=True,
    )

    print("[run_sec_download_history] metrics=" + json.dumps(metrics, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
