#!/usr/bin/env python3
from __future__ import annotations

"""
apply_listing_events_from_nasdaq_daily_list.py

Version prudente / production scientifique:
- cherche des fichiers Daily List Nasdaq dans un répertoire local
- parse les fichiers texte / csv s'ils existent
- n'invente rien si aucune source n'est disponible
- écrit uniquement dans listing_event_history + history_reconstruction_audit
- ne modifie pas directement listing_status_history

But:
- construire une couche event-derived séparée et auditable
- préparer le repo à une reconstruction historique plus défendable
"""

import argparse
import csv
import json
import re
from pathlib import Path
from uuid import uuid4

import duckdb
from tqdm import tqdm


EVENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("NASDAQ_NEW_LISTING", re.compile(r"\b(new listing|listed)\b", re.IGNORECASE)),
    ("NASDAQ_DELISTING", re.compile(r"\b(delist|delisting|deleted)\b", re.IGNORECASE)),
    ("NASDAQ_NAME_CHANGE", re.compile(r"\b(name change)\b", re.IGNORECASE)),
    ("NASDAQ_TICKER_CHANGE", re.compile(r"\b(symbol change|ticker change)\b", re.IGNORECASE)),
    ("NASDAQ_EXCHANGE_CHANGE", re.compile(r"\b(exchange change|transfer)\b", re.IGNORECASE)),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Apply listing events from local Nasdaq Daily List files into listing_event_history."
    )
    p.add_argument("--db-path", required=True)
    p.add_argument(
        "--daily-list-root",
        default="~/stock-quant-oop/data/nasdaq_daily_list",
        help="Directory containing local Nasdaq Daily List files.",
    )
    return p.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def infer_event_type(text_blob: str) -> str | None:
    normalized = text_blob.strip()
    if not normalized:
        return None
    for event_type, pattern in EVENT_PATTERNS:
        if pattern.search(normalized):
            return event_type
    return None


def iter_candidate_files(root: Path) -> list[Path]:
    patterns = ["**/*.txt", "**/*.csv", "**/*.tsv"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(root.glob(pattern))
    return sorted([p.resolve() for p in files if p.is_file()])


def parse_text_line(line: str) -> dict | None:
    """
    Parse minimaliste:
    - extrait un ticker si présent
    - classe l'événement par regex
    - laisse old/new symbol/name/exchange à NULL si absent

    Cette version vise surtout l'audit et la sécurité scientifique.
    """
    text = line.strip()
    if not text:
        return None

    event_type = infer_event_type(text)
    if event_type is None:
        return None

    # Ticker simple style uppercase 1-5 chars, parfois avec suffixes.
    ticker_match = re.search(r"\b([A-Z][A-Z0-9\.\-]{0,9})\b", text)
    symbol = ticker_match.group(1) if ticker_match else None

    return {
        "symbol": symbol,
        "event_type": event_type,
        "notes": text,
    }


def parse_csv_file(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)

        delimiter = ","
        if "\t" in sample and sample.count("\t") > sample.count(","):
            delimiter = "\t"

        reader = csv.DictReader(handle, delimiter=delimiter)
        fieldnames = {str(x).strip().lower() for x in (reader.fieldnames or [])}

        for raw in reader:
            normalized = {str(k).strip().lower(): (v or "").strip() for k, v in raw.items()}

            symbol = (
                normalized.get("symbol")
                or normalized.get("ticker")
                or normalized.get("new symbol")
                or normalized.get("issue symbol")
                or ""
            ).strip().upper() or None

            notes_blob = " | ".join(
                value for value in normalized.values() if value
            )
            event_type = infer_event_type(notes_blob)
            if event_type is None:
                continue

            event_date = (
                normalized.get("date")
                or normalized.get("effective date")
                or normalized.get("event date")
                or None
            )

            rows.append(
                {
                    "symbol": symbol,
                    "event_type": event_type,
                    "event_date_raw": event_date,
                    "old_symbol": normalized.get("old symbol") or None,
                    "new_symbol": normalized.get("new symbol") or None,
                    "old_name": normalized.get("old name") or None,
                    "new_name": normalized.get("new name") or None,
                    "old_exchange": normalized.get("old exchange") or None,
                    "new_exchange": normalized.get("new exchange") or None,
                    "notes": notes_blob,
                }
            )

    return rows


def parse_text_file(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            parsed = parse_text_line(line)
            if parsed is None:
                continue
            rows.append(parsed)
    return rows


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    daily_root = Path(args.daily_list_root).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=6, desc="apply_listing_events_from_nasdaq_daily_list", unit="step")

    try:
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:validate")
        required = ["listing_event_history", "history_reconstruction_audit"]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:discover_files")
        files = iter_candidate_files(daily_root) if daily_root.exists() else []
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:parse")
        parsed_rows: list[dict] = []
        for file_path in files:
            suffix = file_path.suffix.lower()
            if suffix in {".csv", ".tsv"}:
                parsed_rows.extend(parse_csv_file(file_path))
            else:
                parsed_rows.extend(parse_text_file(file_path))
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:stage")
        con.execute("DROP TABLE IF EXISTS tmp_nasdaq_daily_list_events")
        con.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_daily_list_events (
                symbol VARCHAR,
                event_type VARCHAR,
                event_date_raw VARCHAR,
                old_symbol VARCHAR,
                new_symbol VARCHAR,
                old_name VARCHAR,
                new_name VARCHAR,
                old_exchange VARCHAR,
                new_exchange VARCHAR,
                notes VARCHAR,
                source_name VARCHAR,
                source_url VARCHAR
            )
            """
        )
        if parsed_rows:
            con.executemany(
                """
                INSERT INTO tmp_nasdaq_daily_list_events (
                    symbol,
                    event_type,
                    event_date_raw,
                    old_symbol,
                    new_symbol,
                    old_name,
                    new_name,
                    old_exchange,
                    new_exchange,
                    notes,
                    source_name,
                    source_url
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("symbol"),
                        row.get("event_type"),
                        row.get("event_date_raw"),
                        row.get("old_symbol"),
                        row.get("new_symbol"),
                        row.get("old_name"),
                        row.get("new_name"),
                        row.get("old_exchange"),
                        row.get("new_exchange"),
                        row.get("notes"),
                        "nasdaq_daily_list",
                        None,
                    )
                    for row in parsed_rows
                ],
            )
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:apply")
        run_id = f"nasdaq_daily_list_{uuid4().hex}"

        con.execute(
            """
            INSERT INTO listing_event_history (
                event_id,
                listing_id,
                symbol,
                company_id,
                cik,
                event_type,
                event_date,
                old_symbol,
                new_symbol,
                old_name,
                new_name,
                old_exchange,
                new_exchange,
                source_name,
                source_url,
                evidence_type,
                confidence_level,
                notes,
                created_at
            )
            SELECT
                CONCAT(
                    'NASDAQ_DAILY_LIST:',
                    md5(
                        COALESCE(symbol, '') || '|' ||
                        COALESCE(event_type, '') || '|' ||
                        COALESCE(event_date_raw, '') || '|' ||
                        COALESCE(notes, '')
                    )
                ) AS event_id,
                NULL AS listing_id,
                symbol,
                NULL AS company_id,
                NULL AS cik,
                event_type,
                TRY_CAST(event_date_raw AS DATE) AS event_date,
                old_symbol,
                new_symbol,
                old_name,
                new_name,
                old_exchange,
                new_exchange,
                source_name,
                source_url,
                'nasdaq_daily_list_local_file' AS evidence_type,
                'MEDIUM' AS confidence_level,
                notes,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_daily_list_events
            )
            """
        )

        con.execute(
            """
            INSERT INTO history_reconstruction_audit (
                run_id,
                entity_type,
                entity_key,
                action_type,
                source_name,
                evidence_type,
                confidence_level,
                notes,
                created_at
            )
            SELECT
                ? AS run_id,
                'listing_event_history' AS entity_type,
                COALESCE(symbol, '') AS entity_key,
                event_type AS action_type,
                source_name,
                'nasdaq_daily_list_local_file' AS evidence_type,
                'MEDIUM' AS confidence_level,
                notes,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_daily_list_events
            )
            """,
            [run_id],
        )
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:metrics")
        file_count = len(files)
        parsed_count = len(parsed_rows)
        event_rows_total = con.execute(
            "SELECT COUNT(*) FROM listing_event_history WHERE source_name = 'nasdaq_daily_list'"
        ).fetchone()[0]
        audit_rows_total = con.execute(
            "SELECT COUNT(*) FROM history_reconstruction_audit WHERE source_name = 'nasdaq_daily_list'"
        ).fetchone()[0]
        steps.update(1)

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "daily_list_root": str(daily_root),
                    "files_discovered": int(file_count),
                    "parsed_rows": int(parsed_count),
                    "event_rows_total_for_nasdaq_daily_list": int(event_rows_total),
                    "audit_rows_total_for_nasdaq_daily_list": int(audit_rows_total),
                    "mode": "event_only_non_destructive_local_source",
                },
                indent=2,
            )
        )
        return 0
    finally:
        steps.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
