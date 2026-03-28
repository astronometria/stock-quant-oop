#!/usr/bin/env python3
from __future__ import annotations

"""
apply_listing_events_from_nasdaq_daily_list.py

Script prudent / SQL-first / non destructif.
"""

import argparse
import csv
import json
import re
from pathlib import Path

import duckdb
from tqdm import tqdm


EVENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("LISTING_ADDED", re.compile(r"\b(new listing|listed|addition|added)\b", re.IGNORECASE)),
    ("DELISTED", re.compile(r"\b(delist|delisting|deleted|removal|removed)\b", re.IGNORECASE)),
    ("RENAMED", re.compile(r"\b(name change|rename|renamed)\b", re.IGNORECASE)),
    ("TICKER_CHANGED", re.compile(r"\b(symbol change|ticker change|change symbol)\b", re.IGNORECASE)),
    ("REACTIVATED", re.compile(r"\b(reactivat|reinstated)\b", re.IGNORECASE)),
    ("SUSPENDED", re.compile(r"\b(suspend|halted)\b", re.IGNORECASE)),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply listing events from local Nasdaq Daily List files into listing_event_history."
    )
    parser.add_argument("--db-path", required=True, help="Path to the DuckDB database.")
    parser.add_argument(
        "--daily-list-root",
        default="~/stock-quant-oop/data/nasdaq_daily_list",
        help="Directory containing local Nasdaq Daily List files.",
    )
    return parser.parse_args()


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
    normalized = (text_blob or "").strip()
    if not normalized:
        return None

    for event_type, pattern in EVENT_PATTERNS:
        if pattern.search(normalized):
            return event_type

    return None


def normalize_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized or None


def extract_first_symbol(text: str) -> str | None:
    match = re.search(r"\b([A-Z][A-Z0-9\.\-]{0,9})\b", text)
    if not match:
        return None
    return normalize_symbol(match.group(1))


def iter_candidate_files(root: Path) -> list[Path]:
    patterns = ["**/*.txt", "**/*.csv", "**/*.tsv"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(root.glob(pattern))
    return sorted([p.resolve() for p in files if p.is_file()])


def parse_date_string(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    value = raw_value.strip()
    return value or None


def build_row_from_csv_record(path: Path, normalized: dict[str, str]) -> dict | None:
    notes_blob = " | ".join(value for value in normalized.values() if value)
    event_type = infer_event_type(notes_blob)
    if event_type is None:
        return None

    old_symbol = normalize_symbol(
        normalized.get("old symbol")
        or normalized.get("old_symbol")
        or normalized.get("from symbol")
        or normalized.get("from_symbol")
    )
    new_symbol = normalize_symbol(
        normalized.get("new symbol")
        or normalized.get("new_symbol")
        or normalized.get("to symbol")
        or normalized.get("to_symbol")
    )
    primary_symbol = normalize_symbol(
        normalized.get("symbol")
        or normalized.get("ticker")
        or normalized.get("issue symbol")
        or normalized.get("issue_symbol")
    )

    if event_type == "TICKER_CHANGED":
        symbol = new_symbol or primary_symbol
        related_symbol = old_symbol
    else:
        symbol = primary_symbol or new_symbol or old_symbol
        related_symbol = old_symbol if symbol != old_symbol else new_symbol

    if not symbol:
        return None

    event_date_raw = parse_date_string(
        normalized.get("date")
        or normalized.get("effective date")
        or normalized.get("effective_date")
        or normalized.get("event date")
        or normalized.get("event_date")
    )

    if event_date_raw is None:
        return None

    payload = {
        "file_name": path.name,
        "path": str(path),
        "record": normalized,
    }

    return {
        "symbol": symbol,
        "related_symbol": related_symbol,
        "event_type": event_type,
        "event_date_raw": event_date_raw,
        "effective_date_raw": event_date_raw,
        "event_label": (
            normalized.get("description")
            or normalized.get("event")
            or normalized.get("action")
            or event_type
        ),
        "notes": notes_blob,
        "source_name": "nasdaq_daily_list",
        "source_table": "local_daily_list_file",
        "source_ref": f"{path.name}",
        "raw_payload_json": json.dumps(payload, ensure_ascii=False),
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
        for raw in reader:
            normalized = {
                str(k).strip().lower(): (v or "").strip()
                for k, v in raw.items()
                if k is not None
            }
            built = build_row_from_csv_record(path=path, normalized=normalized)
            if built is not None:
                rows.append(built)

    return rows


def parse_text_line(path: Path, line: str) -> dict | None:
    text = line.strip()
    if not text:
        return None

    event_type = infer_event_type(text)
    if event_type is None:
        return None

    date_match = re.search(
        r"\b(\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{4}/\d{2}/\d{2})\b",
        text,
    )
    event_date_raw = date_match.group(1) if date_match else None
    if event_date_raw is None:
        return None

    symbol = extract_first_symbol(text)
    if symbol is None:
        return None

    payload = {
        "file_name": path.name,
        "path": str(path),
        "line": text,
    }

    return {
        "symbol": symbol,
        "related_symbol": None,
        "event_type": event_type,
        "event_date_raw": event_date_raw,
        "effective_date_raw": event_date_raw,
        "event_label": event_type,
        "notes": text,
        "source_name": "nasdaq_daily_list",
        "source_table": "local_daily_list_file",
        "source_ref": f"{path.name}",
        "raw_payload_json": json.dumps(payload, ensure_ascii=False),
    }


def parse_text_file(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            built = parse_text_line(path=path, line=line)
            if built is not None:
                rows.append(built)
    return rows


def require_tables(con: duckdb.DuckDBPyConnection, required: list[str]) -> None:
    missing = [table_name for table_name in required if not table_exists(con, table_name)]
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}")


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    daily_root = Path(args.daily_list_root).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=7, desc="apply_listing_events_from_nasdaq_daily_list", unit="step")

    try:
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:validate")
        require_tables(
            con,
            required=[
                "listing_event_history",
                "history_reconstruction_audit",
            ],
        )
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
                related_symbol VARCHAR,
                event_type VARCHAR,
                event_date_raw VARCHAR,
                effective_date_raw VARCHAR,
                event_label VARCHAR,
                notes VARCHAR,
                source_name VARCHAR,
                source_table VARCHAR,
                source_ref VARCHAR,
                raw_payload_json VARCHAR
            )
            """
        )

        if parsed_rows:
            con.executemany(
                """
                INSERT INTO tmp_nasdaq_daily_list_events (
                    symbol,
                    related_symbol,
                    event_type,
                    event_date_raw,
                    effective_date_raw,
                    event_label,
                    notes,
                    source_name,
                    source_table,
                    source_ref,
                    raw_payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("symbol"),
                        row.get("related_symbol"),
                        row.get("event_type"),
                        row.get("event_date_raw"),
                        row.get("effective_date_raw"),
                        row.get("event_label"),
                        row.get("notes"),
                        row.get("source_name"),
                        row.get("source_table"),
                        row.get("source_ref"),
                        row.get("raw_payload_json"),
                    )
                    for row in parsed_rows
                ],
            )
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:apply_events")
        con.execute(
            """
            INSERT INTO listing_event_history (
                event_id,
                symbol,
                related_symbol,
                company_id,
                cik,
                event_type,
                event_date,
                effective_date,
                source_name,
                source_table,
                source_ref,
                event_label,
                confidence_level,
                is_observed,
                is_inferred,
                raw_payload,
                notes,
                created_at
            )
            SELECT
                CONCAT(
                    'NASDAQ_DAILY_LIST:',
                    md5(
                        COALESCE(symbol, '') || '|' ||
                        COALESCE(related_symbol, '') || '|' ||
                        COALESCE(event_type, '') || '|' ||
                        COALESCE(event_date_raw, '') || '|' ||
                        COALESCE(effective_date_raw, '') || '|' ||
                        COALESCE(source_name, '') || '|' ||
                        COALESCE(source_ref, '')
                    )
                ) AS event_id,
                symbol,
                related_symbol,
                NULL AS company_id,
                NULL AS cik,
                event_type,
                TRY_CAST(event_date_raw AS DATE) AS event_date,
                TRY_CAST(effective_date_raw AS DATE) AS effective_date,
                source_name,
                source_table,
                source_ref,
                event_label,
                'MEDIUM' AS confidence_level,
                TRUE AS is_observed,
                FALSE AS is_inferred,
                TRY_CAST(raw_payload_json AS JSON) AS raw_payload,
                notes,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_daily_list_events
            ) s
            WHERE
                TRY_CAST(event_date_raw AS DATE) IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM listing_event_history t
                    WHERE t.symbol = s.symbol
                      AND COALESCE(t.related_symbol, '') = COALESCE(s.related_symbol, '')
                      AND t.event_type = s.event_type
                      AND t.event_date = TRY_CAST(s.event_date_raw AS DATE)
                      AND COALESCE(t.effective_date, t.event_date) = COALESCE(
                          TRY_CAST(s.effective_date_raw AS DATE),
                          TRY_CAST(s.event_date_raw AS DATE)
                      )
                      AND t.source_name = s.source_name
                      AND COALESCE(t.source_ref, '') = COALESCE(s.source_ref, '')
                )
            """
        )
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:apply_audit")
        run_id = con.execute(
            "SELECT CONCAT('nasdaq_daily_list_', md5(CAST(current_timestamp AS VARCHAR)))"
        ).fetchone()[0]

        con.execute(
            """
            INSERT INTO history_reconstruction_audit (
                audit_row_id,
                run_id,
                target_table,
                target_key,
                action_type,
                reason_code,
                rule_name,
                input_source_summary,
                input_row_refs,
                old_values,
                new_values,
                confidence_level,
                created_at
            )
            SELECT
                CONCAT(
                    'AUDIT:',
                    md5(
                        COALESCE(symbol, '') || '|' ||
                        COALESCE(related_symbol, '') || '|' ||
                        COALESCE(event_type, '') || '|' ||
                        COALESCE(event_date_raw, '') || '|' ||
                        COALESCE(source_name, '') || '|' ||
                        COALESCE(source_ref, '') || '|' ||
                        ?
                    )
                ) AS audit_row_id,
                ? AS run_id,
                'listing_event_history' AS target_table,
                CONCAT(
                    COALESCE(symbol, ''),
                    '|',
                    COALESCE(related_symbol, ''),
                    '|',
                    COALESCE(event_type, ''),
                    '|',
                    COALESCE(event_date_raw, '')
                ) AS target_key,
                'INSERT' AS action_type,
                'OBSERVED_SNAPSHOT' AS reason_code,
                'apply_listing_events_from_nasdaq_daily_list' AS rule_name,
                CONCAT(
                    COALESCE(source_name, ''),
                    ':',
                    COALESCE(source_ref, '')
                ) AS input_source_summary,
                TRY_CAST(
                    json_object(
                        'source_name', source_name,
                        'source_ref', source_ref,
                        'event_label', event_label
                    ) AS JSON
                ) AS input_row_refs,
                NULL AS old_values,
                TRY_CAST(
                    json_object(
                        'symbol', symbol,
                        'related_symbol', related_symbol,
                        'event_type', event_type,
                        'event_date_raw', event_date_raw,
                        'effective_date_raw', effective_date_raw,
                        'event_label', event_label
                    ) AS JSON
                ) AS new_values,
                'MEDIUM' AS confidence_level,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_daily_list_events
            ) s
            WHERE TRY_CAST(event_date_raw AS DATE) IS NOT NULL
            """,
            [run_id, run_id],
        )
        steps.update(1)

        steps.set_description("apply_listing_events_from_nasdaq_daily_list:metrics")
        file_count = len(files)
        parsed_count = len(parsed_rows)

        event_rows_total = con.execute(
            """
            SELECT COUNT(*)
            FROM listing_event_history
            WHERE source_name = 'nasdaq_daily_list'
            """
        ).fetchone()[0]

        audit_rows_total = con.execute(
            """
            SELECT COUNT(*)
            FROM history_reconstruction_audit
            WHERE target_table = 'listing_event_history'
              AND rule_name = 'apply_listing_events_from_nasdaq_daily_list'
            """
        ).fetchone()[0]

        distinct_symbols = con.execute(
            """
            SELECT COUNT(DISTINCT symbol)
            FROM listing_event_history
            WHERE source_name = 'nasdaq_daily_list'
            """
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
                    "distinct_symbols_for_nasdaq_daily_list": int(distinct_symbols),
                    "mode": "event_only_non_destructive_local_source_sql_first",
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
