#!/usr/bin/env python3
from __future__ import annotations

"""
apply_listing_events_from_nasdaq_daily_list.py

Correctif de compatibilité pour le schéma historique legacy actuellement
présent dans la DB runtime.

Constat issu du probe
---------------------
La table listing_event_history actuelle a les colonnes suivantes :
- event_id
- listing_id
- symbol
- company_id
- cik
- event_type
- event_date
- old_value
- new_value
- source_name
- evidence_type
- confidence_level
- notes
- created_at

La table history_reconstruction_audit actuelle a les colonnes suivantes :
- audit_id
- entity_type
- entity_key
- action_type
- source_name
- evidence_type
- confidence_level
- old_value
- new_value
- notes
- created_at

Objectif de ce script
---------------------
- rester non destructif
- rester SQL-first
- ne pas modifier listing_status_history directement
- parser prudemment des fichiers Nasdaq Daily List locaux
- écrire uniquement dans :
  * listing_event_history
  * history_reconstruction_audit

Convention legacy utilisée ici
------------------------------
Pour un changement de ticker :
- symbol = nouveau symbole si disponible, sinon symbole principal
- old_value = ancien symbole
- new_value = nouveau symbole

Pour les autres événements :
- symbol = symbole principal
- old_value / new_value restent optionnels selon les colonnes source trouvées
"""

import argparse
import csv
import json
import re
from pathlib import Path

import duckdb
from tqdm import tqdm


# ============================================================================
# Détection prudente des événements depuis le texte brut.
# ============================================================================
EVENT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("LISTING_ADDED", re.compile(r"\b(new listing|listed|addition|added)\b", re.IGNORECASE)),
    ("LISTING_REMOVED", re.compile(r"\b(removal|removed|deleted)\b", re.IGNORECASE)),
    ("DELISTED", re.compile(r"\b(delist|delisting)\b", re.IGNORECASE)),
    ("RENAMED", re.compile(r"\b(name change|rename|renamed)\b", re.IGNORECASE)),
    ("TICKER_CHANGED", re.compile(r"\b(symbol change|ticker change|change symbol)\b", re.IGNORECASE)),
    ("REACTIVATED", re.compile(r"\b(reactivat|reinstated)\b", re.IGNORECASE)),
    ("SUSPENDED", re.compile(r"\b(suspend|halted)\b", re.IGNORECASE)),
]


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    On garde une interface simple pour l'orchestration.
    """
    parser = argparse.ArgumentParser(
        description="Apply listing events from local Nasdaq Daily List files into legacy listing_event_history."
    )
    parser.add_argument("--db-path", required=True, help="Path to the DuckDB database.")
    parser.add_argument(
        "--daily-list-root",
        default="~/stock-quant-oop/data/nasdaq_daily_list",
        help="Directory containing local Nasdaq Daily List files.",
    )
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Vérifie si une table existe.
    """
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def require_tables(con: duckdb.DuckDBPyConnection, required: list[str]) -> None:
    """
    Valide la présence des tables requises.
    """
    missing = [table_name for table_name in required if not table_exists(con, table_name)]
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}")


def normalize_symbol(value: str | None) -> str | None:
    """
    Nettoie un ticker.
    """
    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized or None


def parse_date_string(raw_value: str | None) -> str | None:
    """
    Nettoie une date texte.

    La conversion réelle vers DATE est laissée à DuckDB via TRY_CAST
    pour garder la logique SQL-first.
    """
    if raw_value is None:
        return None
    value = raw_value.strip()
    return value or None


def infer_event_type(text_blob: str) -> str | None:
    """
    Déduit un type d'événement depuis un blob texte.

    Si rien n'est crédible, on retourne None.
    """
    normalized = (text_blob or "").strip()
    if not normalized:
        return None

    for event_type, pattern in EVENT_PATTERNS:
        if pattern.search(normalized):
            return event_type

    return None


def extract_first_symbol(text: str) -> str | None:
    """
    Extrait un ticker plausible d'une ligne de texte libre.

    Heuristique prudente :
    - commence par une lettre
    - jusqu'à 10 caractères alnum / . / -
    """
    match = re.search(r"\b([A-Z][A-Z0-9\.\-]{0,9})\b", text)
    if not match:
        return None
    return normalize_symbol(match.group(1))


def iter_candidate_files(root: Path) -> list[Path]:
    """
    Liste récursivement les fichiers candidats.
    """
    patterns = ["**/*.txt", "**/*.csv", "**/*.tsv"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(root.glob(pattern))
    return sorted([p.resolve() for p in files if p.is_file()])


def build_row_from_csv_record(path: Path, normalized: dict[str, str]) -> dict | None:
    """
    Construit une ligne standardisée depuis un enregistrement CSV/TSV.

    Cette fonction mappe le contenu source vers le contrat legacy actuel.
    """
    notes_blob = " | ".join(value for value in normalized.values() if value)
    event_type = infer_event_type(notes_blob)
    if event_type is None:
        return None

    # ------------------------------------------------------------------------
    # Extraction prudente des différents symboles potentiels.
    # ------------------------------------------------------------------------
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

    # ------------------------------------------------------------------------
    # Convention :
    # - pour TICKER_CHANGED, symbol = nouveau symbole quand disponible
    # - old_value/new_value conservent l'information brute de transition
    # ------------------------------------------------------------------------
    if event_type == "TICKER_CHANGED":
        symbol = new_symbol or primary_symbol or old_symbol
        old_value = old_symbol
        new_value = new_symbol or symbol
    else:
        symbol = primary_symbol or new_symbol or old_symbol
        old_value = old_symbol
        new_value = new_symbol

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
        "listing_id": None,
        "symbol": symbol,
        "company_id": None,
        "cik": None,
        "event_type": event_type,
        "event_date_raw": event_date_raw,
        "old_value": old_value,
        "new_value": new_value,
        "source_name": "nasdaq_daily_list",
        "evidence_type": "DIRECT_SOURCE_FILE",
        "confidence_level": "MEDIUM",
        "notes": json.dumps(
            {
                "event_label": (
                    normalized.get("description")
                    or normalized.get("event")
                    or normalized.get("action")
                    or event_type
                ),
                "notes_blob": notes_blob,
                "raw_payload": payload,
            },
            ensure_ascii=False,
        ),
    }


def parse_csv_file(path: Path) -> list[dict]:
    """
    Parse un CSV/TSV de manière simple et robuste.
    """
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
    """
    Parse une ligne texte libre.

    On reste très prudent :
    - il faut un type d'événement détecté
    - il faut une date parseable
    - il faut un symbole plausible
    """
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
        "listing_id": None,
        "symbol": symbol,
        "company_id": None,
        "cik": None,
        "event_type": event_type,
        "event_date_raw": event_date_raw,
        "old_value": None,
        "new_value": None,
        "source_name": "nasdaq_daily_list",
        "evidence_type": "DIRECT_SOURCE_FILE",
        "confidence_level": "LOW",
        "notes": json.dumps(
            {
                "event_label": event_type,
                "notes_blob": text,
                "raw_payload": payload,
            },
            ensure_ascii=False,
        ),
    }


def parse_text_file(path: Path) -> list[dict]:
    """
    Parse un fichier texte libre ligne par ligne.
    """
    rows: list[dict] = []
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            built = parse_text_line(path=path, line=line)
            if built is not None:
                rows.append(built)
    return rows


def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. validate
    2. discover files
    3. parse
    4. stage
    5. insert events
    6. insert audit
    7. metrics
    """
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    daily_root = Path(args.daily_list_root).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=7, desc="apply_listing_events_from_nasdaq_daily_list", unit="step")

    try:
        # --------------------------------------------------------------------
        # 1) Validation du schéma runtime existant
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:validate")
        require_tables(
            con,
            required=[
                "listing_event_history",
                "history_reconstruction_audit",
            ],
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 2) Découverte des fichiers locaux
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:discover_files")
        files = iter_candidate_files(daily_root) if daily_root.exists() else []
        steps.update(1)

        # --------------------------------------------------------------------
        # 3) Parsing local
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:parse")
        parsed_rows: list[dict] = []
        for file_path in files:
            suffix = file_path.suffix.lower()
            if suffix in {".csv", ".tsv"}:
                parsed_rows.extend(parse_csv_file(file_path))
            else:
                parsed_rows.extend(parse_text_file(file_path))
        steps.update(1)

        # --------------------------------------------------------------------
        # 4) Staging temporaire
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:stage")
        con.execute("DROP TABLE IF EXISTS tmp_nasdaq_daily_list_events")
        con.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_daily_list_events (
                listing_id VARCHAR,
                symbol VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                event_type VARCHAR,
                event_date_raw VARCHAR,
                old_value VARCHAR,
                new_value VARCHAR,
                source_name VARCHAR,
                evidence_type VARCHAR,
                confidence_level VARCHAR,
                notes VARCHAR
            )
            """
        )

        if parsed_rows:
            con.executemany(
                """
                INSERT INTO tmp_nasdaq_daily_list_events (
                    listing_id,
                    symbol,
                    company_id,
                    cik,
                    event_type,
                    event_date_raw,
                    old_value,
                    new_value,
                    source_name,
                    evidence_type,
                    confidence_level,
                    notes
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("listing_id"),
                        row.get("symbol"),
                        row.get("company_id"),
                        row.get("cik"),
                        row.get("event_type"),
                        row.get("event_date_raw"),
                        row.get("old_value"),
                        row.get("new_value"),
                        row.get("source_name"),
                        row.get("evidence_type"),
                        row.get("confidence_level"),
                        row.get("notes"),
                    )
                    for row in parsed_rows
                ],
            )
        steps.update(1)

        # --------------------------------------------------------------------
        # 5) Insertion append-only dans la table legacy
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:apply_events")
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
                old_value,
                new_value,
                source_name,
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
                        COALESCE(old_value, '') || '|' ||
                        COALESCE(new_value, '') || '|' ||
                        COALESCE(source_name, '')
                    )
                ) AS event_id,
                listing_id,
                symbol,
                company_id,
                cik,
                event_type,
                TRY_CAST(event_date_raw AS DATE) AS event_date,
                old_value,
                new_value,
                source_name,
                evidence_type,
                confidence_level,
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
                    WHERE COALESCE(t.symbol, '') = COALESCE(s.symbol, '')
                      AND COALESCE(t.event_type, '') = COALESCE(s.event_type, '')
                      AND t.event_date = TRY_CAST(s.event_date_raw AS DATE)
                      AND COALESCE(t.old_value, '') = COALESCE(s.old_value, '')
                      AND COALESCE(t.new_value, '') = COALESCE(s.new_value, '')
                      AND COALESCE(t.source_name, '') = COALESCE(s.source_name, '')
                )
            """
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 6) Audit legacy append-only
        # --------------------------------------------------------------------
        steps.set_description("apply_listing_events_from_nasdaq_daily_list:apply_audit")
        con.execute(
            """
            INSERT INTO history_reconstruction_audit (
                audit_id,
                entity_type,
                entity_key,
                action_type,
                source_name,
                evidence_type,
                confidence_level,
                old_value,
                new_value,
                notes,
                created_at
            )
            SELECT
                CONCAT(
                    'AUDIT:',
                    md5(
                        COALESCE(symbol, '') || '|' ||
                        COALESCE(event_type, '') || '|' ||
                        COALESCE(event_date_raw, '') || '|' ||
                        COALESCE(old_value, '') || '|' ||
                        COALESCE(new_value, '') || '|' ||
                        COALESCE(source_name, '')
                    )
                ) AS audit_id,
                'listing_event_history' AS entity_type,
                CONCAT(
                    COALESCE(symbol, ''),
                    '|',
                    COALESCE(event_type, ''),
                    '|',
                    COALESCE(event_date_raw, '')
                ) AS entity_key,
                'INSERT' AS action_type,
                source_name,
                evidence_type,
                confidence_level,
                old_value,
                new_value,
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
                    FROM history_reconstruction_audit a
                    WHERE COALESCE(a.entity_type, '') = 'listing_event_history'
                      AND COALESCE(a.entity_key, '') = CONCAT(
                          COALESCE(s.symbol, ''),
                          '|',
                          COALESCE(s.event_type, ''),
                          '|',
                          COALESCE(s.event_date_raw, '')
                      )
                      AND COALESCE(a.action_type, '') = 'INSERT'
                      AND COALESCE(a.source_name, '') = COALESCE(s.source_name, '')
                      AND COALESCE(a.old_value, '') = COALESCE(s.old_value, '')
                      AND COALESCE(a.new_value, '') = COALESCE(s.new_value, '')
                )
            """
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 7) Métriques finales
        # --------------------------------------------------------------------
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
            WHERE source_name = 'nasdaq_daily_list'
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
                    "mode": "legacy_schema_compatible_non_destructive_sql_first",
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
