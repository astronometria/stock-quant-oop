#!/usr/bin/env python3
from __future__ import annotations

"""
load_market_universe_history_from_nasdaq_snapshots.py

Objectif
--------
Charger des snapshots observés Nasdaq Symbol Directory depuis :

- nasdaqlisted_YYYY-MM-DD.csv
- otherlisted_YYYY-MM-DD.csv

vers la table legacy actuelle : market_universe_history

Important
---------
Le schéma runtime observé dans la DB actuelle est legacy :

market_universe_history(
    listing_id,
    company_id,
    symbol,
    cik,
    exchange,
    security_type,
    eligible_flag,
    eligible_reason,
    rule_version,
    effective_from,
    effective_to,
    is_active,
    created_at,
    updated_at
)

Ce script reste compatible avec ce schéma exact.

Philosophie
-----------
- SQL-first
- non destructif
- append-only au niveau des observations
- pas d'inférence agressive
- on charge uniquement des snapshots observés
- on ne ferme pas d'intervalles historiques ici

Convention retenue
------------------
Comme la table legacy n'a pas une vraie structure "snapshot_row_id/source_name/source_ref",
on encode l'observation comme suit :

- listing_id = identifiant déterministe du snapshot observé
- effective_from = date du snapshot
- effective_to = date du snapshot
- is_active = TRUE
- eligible_flag = TRUE
- eligible_reason = 'OBSERVED_IN_NASDAQ_SYMBOL_DIRECTORY_SNAPSHOT'
- rule_version = 'nasdaq_symbol_directory_snapshot_v1'

Ainsi :
- chaque ligne représente une observation datée
- aucune continuité n'est inventée
- une même valeur peut être observée plusieurs jours différents

Audit
-----
On écrit aussi une ligne dans history_reconstruction_audit pour chaque insertion logique.
"""

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Iterable

import duckdb
from tqdm import tqdm


# ============================================================================
# Helpers généraux
# ============================================================================


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Valeur par défaut alignée sur ce que ton probe a trouvé :
    ~/stock-quant-oop/data/symbol_sources/nasdaq
    """
    parser = argparse.ArgumentParser(
        description="Load observed Nasdaq symbol directory snapshots into legacy market_universe_history."
    )
    parser.add_argument("--db-path", required=True, help="Path to the DuckDB database.")
    parser.add_argument(
        "--snapshots-root",
        default="~/stock-quant-oop/data/symbol_sources/nasdaq",
        help="Directory containing nasdaqlisted_YYYY-MM-DD.csv and otherlisted_YYYY-MM-DD.csv files.",
    )
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Vérifie l'existence d'une table dans la DB.
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
    Échoue tôt si les tables attendues n'existent pas.
    """
    missing = [name for name in required if not table_exists(con, name)]
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}")


def normalize_text(value: str | None) -> str | None:
    """
    Nettoyage texte simple.
    """
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def normalize_symbol(value: str | None) -> str | None:
    """
    Nettoyage d'un ticker.
    """
    if value is None:
        return None
    cleaned = value.strip().upper()
    return cleaned or None


def normalize_boolish_to_security_type(raw_record: dict[str, str]) -> str:
    """
    Déduit un security_type simple à partir des colonnes disponibles.

    On garde volontairement une logique sobre et lisible.
    """
    etf_flag = normalize_text(raw_record.get("etf"))
    if etf_flag and etf_flag.upper() in {"Y", "YES", "TRUE", "1"}:
        return "ETF"

    test_issue = normalize_text(raw_record.get("test issue"))
    if test_issue and test_issue.upper() in {"Y", "YES", "TRUE", "1"}:
        return "TEST_ISSUE"

    return "EQUITY"


def infer_exchange_from_filename(file_name: str) -> str:
    """
    Déduit un exchange logique depuis le nom du fichier snapshot.

    Ce n'est pas une vérité absolue de marché ; c'est seulement un tag de
    provenance raisonnable pour le snapshot.
    """
    lower_name = file_name.lower()
    if "nasdaqlisted_" in lower_name:
        return "NASDAQ"
    if "otherlisted_" in lower_name:
        return "OTHERLISTED"
    return "UNKNOWN"


def extract_snapshot_date_from_filename(path: Path) -> str | None:
    """
    Extrait la date snapshot depuis un nom comme :
    - nasdaqlisted_2026-03-27.csv
    - otherlisted_2026-03-27.csv
    """
    match = re.search(r"_(\d{4}-\d{2}-\d{2})\.csv$", path.name, flags=re.IGNORECASE)
    if not match:
        return None
    return match.group(1)


def iter_candidate_files(root: Path) -> list[Path]:
    """
    Retourne les fichiers snapshots candidats.

    On ne prend que les CSV attendus pour éviter d'avaler d'autres fichiers
    non liés.
    """
    if not root.exists() or not root.is_dir():
        return []

    files: list[Path] = []
    files.extend(root.glob("nasdaqlisted_*.csv"))
    files.extend(root.glob("otherlisted_*.csv"))
    return sorted([p.resolve() for p in files if p.is_file()])


def pick_first(record: dict[str, str], keys: Iterable[str]) -> str | None:
    """
    Retourne la première valeur non vide parmi plusieurs clés candidates.
    """
    for key in keys:
        value = normalize_text(record.get(key))
        if value:
            return value
    return None


def parse_snapshot_csv(path: Path) -> list[dict]:
    """
    Parse un snapshot CSV Nasdaq symbol directory.

    Le format exact peut varier légèrement selon la source, donc on reste
    tolérant côté noms de colonnes.
    """
    snapshot_date = extract_snapshot_date_from_filename(path)
    if snapshot_date is None:
        return []

    exchange = infer_exchange_from_filename(path.name)
    rows: list[dict] = []

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            record = {
                str(k).strip().lower(): (v or "").strip()
                for k, v in raw.items()
                if k is not None
            }

            symbol = normalize_symbol(
                pick_first(
                    record,
                    [
                        "symbol",
                        "act symbol",
                        "ticker",
                        "issue symbol",
                    ],
                )
            )
            if not symbol:
                # Pas de symbole = rien à charger.
                continue

            company_name = pick_first(
                record,
                [
                    "security name",
                    "company name",
                    "issuer name",
                    "name",
                ],
            )

            cik = normalize_text(
                pick_first(
                    record,
                    [
                        "cik",
                    ],
                )
            )

            security_type = normalize_text(
                pick_first(
                    record,
                    [
                        "security type",
                        "financial status",
                    ],
                )
            )
            if not security_type:
                security_type = normalize_boolish_to_security_type(record)

            payload = {
                "source_file": path.name,
                "snapshot_date": snapshot_date,
                "exchange_inferred_from_file_name": exchange,
                "raw_record": record,
                "company_name": company_name,
            }

            # ----------------------------------------------------------------
            # listing_id déterministe pour la table legacy.
            #
            # On encode :
            # - type de source
            # - date snapshot
            # - fichier
            # - symbole
            #
            # Cela rend l'insertion idempotente à granularité snapshot+symbol.
            # ----------------------------------------------------------------
            listing_id = (
                f"NASDAQ_SNAPSHOT|{snapshot_date}|{path.name}|{symbol}"
            )

            rows.append(
                {
                    "listing_id": listing_id,
                    "company_id": None,
                    "symbol": symbol,
                    "cik": cik,
                    "exchange": exchange,
                    "security_type": security_type,
                    "eligible_flag": True,
                    "eligible_reason": "OBSERVED_IN_NASDAQ_SYMBOL_DIRECTORY_SNAPSHOT",
                    "rule_version": "nasdaq_symbol_directory_snapshot_v1",
                    "effective_from_raw": snapshot_date,
                    "effective_to_raw": snapshot_date,
                    "is_active": True,
                    "notes_json": json.dumps(payload, ensure_ascii=False),
                }
            )

    return rows


# ============================================================================
# Main
# ============================================================================


def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. validate
    2. discover files
    3. parse
    4. stage
    5. insert market_universe_history
    6. insert audit
    7. metrics
    """
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    snapshots_root = Path(args.snapshots_root).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=7, desc="load_market_universe_history_from_nasdaq_snapshots", unit="step")

    try:
        # --------------------------------------------------------------------
        # 1) Validation des tables runtime legacy
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:validate")
        require_tables(
            con,
            required=[
                "market_universe_history",
                "history_reconstruction_audit",
            ],
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 2) Découverte des fichiers snapshots
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:discover_files")
        files = iter_candidate_files(snapshots_root)
        steps.update(1)

        # --------------------------------------------------------------------
        # 3) Parsing des fichiers
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:parse")
        parsed_rows: list[dict] = []
        for file_path in files:
            parsed_rows.extend(parse_snapshot_csv(file_path))
        steps.update(1)

        # --------------------------------------------------------------------
        # 4) Staging temporaire SQL-first
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:stage")
        con.execute("DROP TABLE IF EXISTS tmp_nasdaq_snapshot_market_universe")
        con.execute(
            """
            CREATE TEMP TABLE tmp_nasdaq_snapshot_market_universe (
                listing_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                cik VARCHAR,
                exchange VARCHAR,
                security_type VARCHAR,
                eligible_flag BOOLEAN,
                eligible_reason VARCHAR,
                rule_version VARCHAR,
                effective_from_raw VARCHAR,
                effective_to_raw VARCHAR,
                is_active BOOLEAN,
                notes_json VARCHAR
            )
            """
        )

        if parsed_rows:
            con.executemany(
                """
                INSERT INTO tmp_nasdaq_snapshot_market_universe (
                    listing_id,
                    company_id,
                    symbol,
                    cik,
                    exchange,
                    security_type,
                    eligible_flag,
                    eligible_reason,
                    rule_version,
                    effective_from_raw,
                    effective_to_raw,
                    is_active,
                    notes_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row.get("listing_id"),
                        row.get("company_id"),
                        row.get("symbol"),
                        row.get("cik"),
                        row.get("exchange"),
                        row.get("security_type"),
                        row.get("eligible_flag"),
                        row.get("eligible_reason"),
                        row.get("rule_version"),
                        row.get("effective_from_raw"),
                        row.get("effective_to_raw"),
                        row.get("is_active"),
                        row.get("notes_json"),
                    )
                    for row in parsed_rows
                ],
            )
        steps.update(1)

        # --------------------------------------------------------------------
        # 5) Insertion non destructive dans market_universe_history
        # --------------------------------------------------------------------
        # Comme on ne connaît pas forcément les contraintes DB existantes,
        # on fait une insertion idempotente avec NOT EXISTS sur la clé logique
        # snapshot+symbol (encodée dans listing_id et effective_from).
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:apply_market_universe")
        con.execute(
            """
            INSERT INTO market_universe_history (
                listing_id,
                company_id,
                symbol,
                cik,
                exchange,
                security_type,
                eligible_flag,
                eligible_reason,
                rule_version,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            )
            SELECT
                s.listing_id,
                s.company_id,
                s.symbol,
                s.cik,
                s.exchange,
                s.security_type,
                s.eligible_flag,
                s.eligible_reason,
                s.rule_version,
                TRY_CAST(s.effective_from_raw AS DATE) AS effective_from,
                TRY_CAST(s.effective_to_raw AS DATE) AS effective_to,
                s.is_active,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_snapshot_market_universe
            ) s
            WHERE
                TRY_CAST(s.effective_from_raw AS DATE) IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM market_universe_history t
                    WHERE COALESCE(t.listing_id, '') = COALESCE(s.listing_id, '')
                      AND COALESCE(t.symbol, '') = COALESCE(s.symbol, '')
                      AND COALESCE(t.exchange, '') = COALESCE(s.exchange, '')
                      AND COALESCE(t.security_type, '') = COALESCE(s.security_type, '')
                      AND COALESCE(t.eligible_reason, '') = COALESCE(s.eligible_reason, '')
                      AND t.effective_from = TRY_CAST(s.effective_from_raw AS DATE)
                      AND COALESCE(t.effective_to, TRY_CAST(s.effective_to_raw AS DATE)) = COALESCE(
                          TRY_CAST(s.effective_to_raw AS DATE),
                          TRY_CAST(s.effective_from_raw AS DATE)
                      )
                )
            """
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 6) Audit append-only
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:apply_audit")
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
                        COALESCE(s.listing_id, '') || '|' ||
                        COALESCE(s.symbol, '') || '|' ||
                        COALESCE(s.exchange, '') || '|' ||
                        COALESCE(s.effective_from_raw, '')
                    )
                ) AS audit_id,
                'market_universe_history' AS entity_type,
                COALESCE(s.listing_id, '') AS entity_key,
                'INSERT' AS action_type,
                'nasdaq_symbol_directory_snapshot' AS source_name,
                'DIRECT_SOURCE_FILE' AS evidence_type,
                'HIGH' AS confidence_level,
                NULL AS old_value,
                s.symbol AS new_value,
                s.notes_json AS notes,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT DISTINCT *
                FROM tmp_nasdaq_snapshot_market_universe
            ) s
            WHERE
                TRY_CAST(s.effective_from_raw AS DATE) IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM history_reconstruction_audit a
                    WHERE COALESCE(a.entity_type, '') = 'market_universe_history'
                      AND COALESCE(a.entity_key, '') = COALESCE(s.listing_id, '')
                      AND COALESCE(a.action_type, '') = 'INSERT'
                      AND COALESCE(a.source_name, '') = 'nasdaq_symbol_directory_snapshot'
                )
            """
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 7) Métriques finales
        # --------------------------------------------------------------------
        steps.set_description("load_market_universe_history_from_nasdaq_snapshots:metrics")
        file_count = len(files)
        parsed_count = len(parsed_rows)

        market_rows_total = con.execute(
            """
            SELECT COUNT(*)
            FROM market_universe_history
            WHERE rule_version = 'nasdaq_symbol_directory_snapshot_v1'
            """
        ).fetchone()[0]

        audit_rows_total = con.execute(
            """
            SELECT COUNT(*)
            FROM history_reconstruction_audit
            WHERE source_name = 'nasdaq_symbol_directory_snapshot'
            """
        ).fetchone()[0]

        distinct_symbols = con.execute(
            """
            SELECT COUNT(DISTINCT symbol)
            FROM market_universe_history
            WHERE rule_version = 'nasdaq_symbol_directory_snapshot_v1'
            """
        ).fetchone()[0]

        snapshot_dates = con.execute(
            """
            SELECT
                MIN(effective_from),
                MAX(effective_from),
                COUNT(DISTINCT effective_from)
            FROM market_universe_history
            WHERE rule_version = 'nasdaq_symbol_directory_snapshot_v1'
            """
        ).fetchone()

        steps.update(1)

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "snapshots_root": str(snapshots_root),
                    "files_discovered": int(file_count),
                    "parsed_rows": int(parsed_count),
                    "market_universe_rows_total_for_nasdaq_snapshots": int(market_rows_total),
                    "audit_rows_total_for_nasdaq_snapshots": int(audit_rows_total),
                    "distinct_symbols_for_nasdaq_snapshots": int(distinct_symbols),
                    "min_snapshot_date": str(snapshot_dates[0]) if snapshot_dates and snapshot_dates[0] is not None else None,
                    "max_snapshot_date": str(snapshot_dates[1]) if snapshot_dates and snapshot_dates[1] is not None else None,
                    "distinct_snapshot_dates": int(snapshot_dates[2]) if snapshot_dates and snapshot_dates[2] is not None else 0,
                    "mode": "legacy_schema_compatible_observed_snapshot_loader_sql_first",
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
