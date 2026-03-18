#!/usr/bin/env python3
from __future__ import annotations

"""
Orchestrateur quotidien incrémental.

Objectif
--------
Lancer, en séquence :
- prix
- FINRA daily short volume
- FINRA short interest
- short features
- SEC filings

Version incrémentale
--------------------
Cette version ajoute une couche SQL-first minimale :
- table de watermarks dans DuckDB
- skip de certains steps coûteux si les signatures source n'ont pas changé

Pourquoi ici ?
--------------
Le gain le plus rapide et le plus sûr est d'éviter les rebuilds inutiles dans
l'orchestrateur, sans réécrire toute la logique métier des pipelines existants.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import duckdb


PROJECT_ROOT = Path("/home/marty/stock-quant-oop")
DB_PATH = PROJECT_ROOT / "market.duckdb"
LOG_DIR = PROJECT_ROOT / "logs"


def utc_stamp() -> str:
    """Timestamp UTC compact pour nommer les logs."""
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def run_step(step_name: str, cmd: list[str]) -> None:
    """
    Exécute une étape en streaming stdout/stderr.

    Détails importants :
    - sortie affichée en live pour garder tqdm visible
    - sortie aussi persistée dans un log dédié
    - arrêt immédiat si l'étape échoue
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"{step_name}_{utc_stamp()}.log"

    print(f"\n===== RUN STEP: {step_name} =====", flush=True)
    print("cmd =", " ".join(cmd), flush=True)
    print("log =", str(log_path), flush=True)

    with log_path.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None

        for line in process.stdout:
            print(line, end="", flush=True)
            handle.write(line)

        return_code = process.wait()

    if return_code != 0:
        print(f"\n❌ STEP FAILED: {step_name} (exit={return_code})", flush=True)
        raise SystemExit(return_code)

    print(f"✅ STEP OK: {step_name}", flush=True)


def connect_db() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB dédiée aux probes de pilotage."""
    return duckdb.connect(str(DB_PATH))


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Teste l'existence d'une table sans planter l'orchestrateur."""
    rows = con.execute("SHOW TABLES").fetchall()
    existing = {str(row[0]).strip().lower() for row in rows}
    return table_name.strip().lower() in existing


def ensure_pipeline_watermark_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Table SQL-first minimale pour mémoriser les signatures de sources.

    Cette table ne remplace pas la logique métier des pipelines.
    Elle sert seulement à décider rapidement si un step coûteux mérite
    d'être relancé.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_run_watermark (
            pipeline_name VARCHAR PRIMARY KEY,
            source_signature VARCHAR NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """
    )


def read_saved_signature(
    con: duckdb.DuckDBPyConnection,
    pipeline_name: str,
) -> str | None:
    """Lit la dernière signature source connue pour un pipeline."""
    row = con.execute(
        """
        SELECT source_signature
        FROM pipeline_run_watermark
        WHERE pipeline_name = ?
        """,
        [pipeline_name],
    ).fetchone()
    return None if row is None else str(row[0])


def write_saved_signature(
    con: duckdb.DuckDBPyConnection,
    pipeline_name: str,
    source_signature: str,
) -> None:
    """Upsert de la signature source après exécution réussie."""
    con.execute(
        """
        INSERT INTO pipeline_run_watermark (
            pipeline_name,
            source_signature,
            updated_at
        )
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT (pipeline_name) DO UPDATE
        SET
            source_signature = EXCLUDED.source_signature,
            updated_at = EXCLUDED.updated_at
        """,
        [pipeline_name, source_signature],
    )


def daily_short_volume_signature(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Signature source pour build_finra_daily_short_volume.

    On compare l'état de la raw table et de l'history table.
    Si elles sont déjà alignées sur les métriques simples, on peut skipper
    le rebuild canonique journalier.
    """
    payload: dict[str, object] = {
        "raw_row_count": 0,
        "raw_max_trade_date": None,
        "history_row_count": 0,
        "history_max_trade_date": None,
    }

    if table_exists(con, "finra_daily_short_volume_source_raw"):
        payload["raw_row_count"] = int(
            con.execute(
                "SELECT COUNT(*) FROM finra_daily_short_volume_source_raw"
            ).fetchone()[0]
        )
        payload["raw_max_trade_date"] = str(
            con.execute(
                "SELECT MAX(trade_date) FROM finra_daily_short_volume_source_raw"
            ).fetchone()[0]
        )

    if table_exists(con, "daily_short_volume_history"):
        payload["history_row_count"] = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history"
            ).fetchone()[0]
        )
        payload["history_max_trade_date"] = str(
            con.execute(
                "SELECT MAX(trade_date) FROM daily_short_volume_history"
            ).fetchone()[0]
        )

    return payload


def should_run_daily_short_volume(con: duckdb.DuckDBPyConnection) -> tuple[bool, dict]:
    """
    Décide si build_finra_daily_short_volume doit tourner.

    Règle simple et conservative :
    - si raw est vide => skip
    - si history est vide => run
    - si raw/history diffèrent => run
    - sinon skip
    """
    sig = daily_short_volume_signature(con)

    raw_row_count = int(sig["raw_row_count"] or 0)
    raw_max_trade_date = sig["raw_max_trade_date"]
    history_row_count = int(sig["history_row_count"] or 0)
    history_max_trade_date = sig["history_max_trade_date"]

    if raw_row_count == 0:
        return False, sig

    if history_row_count == 0:
        return True, sig

    if raw_row_count != history_row_count:
        return True, sig

    if str(raw_max_trade_date) != str(history_max_trade_date):
        return True, sig

    return False, sig


def short_features_signature(con: duckdb.DuckDBPyConnection) -> dict:
    """
    Signature source pour build_short_features.

    Sources principales :
    - daily_short_volume_history
    - finra_short_interest_history
    - symbol_normalization active rules (compteur simple)

    Note :
    le compteur de règles de normalisation est volontairement simple.
    Il attrape les ajouts/suppressions, qui sont les cas les plus fréquents.
    """
    payload: dict[str, object] = {
        "daily_short_volume_history_count": 0,
        "daily_short_volume_history_max_trade_date": None,
        "finra_short_interest_history_count": 0,
        "finra_short_interest_history_max_settlement_date": None,
        "symbol_normalization_active_count": 0,
    }

    if table_exists(con, "daily_short_volume_history"):
        payload["daily_short_volume_history_count"] = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history"
            ).fetchone()[0]
        )
        payload["daily_short_volume_history_max_trade_date"] = str(
            con.execute(
                "SELECT MAX(trade_date) FROM daily_short_volume_history"
            ).fetchone()[0]
        )

    if table_exists(con, "finra_short_interest_history"):
        payload["finra_short_interest_history_count"] = int(
            con.execute(
                "SELECT COUNT(*) FROM finra_short_interest_history"
            ).fetchone()[0]
        )
        payload["finra_short_interest_history_max_settlement_date"] = str(
            con.execute(
                "SELECT MAX(settlement_date) FROM finra_short_interest_history"
            ).fetchone()[0]
        )

    if table_exists(con, "symbol_normalization"):
        payload["symbol_normalization_active_count"] = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM symbol_normalization
                WHERE is_active = TRUE
                """
            ).fetchone()[0]
        )

    return payload


def canonical_signature(payload: dict) -> str:
    """
    Sérialise une signature de manière stable.

    Le tri des clés évite des diffs parasites entre runs.
    """
    return json.dumps(payload, sort_keys=True, default=str)


def should_run_short_features(con: duckdb.DuckDBPyConnection) -> tuple[bool, dict, str | None]:
    """
    Décide si build_short_features doit tourner.

    Règle :
    - si une source canonique de base est vide => skip
    - sinon on compare la signature courante à la dernière signature réussie
    """
    payload = short_features_signature(con)

    daily_count = int(payload["daily_short_volume_history_count"] or 0)
    short_interest_count = int(payload["finra_short_interest_history_count"] or 0)

    if daily_count == 0 or short_interest_count == 0:
        return False, payload, None

    current_signature = canonical_signature(payload)
    saved_signature = read_saved_signature(con, "build_short_features")

    return current_signature != saved_signature, payload, current_signature


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    print("===== DAILY PIPELINE START =====", flush=True)
    print("db_path =", str(DB_PATH), flush=True)

    con = connect_db()
    try:
        ensure_pipeline_watermark_table(con)

        # 1) PRICES
        # Déjà auto-incrémental côté CLI.
        run_step(
            "build_prices",
            [
                "python3",
                "cli/core/build_prices.py",
                "--db-path",
                str(DB_PATH),
            ],
        )

        # 2) FINRA DAILY SHORT VOLUME
        run_dsv, dsv_sig = should_run_daily_short_volume(con)
        print("\n===== INCREMENTAL PROBE: build_finra_daily_short_volume =====", flush=True)
        print(json.dumps(dsv_sig, default=str, indent=2), flush=True)

        if run_dsv:
            run_step(
                "build_finra_daily_short_volume",
                [
                    "python3",
                    "cli/core/build_finra_daily_short_volume.py",
                    "--db-path",
                    str(DB_PATH),
                ],
            )
        else:
            print("⏭️  SKIP build_finra_daily_short_volume (already aligned)", flush=True)

        # 3) FINRA SHORT INTEREST
        # Ce step est déjà très rapide en noop; on le garde toujours.
        run_step(
            "build_finra_short_interest",
            [
                "python3",
                "cli/core/build_finra_short_interest.py",
                "--db-path",
                str(DB_PATH),
            ],
        )

        # 4) SHORT FEATURES
        # Grosse étape : on la gate via signature source persistée en SQL.
        run_sf, sf_sig, sf_signature_text = should_run_short_features(con)
        print("\n===== INCREMENTAL PROBE: build_short_features =====", flush=True)
        print(json.dumps(sf_sig, default=str, indent=2), flush=True)

        if run_sf:
            run_step(
                "build_short_features",
                [
                    "python3",
                    "cli/core/build_short_features.py",
                    "--db-path",
                    str(DB_PATH),
                    "--duckdb-threads",
                    "2",
                    "--duckdb-memory-limit",
                    "36GB",
                ],
            )

            # On persiste la signature seulement après succès.
            assert sf_signature_text is not None
            write_saved_signature(con, "build_short_features", sf_signature_text)
        else:
            print("⏭️  SKIP build_short_features (source signature unchanged)", flush=True)

        # 5) SEC FILINGS
        # On le laisse tourner pour l'instant; optimisation secondaire.
        run_step(
            "build_sec_filings",
            [
                "python3",
                "cli/core/build_sec_filings.py",
                "--db-path",
                str(DB_PATH),
            ],
        )

    finally:
        con.close()

    print("\n===== DAILY PIPELINE SUCCESS =====", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
