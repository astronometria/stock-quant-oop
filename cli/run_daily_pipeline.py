#!/usr/bin/env python3
from __future__ import annotations

"""
Daily pipeline with research-grade signature guards.

Important
---------
Cette version évite explicitement de garder une connexion DuckDB ouverte
pendant l'exécution d'un sous-processus pipeline.

Pourquoi
--------
Le repo utilise plusieurs CLI qui ouvrent chacun leur propre connexion
DuckDB. Si l'orchestrateur garde déjà un handle ouvert, on risque un
conflit de lock au moment d'appeler un autre script.

Règle
-----
- ouvrir une connexion courte
- lire ou écrire les signatures
- fermer immédiatement
- seulement ensuite lancer les sous-processus
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_manifest_repository import (
    DuckDbResearchManifestRepository,
    ResearchBuildSignature,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "market.duckdb"
LOG_DIR = PROJECT_ROOT / "logs"


def _ensure_log_dir() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _run_step(name: str, cmd: list[str]) -> None:
    print(f"\n===== RUN STEP: {name} =====")
    print("cmd =", " ".join(cmd), flush=True)

    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ STEP FAILED: {name} (exit={result.returncode})", flush=True)
        raise SystemExit(result.returncode)

    print(f"✅ STEP OK: {name}", flush=True)


def _with_repo(fn):
    """
    Ouvre une connexion courte, exécute l'action, puis referme.

    Très important pour éviter les conflits de lock DuckDB avec les
    sous-processus lancés par l'orchestrateur.
    """
    con = duckdb.connect(str(DB_PATH))
    try:
        repo = DuckDbResearchManifestRepository(con)
        repo.ensure_tables()
        return fn(con, repo)
    finally:
        con.close()


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _choose_date_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> str | None:
    """
    Choisit la colonne date métier la plus pertinente parmi les conventions
    déjà présentes dans le repo.
    """
    rows = con.execute(
        f"PRAGMA table_info('{table_name}')"
    ).fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "trade_date",
        "settlement_date",
        "as_of_date",
        "date",
        "filing_date",
        "latest_price_date",
    ]
    for col in preferred_order:
        if col in cols:
            return col
    return None


def _choose_symbol_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> str | None:
    rows = con.execute(
        f"PRAGMA table_info('{table_name}')"
    ).fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "symbol",
        "raw_symbol",
        "normalized_symbol",
    ]
    for col in preferred_order:
        if col in cols:
            return col
    return None


def _compute_table_signature(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, Any]:
    """
    Calcule une signature SQL-first d'une table source.

    Important
    ---------
    On ne cherche pas ici un hash crypto parfait.
    On cherche une signature:
    - stable
    - lisible
    - suffisamment sensible aux changements réels
    """
    if not _table_exists(con, table_name):
        return {
            "table_name": table_name,
            "row_count": 0,
            "min_business_date": None,
            "max_business_date": None,
            "checksum": None,
            "signature_hash": f"{table_name}|missing",
        }

    date_col = _choose_date_column(con, table_name)
    symbol_col = _choose_symbol_column(con, table_name)

    date_expr_min = f"MIN({date_col})" if date_col else "NULL"
    date_expr_max = f"MAX({date_col})" if date_col else "NULL"

    # Checksum conservative:
    # - si symbol + date existent, on les utilise
    # - sinon on retombe sur count + min/max
    if symbol_col and date_col:
        checksum_expr = (
            f"SUM(hash(COALESCE(CAST({symbol_col} AS VARCHAR), ''), "
            f"COALESCE(CAST({date_col} AS VARCHAR), '')))"
        )
    elif symbol_col:
        checksum_expr = (
            f"SUM(hash(COALESCE(CAST({symbol_col} AS VARCHAR), '')))"
        )
    elif date_col:
        checksum_expr = (
            f"SUM(hash(COALESCE(CAST({date_col} AS VARCHAR), '')))"
        )
    else:
        checksum_expr = "NULL"

    sql = f"""
        SELECT
            COUNT(*) AS row_count,
            {date_expr_min} AS min_business_date,
            {date_expr_max} AS max_business_date,
            {checksum_expr} AS checksum
        FROM {table_name}
    """

    row = con.execute(sql).fetchone()
    row_count = int(row[0]) if row and row[0] is not None else 0
    min_business_date = row[1] if row else None
    max_business_date = row[2] if row else None
    checksum = row[3] if row else None

    signature_hash = (
        f"{table_name}|"
        f"{row_count}|"
        f"{min_business_date}|"
        f"{max_business_date}|"
        f"{checksum}"
    )

    return {
        "table_name": table_name,
        "row_count": row_count,
        "min_business_date": min_business_date,
        "max_business_date": max_business_date,
        "checksum": checksum,
        "signature_hash": signature_hash,
    }


def _load_signature_map(
    pipeline_name: str,
    source_names: list[str],
) -> dict[str, dict[str, Any]]:
    def _impl(con: duckdb.DuckDBPyConnection, repo: DuckDbResearchManifestRepository):
        out: dict[str, dict[str, Any]] = {}
        for source_name in source_names:
            current = _compute_table_signature(con, source_name)
            previous = repo.get_latest_build_signature(
                pipeline_name=pipeline_name,
                signature_scope="default",
                source_name=source_name,
            )
            out[source_name] = {
                "current": current,
                "previous": previous,
            }
        return out

    return _with_repo(_impl)


def _persist_signatures(
    pipeline_name: str,
    source_names: list[str],
) -> None:
    def _impl(con: duckdb.DuckDBPyConnection, repo: DuckDbResearchManifestRepository):
        for source_name in source_names:
            current = _compute_table_signature(con, source_name)
            repo.insert_build_signature(
                ResearchBuildSignature(
                    pipeline_name=pipeline_name,
                    signature_scope="default",
                    source_name=source_name,
                    signature_hash=current["signature_hash"],
                    row_count=current["row_count"],
                    min_business_date=current["min_business_date"],
                    max_business_date=current["max_business_date"],
                    notes="persisted by daily pipeline signature guard",
                )
            )

    _with_repo(_impl)


def _should_skip_from_signature_map(
    signature_map: dict[str, dict[str, Any]],
) -> bool:
    """
    Skip seulement si:
    - toutes les sources ont déjà une signature précédente
    - et toutes les signatures sont identiques
    """
    if not signature_map:
        return False

    for _, item in signature_map.items():
        previous = item["previous"]
        current = item["current"]

        if previous is None:
            return False

        if previous["signature_hash"] != current["signature_hash"]:
            return False

    return True


def _print_signature_probe(
    pipeline_name: str,
    signature_map: dict[str, dict[str, Any]],
) -> None:
    printable: dict[str, Any] = {}
    for source_name, item in signature_map.items():
        printable[source_name] = {
            "current": item["current"],
            "previous_signature_hash": (
                item["previous"]["signature_hash"]
                if item["previous"] is not None
                else None
            ),
        }

    print(f"\n===== SIGNATURE PROBE: {pipeline_name} =====", flush=True)
    print(json.dumps(printable, default=str, indent=2), flush=True)


def main() -> None:
    _ensure_log_dir()

    print("===== DAILY PIPELINE START =====", flush=True)
    print("db_path =", DB_PATH, flush=True)

    # ------------------------------------------------------------------
    # 1) Prices
    # ------------------------------------------------------------------
    _run_step(
        "build_prices",
        [
            "python3",
            "cli/core/build_prices.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    # ------------------------------------------------------------------
    # 2) FINRA daily short volume canonical build
    # ------------------------------------------------------------------
    finra_daily_sources = [
        "finra_daily_short_volume_source_raw",
        "daily_short_volume_history",
    ]
    finra_daily_signature_map = _load_signature_map(
        pipeline_name="build_finra_daily_short_volume",
        source_names=finra_daily_sources,
    )
    _print_signature_probe(
        "build_finra_daily_short_volume",
        finra_daily_signature_map,
    )

    if _should_skip_from_signature_map(finra_daily_signature_map):
        print("⏭️  SKIP build_finra_daily_short_volume (signature unchanged)", flush=True)
    else:
        _run_step(
            "build_finra_daily_short_volume",
            [
                "python3",
                "cli/core/build_finra_daily_short_volume.py",
                "--db-path",
                str(DB_PATH),
            ],
        )
        _persist_signatures(
            pipeline_name="build_finra_daily_short_volume",
            source_names=finra_daily_sources,
        )

    # ------------------------------------------------------------------
    # 3) FINRA short interest
    # ------------------------------------------------------------------
    _run_step(
        "build_finra_short_interest",
        [
            "python3",
            "cli/core/build_finra_short_interest.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    # ------------------------------------------------------------------
    # 4) Short features
    # ------------------------------------------------------------------
    short_feature_sources = [
        "daily_short_volume_history",
        "finra_short_interest_history",
        "symbol_normalization",
    ]
    short_feature_signature_map = _load_signature_map(
        pipeline_name="build_short_features",
        source_names=short_feature_sources,
    )
    _print_signature_probe(
        "build_short_features",
        short_feature_signature_map,
    )

    if _should_skip_from_signature_map(short_feature_signature_map):
        print("⏭️  SKIP build_short_features (signature unchanged)", flush=True)
    else:
        _run_step(
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
        _persist_signatures(
            pipeline_name="build_short_features",
            source_names=short_feature_sources,
        )

    # ------------------------------------------------------------------
    # 5) SEC
    # ------------------------------------------------------------------
    _run_step(
        "build_sec_filings",
        [
            "python3",
            "cli/core/build_sec_filings.py",
            "--db-path",
            str(DB_PATH),
        ],
    )

    print("\n===== DAILY PIPELINE SUCCESS =====", flush=True)


if __name__ == "__main__":
    main()
