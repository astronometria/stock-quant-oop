#!/usr/bin/env python3
from __future__ import annotations

"""
Daily pipeline with research-grade signature guard.

Amélioration clé:
----------------
On remplace les heuristiques faibles (max_date)
par des signatures de contenu persistées.

Résultat:
---------
- skip robuste
- détecte corrections silencieuses
- compatible recherche scientifique
"""

import subprocess
import sys
from pathlib import Path

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_manifest_repository import (
    DuckDbResearchManifestRepository,
    ResearchBuildSignature,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "market.duckdb"


def run_step(name: str, cmd: list[str]) -> None:
    print(f"\n===== RUN STEP: {name} =====")
    print("cmd =", " ".join(cmd))

    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"❌ STEP FAILED: {name}")
        sys.exit(result.returncode)

    print(f"✅ STEP OK: {name}")


def compute_table_signature(con: duckdb.DuckDBPyConnection, table: str) -> dict:
    """
    Signature robuste SQL-first.

    Important:
    - pas parfait cryptographiquement
    - mais suffisant pour détecter changements réels
    """
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            MIN(trade_date) AS min_date,
            MAX(trade_date) AS max_date,
            SUM(hash(symbol, trade_date)) AS checksum
        FROM {table}
        """
    ).fetchone()

    return {
        "row_count": row[0],
        "min_date": row[1],
        "max_date": row[2],
        "checksum": row[3],
        "signature_hash": f"{row[0]}|{row[1]}|{row[2]}|{row[3]}",
    }


def should_skip(
    repo: DuckDbResearchManifestRepository,
    pipeline_name: str,
    source_name: str,
    new_sig: dict,
) -> bool:
    """
    Compare avec dernière signature persistée.
    """
    last = repo.get_latest_build_signature(
        pipeline_name=pipeline_name,
        signature_scope="default",
        source_name=source_name,
    )

    if last is None:
        return False

    return last["signature_hash"] == new_sig["signature_hash"]


def persist_signature(
    repo: DuckDbResearchManifestRepository,
    pipeline_name: str,
    source_name: str,
    sig: dict,
) -> None:
    repo.insert_build_signature(
        ResearchBuildSignature(
            pipeline_name=pipeline_name,
            signature_scope="default",
            source_name=source_name,
            signature_hash=sig["signature_hash"],
            row_count=sig["row_count"],
            min_business_date=sig["min_date"],
            max_business_date=sig["max_date"],
        )
    )


def main() -> None:
    print("===== DAILY PIPELINE START =====")
    print("db_path =", DB_PATH)

    con = duckdb.connect(str(DB_PATH))
    repo = DuckDbResearchManifestRepository(con)
    repo.ensure_tables()

    # ------------------------------------------------------------------
    # PRICES (toujours exécuté)
    # ------------------------------------------------------------------
    run_step(
        "build_prices",
        ["python3", "cli/core/build_prices.py", "--db-path", str(DB_PATH)],
    )

    # ------------------------------------------------------------------
    # SHORT FEATURES (avec signature)
    # ------------------------------------------------------------------
    print("\n===== SIGNATURE PROBE: short_features =====")

    sig = compute_table_signature(con, "daily_short_volume_history")

    if should_skip(repo, "build_short_features", "daily_short_volume_history", sig):
        print("⏭️  SKIP build_short_features (signature unchanged)")
    else:
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
        persist_signature(repo, "build_short_features", "daily_short_volume_history", sig)

    # ------------------------------------------------------------------
    # SEC (inchangé)
    # ------------------------------------------------------------------
    run_step(
        "build_sec_filings",
        ["python3", "cli/core/build_sec_filings.py", "--db-path", str(DB_PATH)],
    )

    con.close()

    print("\n===== DAILY PIPELINE SUCCESS =====")


if __name__ == "__main__":
    main()
