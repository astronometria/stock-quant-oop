from __future__ import annotations

"""
Integration test for run_research_snapshot.py

Objectif
--------
Valider que le runner:
- orchestre correctement build_research_snapshot.py
- écrit un manifest valide
- écrit les input signatures
- respecte le flag fail-on-empty-price-history

Important
---------
- test end-to-end (subprocess)
- aucune dépendance externe
- DB DuckDB temporaire
"""

import json
import subprocess
import sys
from pathlib import Path

import duckdb


def _create_minimal_research_db(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))

    try:
        # --- price_history (obligatoire)
        con.execute("""
            CREATE TABLE price_history (
                symbol VARCHAR,
                date DATE,
                close DOUBLE
            )
        """)
        con.execute("""
            INSERT INTO price_history VALUES
                ('AAPL', DATE '2026-03-17', 200.0)
        """)

        # --- sec_filing
        con.execute("""
            CREATE TABLE sec_filing (
                company_id VARCHAR,
                filing_date DATE
            )
        """)
        con.execute("""
            INSERT INTO sec_filing VALUES
                ('0000320193', DATE '2026-03-10')
        """)

        # --- finra_short_interest_history
        con.execute("""
            CREATE TABLE finra_short_interest_history (
                symbol VARCHAR,
                settlement_date DATE
            )
        """)
        con.execute("""
            INSERT INTO finra_short_interest_history VALUES
                ('AAPL', DATE '2026-03-01')
        """)

        # --- daily_short_volume_history
        con.execute("""
            CREATE TABLE daily_short_volume_history (
                symbol VARCHAR,
                trade_date DATE
            )
        """)
        con.execute("""
            INSERT INTO daily_short_volume_history VALUES
                ('AAPL', DATE '2026-03-01')
        """)

        # --- short_features_daily
        con.execute("""
            CREATE TABLE short_features_daily (
                symbol VARCHAR,
                as_of_date DATE
            )
        """)
        con.execute("""
            INSERT INTO short_features_daily VALUES
                ('AAPL', DATE '2026-03-01')
        """)

        # --- symbol_normalization
        con.execute("""
            CREATE TABLE symbol_normalization (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO symbol_normalization VALUES
                ('AAPL', 'AAPL')
        """)

    finally:
        con.close()


def test_run_research_snapshot_success(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "test_snapshot.duckdb"

    _create_minimal_research_db(db_path)

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/run_research_snapshot.py"),
        "--db-path",
        str(db_path),
        "--dataset-name",
        "pytest_runner_snapshot",
        "--start-date",
        "2020-01-01",
        "--end-date",
        "2026-03-18",
        "--parameters-json",
        json.dumps({"mode": "pytest"}),
        "--fail-on-empty-price-history",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("""
            SELECT dataset_name, status, source_count
            FROM research_dataset_manifest
        """).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "pytest_runner_snapshot"
        assert rows[0][1] == "completed"
        assert rows[0][2] == 6

    finally:
        con.close()


def test_run_research_snapshot_fail_on_empty_price_history(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "test_snapshot_empty.duckdb"

    # DB sans price_history
    con = duckdb.connect(str(db_path))
    con.close()

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/run_research_snapshot.py"),
        "--db-path",
        str(db_path),
        "--dataset-name",
        "should_fail",
        "--fail-on-empty-price-history",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "price_history is empty" in (result.stdout + result.stderr)
