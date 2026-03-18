from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb


def test_build_research_snapshot_fails_when_critical_temporal_bounds_are_missing(
    tmp_path: Path,
) -> None:
    """
    Le snapshot doit échouer si une source critique temporelle ne permet
    pas de calculer min/max business date.
    """
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "research_snapshot_invalid.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        # price_history critique mais sans colonne temporelle détectable
        con.execute("""
            CREATE TABLE price_history (
                symbol VARCHAR,
                close DOUBLE
            )
        """)
        con.execute("""
            INSERT INTO price_history VALUES
                ('AAPL', 215.0)
        """)

        con.execute("""
            CREATE TABLE sec_filing (
                company_id VARCHAR,
                filing_date DATE,
                form_type VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO sec_filing VALUES
                ('0000320193', DATE '2026-03-13', '10-K')
        """)

        con.execute("""
            CREATE TABLE finra_short_interest_history (
                symbol VARCHAR,
                settlement_date DATE,
                short_interest BIGINT
            )
        """)
        con.execute("""
            INSERT INTO finra_short_interest_history VALUES
                ('AAPL', DATE '2026-02-13', 1000000)
        """)

        con.execute("""
            CREATE TABLE daily_short_volume_history (
                symbol VARCHAR,
                trade_date DATE,
                short_volume BIGINT
            )
        """)
        con.execute("""
            INSERT INTO daily_short_volume_history VALUES
                ('AAPL', DATE '2026-03-13', 500000)
        """)

        con.execute("""
            CREATE TABLE short_features_daily (
                symbol VARCHAR,
                as_of_date DATE,
                short_volume_ratio DOUBLE
            )
        """)
        con.execute("""
            INSERT INTO short_features_daily VALUES
                ('AAPL', DATE '2026-03-13', 0.42)
        """)

        con.execute("""
            CREATE TABLE symbol_normalization (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                normalization_type VARCHAR,
                is_active BOOLEAN
            )
        """)
        con.execute("""
            INSERT INTO symbol_normalization VALUES
                ('BRK/B', 'BRK.B', 'slash', TRUE)
        """)
    finally:
        con.close()

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/build_research_snapshot.py"),
        "--db-path",
        str(db_path),
        "--dataset-name",
        "invalid_snapshot_dataset",
        "--start-date",
        "2018-01-31",
        "--end-date",
        "2026-03-18",
        "--notes",
        "pytest strict validation failure",
        "--parameters-json",
        json.dumps({"mode": "integration_test_strict_failure"}),
    ]

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    combined_output = (result.stdout or "") + "\n" + (result.stderr or "")
    assert "failed_validation" in combined_output
    assert "price_history: temporal bounds are missing" in combined_output

    con = duckdb.connect(str(db_path))
    try:
        manifest_count = con.execute(
            "SELECT COUNT(*) FROM research_dataset_manifest"
        ).fetchone()[0]
        input_signature_count = con.execute(
            "SELECT COUNT(*) FROM research_dataset_input_signature"
        ).fetchone()[0]

        assert manifest_count == 0
        assert input_signature_count == 0
    finally:
        con.close()
