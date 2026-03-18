from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def test_experiment_v2(tmp_path: Path):
    repo = Path(__file__).resolve().parents[2]
    db = tmp_path / "test.duckdb"

    con = duckdb.connect(str(db))
    try:
        con.execute("""
            CREATE TABLE research_dataset_manifest (
                snapshot_id VARCHAR,
                status VARCHAR
            )
        """)
        con.execute("INSERT INTO research_dataset_manifest VALUES ('snap1','completed')")

        con.execute("""
            CREATE TABLE price_history (
                symbol VARCHAR,
                date DATE,
                close DOUBLE
            )
        """)

        con.execute("""
            INSERT INTO price_history VALUES
            ('AAPL', DATE '2026-03-10', 100),
            ('AAPL', DATE '2026-03-11', 110)
        """)
    finally:
        con.close()

    result = subprocess.run([
        sys.executable,
        str(repo / "cli/core/run_research_experiment.py"),
        "--db-path", str(db),
        "--snapshot-id", "snap1"
    ], capture_output=True, text=True)

    assert result.returncode == 0

    con = duckdb.connect(str(db))
    try:
        rows = con.execute("SELECT COUNT(*) FROM research_experiment_manifest").fetchone()
        assert rows[0] == 1
    finally:
        con.close()
