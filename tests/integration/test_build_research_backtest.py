from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def test_backtest(tmp_path: Path):
    repo = Path(__file__).resolve().parents[2]
    db = tmp_path / "test.duckdb"

    con = duckdb.connect(str(db))
    try:
        con.execute("""
            CREATE TABLE research_labels (
                dataset_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                fwd_return_1d DOUBLE,
                short_volume_ratio DOUBLE
            )
        """)

        con.execute("""
            INSERT INTO research_labels VALUES
            ('ds1', 'AAPL', DATE '2026-03-10', 0.01, 0.6),
            ('ds1', 'AAPL', DATE '2026-03-11', -0.02, 0.4),
            ('ds1', 'AAPL', DATE '2026-03-12', 0.03, 0.7)
        """)
    finally:
        con.close()

    result = subprocess.run([
        sys.executable,
        str(repo / "cli/core/build_research_backtest.py"),
        "--db-path", str(db),
        "--dataset-id", "ds1"
    ], capture_output=True, text=True)

    assert result.returncode == 0

    con = duckdb.connect(str(db))
    try:
        rows = con.execute("SELECT COUNT(*) FROM research_backtest").fetchone()
        assert rows[0] == 1
    finally:
        con.close()
