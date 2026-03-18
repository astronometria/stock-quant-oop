from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def test_experiment_v2(tmp_path: Path) -> None:
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
                ('AAPL', DATE '2026-03-10', 100.0),
                ('AAPL', DATE '2026-03-11', 110.0),
                ('AAPL', DATE '2026-03-12', 121.0)
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
                ('AAPL', DATE '2026-03-10', 0.60),
                ('AAPL', DATE '2026-03-11', 0.40),
                ('AAPL', DATE '2026-03-12', 0.70)
        """)
    finally:
        con.close()

    result = subprocess.run([
        sys.executable,
        str(repo / "cli/core/run_research_experiment.py"),
        "--db-path", str(db),
        "--snapshot-id", "snap1"
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db))
    try:
        experiment_rows = con.execute("""
            SELECT COUNT(*) FROM research_experiment_manifest
        """).fetchone()
        assert experiment_rows[0] == 1

        backtest_rows = con.execute("""
            SELECT COUNT(*) FROM research_backtest
        """).fetchone()
        assert backtest_rows[0] == 1

        dataset_rows = con.execute("""
            SELECT COUNT(*) FROM research_training_dataset
        """).fetchone()
        assert dataset_rows[0] > 0

        label_rows = con.execute("""
            SELECT COUNT(*) FROM research_labels
        """).fetchone()
        assert label_rows[0] > 0

    finally:
        con.close()
