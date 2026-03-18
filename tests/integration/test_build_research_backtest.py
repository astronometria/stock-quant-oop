from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def test_backtest(tmp_path: Path) -> None:
    repo = Path(__file__).resolve().parents[2]
    db = tmp_path / "test.duckdb"

    con = duckdb.connect(str(db))
    try:
        # Features / signal inputs
        con.execute("""
            CREATE TABLE research_training_dataset (
                dataset_id VARCHAR,
                snapshot_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close DOUBLE,
                short_volume_ratio DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        con.execute("""
            INSERT INTO research_training_dataset (
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                close,
                short_volume_ratio
            )
            VALUES
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-10', 100.0, 0.6),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-11', 110.0, 0.4),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-12', 121.0, 0.7)
        """)

        # Labels
        con.execute("""
            CREATE TABLE research_labels (
                dataset_id VARCHAR,
                snapshot_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        con.execute("""
            INSERT INTO research_labels (
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d
            )
            VALUES
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-10', 0.01, NULL, NULL),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-11', -0.02, NULL, NULL),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-12', 0.03, NULL, NULL)
        """)
    finally:
        con.close()

    result = subprocess.run([
        sys.executable,
        str(repo / "cli/core/build_research_backtest.py"),
        "--db-path", str(db),
        "--dataset-id", "ds1",
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db))
    try:
        row = con.execute("""
            SELECT
                dataset_id,
                signal_name,
                avg_return,
                total_return,
                n_obs
            FROM research_backtest
        """).fetchone()

        assert row is not None
        assert row[0] == "ds1"
        assert row[1] == "short_volume_ratio_long"

        # signaux:
        # 2026-03-10 => 0.6 => long => +0.01
        # 2026-03-11 => 0.4 => flat => 0.00
        # 2026-03-12 => 0.7 => long => +0.03
        # moyenne = (0.01 + 0.00 + 0.03)/3 = 0.013333...
        assert round(float(row[2]), 6) == round((0.01 + 0.00 + 0.03) / 3.0, 6)
        assert round(float(row[3]), 6) == round(0.01 + 0.00 + 0.03, 6)
        assert int(row[4]) == 3
    finally:
        con.close()
