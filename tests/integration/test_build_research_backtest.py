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
        con.execute("""
            CREATE TABLE research_split_manifest (
                split_id VARCHAR,
                train_start DATE,
                train_end DATE,
                valid_start DATE,
                valid_end DATE,
                test_start DATE,
                test_end DATE,
                embargo_days INTEGER,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO research_split_manifest (
                split_id, train_start, train_end, valid_start, valid_end,
                test_start, test_end, embargo_days, notes
            )
            VALUES (
                'split1',
                DATE '2026-03-01', DATE '2026-03-10',
                DATE '2026-03-11', DATE '2026-03-11',
                DATE '2026-03-12', DATE '2026-03-12',
                0,
                'pytest split'
            )
        """)

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
                dataset_id, snapshot_id, symbol, as_of_date, close, short_volume_ratio
            )
            VALUES
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-10', 100.0, 0.6),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-11', 110.0, 0.4),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-12', 121.0, 0.7)
        """)

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
                dataset_id, snapshot_id, symbol, as_of_date,
                fwd_return_1d, fwd_return_5d, fwd_return_20d
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
        "--split-id", "split1",
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db))
    try:
        rows = con.execute("SELECT COUNT(*) FROM research_backtest").fetchone()
        assert rows[0] >= 1
    finally:
        con.close()
