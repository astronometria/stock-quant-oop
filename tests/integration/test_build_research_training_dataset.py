from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def _create_minimal_db(db_path: Path) -> None:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE research_dataset_manifest (
                snapshot_id VARCHAR,
                status VARCHAR
            )
        """)
        con.execute("""
            INSERT INTO research_dataset_manifest VALUES ('snap1', 'completed')
        """)

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
            CREATE TABLE price_history (
                symbol VARCHAR,
                date DATE,
                close DOUBLE
            )
        """)
        con.execute("""
            INSERT INTO price_history VALUES
                ('AAPL', DATE '2026-03-10', 200)
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
                ('AAPL', DATE '2026-03-10', 0.5)
        """)
    finally:
        con.close()


def test_build_dataset(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "test.duckdb"

    _create_minimal_db(db_path)

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/build_research_training_dataset.py"),
        "--db-path",
        str(db_path),
        "--snapshot-id",
        "snap1",
        "--split-id",
        "split1",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("""
            SELECT symbol, close, short_volume_ratio
            FROM research_training_dataset
        """).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "AAPL"
        assert rows[0][1] == 200
        assert rows[0][2] == 0.5
    finally:
        con.close()
