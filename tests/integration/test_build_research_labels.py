from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import duckdb


def test_labels(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
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
                ('AAPL', DATE '2026-03-10', 100),
                ('AAPL', DATE '2026-03-11', 110),
                ('AAPL', DATE '2026-03-12', 121)
        """)
    finally:
        con.close()

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/build_research_labels.py"),
        "--db-path", str(db),
        "--snapshot-id", "snap1",
        "--dataset-id", "ds1",
        "--split-id", "split1",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db))
    try:
        rows = con.execute("""
            SELECT fwd_return_1d
            FROM research_labels
            WHERE symbol = 'AAPL'
            ORDER BY as_of_date
        """).fetchall()

        assert len(rows) >= 1
        assert round(rows[0][0], 2) == 0.10
    finally:
        con.close()
