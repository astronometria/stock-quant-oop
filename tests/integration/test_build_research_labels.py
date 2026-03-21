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

        # Nouveau flux: labels builder consomme research_training_dataset
        con.execute("""
            CREATE TABLE research_training_dataset (
                dataset_id VARCHAR,
                snapshot_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close DOUBLE,
                short_volume_ratio DOUBLE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rsi_14 DOUBLE,
                returns_1d DOUBLE,
                returns_5d DOUBLE,
                returns_20d DOUBLE,
                sma_20 DOUBLE,
                sma_50 DOUBLE,
                sma_200 DOUBLE,
                close_to_sma_20 DOUBLE,
                atr_14 DOUBLE,
                volatility_20 DOUBLE
            )
        """)
        con.execute("""
            INSERT INTO research_training_dataset (
                dataset_id, snapshot_id, symbol, as_of_date, close, short_volume_ratio,
                rsi_14, returns_1d, returns_5d, returns_20d,
                sma_20, sma_50, sma_200, close_to_sma_20, atr_14, volatility_20
            )
            VALUES
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-10', 100, 0.60, 25.0, 0.01, 0.02, 0.03, 98.0, 95.0, 90.0, 0.02, 2.0, 0.10),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-11', 110, 0.40, 35.0, 0.10, 0.11, 0.12, 100.0, 96.0, 91.0, 0.10, 2.5, 0.12),
                ('ds1', 'snap1', 'AAPL', DATE '2026-03-12', 121, 0.70, 45.0, 0.10, 0.12, 0.13, 105.0, 97.0, 92.0, 0.15, 3.0, 0.13)
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

    con = duckdb.connect(str(db), read_only=True)
    try:
        row = con.execute("""
            SELECT COUNT(*), COUNT(fwd_return_1d)
            FROM research_labels
            WHERE dataset_id = 'ds1'
        """).fetchone()
        assert row[0] == 3
        assert row[1] >= 2
    finally:
        con.close()
