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
            CREATE TABLE research_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close DOUBLE,
                returns_1d DOUBLE,
                returns_5d DOUBLE,
                returns_20d DOUBLE,
                sma_20 DOUBLE,
                sma_50 DOUBLE,
                sma_200 DOUBLE,
                close_to_sma_20 DOUBLE,
                rsi_14 DOUBLE,
                atr_14 DOUBLE,
                volatility_20 DOUBLE,
                short_interest DOUBLE,
                days_to_cover DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            INSERT INTO research_features_daily (
                instrument_id, company_id, symbol, as_of_date, close,
                returns_1d, returns_5d, returns_20d,
                sma_20, sma_50, sma_200, close_to_sma_20,
                rsi_14, atr_14, volatility_20, short_volume_ratio, source_name
            )
            VALUES
                ('inst1', 'co1', 'AAPL', DATE '2026-03-10', 100.0, 0.01, 0.02, 0.03, 98.0, 95.0, 90.0, 0.02, 25.0, 2.0, 0.10, 0.60, 'pytest'),
                ('inst1', 'co1', 'AAPL', DATE '2026-03-11', 110.0, 0.10, 0.11, 0.12, 100.0, 96.0, 91.0, 0.10, 35.0, 2.5, 0.12, 0.40, 'pytest'),
                ('inst1', 'co1', 'AAPL', DATE '2026-03-12', 121.0, 0.10, 0.12, 0.13, 105.0, 97.0, 92.0, 0.15, 45.0, 3.0, 0.13, 0.70, 'pytest')
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
                ('AAPL', DATE '2026-03-10', 100.0),
                ('AAPL', DATE '2026-03-11', 110.0),
                ('AAPL', DATE '2026-03-12', 121.0)
        """)
    finally:
        con.close()

    result = subprocess.run([
        sys.executable,
        str(repo / "cli/core/run_research_experiment.py"),
        "--db-path", str(db),
        "--snapshot-id", "snap1",
        "--split-id", "split1",
    ], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr or result.stdout
