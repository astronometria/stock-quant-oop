from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb


def _create_snapshot_ready_db(db_path: Path) -> str:
    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE research_dataset_manifest (
                snapshot_id VARCHAR,
                dataset_name VARCHAR,
                git_commit VARCHAR,
                parameters_json JSON,
                start_date DATE,
                end_date DATE,
                created_by_pipeline VARCHAR,
                source_count BIGINT,
                total_row_count BIGINT,
                status VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE research_dataset_input_signature (
                snapshot_id VARCHAR,
                dataset_name VARCHAR,
                source_name VARCHAR,
                signature_hash VARCHAR,
                row_count BIGINT,
                min_business_date DATE,
                max_business_date DATE,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
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

        snapshot_id = "pytest_snapshot_20260318T000000Z"

        con.execute("""
            INSERT INTO research_dataset_manifest (
                snapshot_id, dataset_name, git_commit, parameters_json,
                start_date, end_date, created_by_pipeline, source_count,
                total_row_count, status, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            snapshot_id,
            "pytest_dataset",
            "abc123",
            json.dumps({"mode": "pytest"}),
            "2020-01-01",
            "2026-03-18",
            "build_research_snapshot",
            6,
            100,
            "completed",
            "pytest snapshot",
        ])

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
                ('AAPL', DATE '2026-03-10', 0.40),
                ('AAPL', DATE '2026-03-11', 0.50),
                ('AAPL', DATE '2026-03-12', 0.60)
        """)

        return snapshot_id
    finally:
        con.close()


def test_run_research_experiment_success(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "experiment_test.duckdb"
    snapshot_id = _create_snapshot_ready_db(db_path)

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/run_research_experiment.py"),
        "--db-path", str(db_path),
        "--snapshot-id", snapshot_id,
        "--split-id", "split1",
        "--experiment-name", "pytest_experiment",
        "--parameters-json", json.dumps({"alpha": 1, "mode": "pytest"}),
        "--notes", "integration test experiment",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    con = duckdb.connect(str(db_path))
    try:
        rows = con.execute("""
            SELECT snapshot_id, dataset_id, experiment_name, status, parameters_json, notes
            FROM research_experiment_manifest
        """).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == snapshot_id
        assert rows[0][1] is not None
        assert rows[0][2] == "pytest_experiment"
        assert rows[0][3] == "completed"
        assert rows[0][4] is not None
        assert rows[0][5] == "integration test experiment"

        dataset_rows = con.execute("""
            SELECT COUNT(*) FROM research_training_dataset
        """).fetchone()[0]
        assert dataset_rows > 0

        label_rows = con.execute("""
            SELECT COUNT(*) FROM research_labels
        """).fetchone()[0]
        assert label_rows > 0
    finally:
        con.close()


def test_run_research_experiment_fails_when_snapshot_missing(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "experiment_missing_snapshot.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        con.execute("""
            CREATE TABLE research_dataset_manifest (
                snapshot_id VARCHAR,
                dataset_name VARCHAR,
                git_commit VARCHAR,
                parameters_json JSON,
                start_date DATE,
                end_date DATE,
                created_by_pipeline VARCHAR,
                source_count BIGINT,
                total_row_count BIGINT,
                status VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE research_dataset_input_signature (
                snapshot_id VARCHAR,
                dataset_name VARCHAR,
                source_name VARCHAR,
                signature_hash VARCHAR,
                row_count BIGINT,
                min_business_date DATE,
                max_business_date DATE,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
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
    finally:
        con.close()

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/run_research_experiment.py"),
        "--db-path", str(db_path),
        "--snapshot-id", "missing_snapshot",
        "--split-id", "split1",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "snapshot_id not found" in (result.stdout + result.stderr)
