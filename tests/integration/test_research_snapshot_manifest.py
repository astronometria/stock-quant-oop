from __future__ import annotations

"""
Integration test for the research-grade snapshot manifest CLI.

But
---
Valider qu'un snapshot research-grade:
- crée bien une ligne dans research_dataset_manifest
- crée bien une ligne par source dans research_dataset_input_signature
- reste exécutable sur une petite base DuckDB synthétique

Pourquoi ce test est important
------------------------------
Le chantier research-grade ne vaut scientifiquement que s'il est:
- traçable
- reproductible
- testé automatiquement

Ce test est volontairement auto-suffisant:
- il construit une petite base locale temporaire
- il y crée les tables minimales
- il exécute le CLI réel
- il relit ensuite les métadonnées écrites
"""

import json
import subprocess
import sys
from pathlib import Path

import duckdb


def test_build_research_snapshot_writes_manifest_and_input_signatures(
    tmp_path: Path,
) -> None:
    """
    Cas nominal minimal.

    On crée une base locale avec les tables attendues par:
    cli/core/build_research_snapshot.py

    Puis on vérifie:
    - 1 manifest écrit
    - 6 signatures d'entrée écrites
    - le dataset_name demandé est bien persisté
    """
    repo_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "research_snapshot_test.duckdb"

    con = duckdb.connect(str(db_path))
    try:
        # ------------------------------------------------------------------
        # Tables minimales pour le snapshot research-grade.
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE TABLE price_history (
                symbol VARCHAR,
                date DATE,
                close DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO price_history VALUES
                ('AAPL', DATE '2026-03-17', 215.0),
                ('MSFT', DATE '2026-03-17', 412.0)
            """
        )

        con.execute(
            """
            CREATE TABLE sec_filing (
                company_id VARCHAR,
                filing_date DATE,
                form_type VARCHAR
            )
            """
        )
        con.execute(
            """
            INSERT INTO sec_filing VALUES
                ('0000320193', DATE '2026-03-13', '10-K')
            """
        )

        con.execute(
            """
            CREATE TABLE finra_short_interest_history (
                symbol VARCHAR,
                settlement_date DATE,
                short_interest BIGINT
            )
            """
        )
        con.execute(
            """
            INSERT INTO finra_short_interest_history VALUES
                ('AAPL', DATE '2026-02-13', 1000000)
            """
        )

        con.execute(
            """
            CREATE TABLE daily_short_volume_history (
                symbol VARCHAR,
                trade_date DATE,
                short_volume BIGINT
            )
            """
        )
        con.execute(
            """
            INSERT INTO daily_short_volume_history VALUES
                ('AAPL', DATE '2026-03-13', 500000)
            """
        )

        con.execute(
            """
            CREATE TABLE short_features_daily (
                symbol VARCHAR,
                as_of_date DATE,
                short_volume_ratio DOUBLE
            )
            """
        )
        con.execute(
            """
            INSERT INTO short_features_daily VALUES
                ('AAPL', DATE '2026-03-13', 0.42)
            """
        )

        con.execute(
            """
            CREATE TABLE symbol_normalization (
                raw_symbol VARCHAR,
                normalized_symbol VARCHAR,
                normalization_type VARCHAR,
                is_active BOOLEAN
            )
            """
        )
        con.execute(
            """
            INSERT INTO symbol_normalization VALUES
                ('BRK/B', 'BRK.B', 'slash', TRUE)
            """
        )
    finally:
        con.close()

    cmd = [
        sys.executable,
        str(repo_root / "cli/core/build_research_snapshot.py"),
        "--db-path",
        str(db_path),
        "--dataset-name",
        "test_snapshot_dataset",
        "--start-date",
        "2018-01-31",
        "--end-date",
        "2026-03-18",
        "--notes",
        "pytest integration snapshot",
        "--parameters-json",
        json.dumps(
            {
                "universe": "pytest_universe",
                "mode": "integration_test",
            }
        ),
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
        manifest_rows = con.execute(
            """
            SELECT
                snapshot_id,
                dataset_name,
                start_date,
                end_date,
                source_count,
                total_row_count,
                status
            FROM research_dataset_manifest
            """
        ).fetchall()

        assert len(manifest_rows) == 1

        snapshot_id, dataset_name, start_date, end_date, source_count, total_row_count, status = manifest_rows[0]

        assert snapshot_id.startswith("test_snapshot_dataset_")
        assert dataset_name == "test_snapshot_dataset"
        assert str(start_date) == "2018-01-31"
        assert str(end_date) == "2026-03-18"
        assert source_count == 6
        assert total_row_count == 7
        assert status == "completed"

        input_signature_rows = con.execute(
            """
            SELECT
                source_name,
                row_count
            FROM research_dataset_input_signature
            ORDER BY source_name
            """
        ).fetchall()

        assert len(input_signature_rows) == 6
        assert input_signature_rows == [
            ("daily_short_volume_history", 1),
            ("finra_short_interest_history", 1),
            ("price_history", 2),
            ("sec_filing", 1),
            ("short_features_daily", 1),
            ("symbol_normalization", 1),
        ]
    finally:
        con.close()
