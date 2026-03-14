from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb


def test_run_core_pipeline_end_to_end(tmp_path: Path) -> None:
    project_root = Path("/home/marty/stock-quant-oop").resolve()
    db_path = tmp_path / "e2e_core_pipeline.duckdb"

    env = dict(os.environ)
    env["PYTHONPATH"] = str(project_root)

    cmd = [
        sys.executable,
        str(project_root / "cli" / "run_core_pipeline.py"),
        "--db-path",
        str(db_path),
        "--drop-existing",
        "--source-market",
        "regular",
        "--verbose",
    ]

    completed = subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, (
        f"run_core_pipeline failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}"
    )

    con = duckdb.connect(str(db_path))

    counts = {
        "symbol_reference_source_raw": con.execute("SELECT COUNT(*) FROM symbol_reference_source_raw").fetchone()[0],
        "market_universe": con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0],
        "market_universe_included": con.execute("SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE").fetchone()[0],
        "market_universe_conflicts": con.execute("SELECT COUNT(*) FROM market_universe_conflicts").fetchone()[0],
        "symbol_reference": con.execute("SELECT COUNT(*) FROM symbol_reference").fetchone()[0],
        "price_source_daily_raw": con.execute("SELECT COUNT(*) FROM price_source_daily_raw").fetchone()[0],
        "price_history": con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0],
        "price_latest": con.execute("SELECT COUNT(*) FROM price_latest").fetchone()[0],
        "finra_short_interest_source_raw": con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0],
        "finra_short_interest_history": con.execute("SELECT COUNT(*) FROM finra_short_interest_history").fetchone()[0],
        "finra_short_interest_latest": con.execute("SELECT COUNT(*) FROM finra_short_interest_latest").fetchone()[0],
        "finra_short_interest_sources": con.execute("SELECT COUNT(*) FROM finra_short_interest_sources").fetchone()[0],
        "news_source_raw": con.execute("SELECT COUNT(*) FROM news_source_raw").fetchone()[0],
        "news_articles_raw": con.execute("SELECT COUNT(*) FROM news_articles_raw").fetchone()[0],
        "news_symbol_candidates": con.execute("SELECT COUNT(*) FROM news_symbol_candidates").fetchone()[0],
    }

    assert counts["symbol_reference_source_raw"] == 6
    assert counts["market_universe"] == 5
    assert counts["market_universe_included"] == 3
    assert counts["market_universe_conflicts"] == 1
    assert counts["symbol_reference"] == 3
    assert counts["price_source_daily_raw"] == 9
    assert counts["price_history"] == 6
    assert counts["price_latest"] == 3
    assert counts["finra_short_interest_source_raw"] == 8
    assert counts["finra_short_interest_history"] == 5
    assert counts["finra_short_interest_latest"] == 3
    assert counts["finra_short_interest_sources"] == 2
    assert counts["news_source_raw"] == 4
    assert counts["news_articles_raw"] == 4
    assert counts["news_symbol_candidates"] == 3

    included_symbols = con.execute(
        """
        SELECT symbol
        FROM market_universe
        WHERE include_in_universe = TRUE
        ORDER BY symbol
        """
    ).fetchall()
    assert included_symbols == [("AAPL",), ("BABA",), ("MSFT",)]

    price_latest_symbols = con.execute(
        """
        SELECT symbol
        FROM price_latest
        ORDER BY symbol
        """
    ).fetchall()
    assert price_latest_symbols == [("AAPL",), ("BABA",), ("MSFT",)]

    finra_latest_symbols = con.execute(
        """
        SELECT symbol
        FROM finra_short_interest_latest
        ORDER BY symbol
        """
    ).fetchall()
    assert finra_latest_symbols == [("AAPL",), ("BABA",), ("MSFT",)]

    news_pairs = con.execute(
        """
        SELECT raw_id, symbol
        FROM news_symbol_candidates
        ORDER BY raw_id, symbol
        """
    ).fetchall()
    assert news_pairs == [(1001, "AAPL"), (1002, "MSFT"), (1003, "BABA")]

    con.close()


def test_pipeline_status_runs_after_core_pipeline(tmp_path: Path) -> None:
    project_root = Path("/home/marty/stock-quant-oop").resolve()
    db_path = tmp_path / "e2e_pipeline_status.duckdb"

    env = dict(os.environ)
    env["PYTHONPATH"] = str(project_root)

    run_cmd = [
        sys.executable,
        str(project_root / "cli" / "run_core_pipeline.py"),
        "--db-path",
        str(db_path),
        "--drop-existing",
        "--source-market",
        "regular",
    ]
    run_completed = subprocess.run(
        run_cmd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert run_completed.returncode == 0, (
        f"run_core_pipeline failed\nSTDOUT:\n{run_completed.stdout}\nSTDERR:\n{run_completed.stderr}"
    )

    status_cmd = [
        sys.executable,
        str(project_root / "cli" / "pipeline_status.py"),
        "--db-path",
        str(db_path),
    ]
    status_completed = subprocess.run(
        status_cmd,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert status_completed.returncode == 0, (
        f"pipeline_status failed\nSTDOUT:\n{status_completed.stdout}\nSTDERR:\n{status_completed.stderr}"
    )

    payload = json.loads(status_completed.stdout)
    assert payload["tables"]["market_universe_included"] == 3
    assert payload["tables"]["symbol_reference"] == 3
    assert payload["tables"]["price_latest"] == 3
    assert payload["tables"]["finra_short_interest_latest"] == 3
    assert payload["tables"]["news_symbol_candidates"] == 3
    assert payload["samples"]["market_universe_included_symbols"] == "AAPL, BABA, MSFT"
    assert payload["samples"]["news_candidate_pairs"] == "1001:AAPL, 1002:MSFT, 1003:BABA"
