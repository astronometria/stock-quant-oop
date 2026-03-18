from __future__ import annotations

import duckdb

from cli.run_daily_pipeline import _compute_table_signature


def test_compute_table_signature_detects_date_column_price_history(tmp_path):
    """
    Vérifie que la signature détecte correctement une colonne date classique.
    """
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    try:
        con.execute("""
            CREATE TABLE price_history (
                symbol VARCHAR,
                date DATE,
                close DOUBLE
            )
        """)

        con.execute("""
            INSERT INTO price_history VALUES
                ('AAPL', DATE '2026-03-17', 200),
                ('AAPL', DATE '2026-03-18', 210)
        """)

        sig = _compute_table_signature(con, "price_history")

        assert sig["row_count"] == 2
        assert str(sig["min_business_date"]) == "2026-03-17"
        assert str(sig["max_business_date"]) == "2026-03-18"

    finally:
        con.close()


def test_compute_table_signature_detects_trade_date(tmp_path):
    """
    Vérifie support trade_date (FINRA style)
    """
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    try:
        con.execute("""
            CREATE TABLE daily_short_volume_history (
                symbol VARCHAR,
                trade_date DATE
            )
        """)

        con.execute("""
            INSERT INTO daily_short_volume_history VALUES
                ('AAPL', DATE '2026-03-10'),
                ('AAPL', DATE '2026-03-11')
        """)

        sig = _compute_table_signature(con, "daily_short_volume_history")

        assert str(sig["min_business_date"]) == "2026-03-10"
        assert str(sig["max_business_date"]) == "2026-03-11"

    finally:
        con.close()
