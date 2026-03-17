import duckdb


def test_training_dataset_no_duplicates(tmp_path):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    # simulate minimal schema
    con.execute("""
    CREATE TABLE training_dataset_daily (
        symbol VARCHAR,
        price_date DATE
    )
    """)

    con.execute("""
    INSERT INTO training_dataset_daily VALUES
    ('AAPL', '2024-01-01'),
    ('AAPL', '2024-01-01')
    """)

    result = con.execute("""
        SELECT symbol, price_date, COUNT(*) c
        FROM training_dataset_daily
        GROUP BY 1,2
        HAVING c > 1
    """).fetchall()

    assert len(result) == 1  # should detect duplicates


def test_available_at_enforced(tmp_path):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))

    con.execute("""
    CREATE TABLE price_history(symbol VARCHAR, price_date DATE)
    """)

    con.execute("""
    CREATE TABLE fundamental_features_daily(symbol VARCHAR, available_at TIMESTAMP)
    """)

    # future data (invalid)
    con.execute("""
    INSERT INTO price_history VALUES ('AAPL', '2024-01-01')
    """)

    con.execute("""
    INSERT INTO fundamental_features_daily VALUES ('AAPL', '2025-01-01')
    """)

    rows = con.execute("""
        SELECT *
        FROM price_history p
        LEFT JOIN fundamental_features_daily f
          ON p.symbol = f.symbol
         AND f.available_at <= p.price_date
    """).fetchall()

    # no join should happen
    assert rows[0][2] is None
