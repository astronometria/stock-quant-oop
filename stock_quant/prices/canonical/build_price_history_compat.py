from __future__ import annotations

import json
import duckdb


def build_price_history_compat(con: duckdb.DuckDBPyConnection):
    con.execute("DROP TABLE IF EXISTS price_history")

    con.execute("""
        CREATE TABLE price_history AS
        SELECT
            symbol,
            price_date,
            open,
            high,
            low,
            close,
            volume,
            source_name,
            ingested_at
        FROM price_source_daily_raw
    """)

    row = con.execute("""
        SELECT COUNT(*), MIN(price_date), MAX(price_date), COUNT(DISTINCT symbol)
        FROM price_history
    """).fetchone()

    return {
        "table_name": "price_history",
        "rows": row[0],
        "min_price_date": str(row[1]),
        "max_price_date": str(row[2]),
        "symbol_count": row[3],
    }


def main():
    con = duckdb.connect("market.duckdb")
    try:
        print(json.dumps(build_price_history_compat(con), indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
