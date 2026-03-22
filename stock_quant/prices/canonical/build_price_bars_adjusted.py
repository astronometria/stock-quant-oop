"""
Adjusted price bars (phase 1 simple).

Pour l'instant:
adj_close = close
"""

from __future__ import annotations

import json
import duckdb


def build_price_bars_adjusted(con: duckdb.DuckDBPyConnection):
    con.execute("DROP TABLE IF EXISTS price_bars_adjusted")

    con.execute("""
        CREATE TABLE price_bars_adjusted AS
        SELECT
            symbol,
            price_date AS bar_date,
            open,
            high,
            low,
            close AS adj_close,
            volume
        FROM price_source_daily_raw
    """)

    row = con.execute("""
        SELECT COUNT(*), MIN(bar_date), MAX(bar_date)
        FROM price_bars_adjusted
    """).fetchone()

    return {
        "rows": row[0],
        "min_date": str(row[1]),
        "max_date": str(row[2])
    }


def main():
    con = duckdb.connect("market.duckdb")
    try:
        print(json.dumps(build_price_bars_adjusted(con), indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
