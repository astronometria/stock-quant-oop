"""
Instrument master builder (minimal viable).

Objectif:
- créer une identité stable (instrument_id)
- basé sur symbol (phase 1)
"""

from __future__ import annotations

import json
import duckdb


def build_instrument_master(con: duckdb.DuckDBPyConnection):
    con.execute("DROP TABLE IF EXISTS instrument_master")

    con.execute("""
        CREATE TABLE instrument_master AS
        SELECT
            symbol,
            symbol AS instrument_id,
            symbol AS company_id,
            TRUE AS is_active
        FROM (
            SELECT DISTINCT symbol
            FROM price_source_daily_raw
        )
    """)

    row = con.execute("""
        SELECT COUNT(*) FROM instrument_master
    """).fetchone()

    return {
        "instrument_count": row[0]
    }


def main():
    con = duckdb.connect("market.duckdb")
    try:
        print(json.dumps(build_instrument_master(con), indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
