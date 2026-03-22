#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import duckdb


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def build(con):
    con.execute("DROP TABLE IF EXISTS research_split_dataset")

    con.execute("""
    CREATE TABLE research_split_dataset AS
    SELECT
        *,
        CASE
            WHEN as_of_date <= DATE '2017-12-31' THEN 'train'
            WHEN as_of_date <= DATE '2021-12-31' THEN 'val'
            ELSE 'test'
        END AS dataset_split
    FROM research_training_dataset
    """)

    stats = con.execute("""
        SELECT
            dataset_split,
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date),
            MAX(as_of_date),
            AVG(target_return),
            AVG(CAST(target_class AS DOUBLE))
        FROM research_split_dataset
        GROUP BY dataset_split
        ORDER BY dataset_split
    """).fetchall()

    return stats


def main():
    args = parse_args()
    con = duckdb.connect(args.db_path)
    try:
        stats = build(con)
        print("===== SPLIT STATS =====")
        for row in stats:
            print(row)
    finally:
        con.close()


if __name__ == "__main__":
    main()
