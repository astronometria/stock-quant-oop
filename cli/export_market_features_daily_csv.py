#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export market_features_daily to CSV."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--output-csv",
        required=True,
        help="Path to output CSV file.",
    )
    parser.add_argument(
        "--date-from",
        default=None,
        help="Inclusive start date filter (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        help="Inclusive end date filter (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated symbols filter, e.g. AAPL,MSFT,BABA",
    )
    parser.add_argument(
        "--drop-null-labels",
        action="store_true",
        help="Exclude rows where all forward return labels are NULL.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    args = parse_args()

    output_csv = Path(args.output_csv).expanduser().resolve()
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(args.db_path)

    where_clauses: list[str] = []

    if args.date_from:
        where_clauses.append(f"feature_date >= DATE {sql_quote(args.date_from)}")

    if args.date_to:
        where_clauses.append(f"feature_date <= DATE {sql_quote(args.date_to)}")

    if args.symbols:
        raw_symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if raw_symbols:
            symbol_list = ", ".join(sql_quote(s) for s in raw_symbols)
            where_clauses.append(f"symbol IN ({symbol_list})")

    if args.drop_null_labels:
        where_clauses.append(
            "("
            "fwd_return_1d IS NOT NULL OR "
            "fwd_return_5d IS NOT NULL OR "
            "fwd_return_10d IS NOT NULL"
            ")"
        )

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    count_sql = f"""
    SELECT COUNT(*)
    FROM market_features_daily
    {where_sql}
    """

    export_sql = f"""
    COPY (
        SELECT
            symbol,
            feature_date,
            close,
            volume,
            return_1d,
            return_5d,
            fwd_return_1d,
            fwd_return_5d,
            fwd_return_10d,
            news_count,
            avg_match_score,
            distinct_sources,
            distinct_domains,
            llm_news_count,
            positive_news_count,
            neutral_news_count,
            negative_news_count,
            avg_sentiment_score,
            avg_relevance_score,
            avg_materiality_score,
            avg_confidence
        FROM market_features_daily
        {where_sql}
        ORDER BY feature_date, symbol
    )
    TO {sql_quote(str(output_csv))}
    WITH (HEADER, DELIMITER ',');
    """

    row_count = int(con.execute(count_sql).fetchone()[0])
    con.execute(export_sql)
    con.close()

    if args.verbose:
        print(f"[export_market_features_daily_csv] db_path={args.db_path}")
        print(f"[export_market_features_daily_csv] output_csv={output_csv}")
        print(f"[export_market_features_daily_csv] row_count={row_count}")
        print(f"[export_market_features_daily_csv] date_from={args.date_from}")
        print(f"[export_market_features_daily_csv] date_to={args.date_to}")
        print(f"[export_market_features_daily_csv] symbols={args.symbols}")
        print(f"[export_market_features_daily_csv] drop_null_labels={args.drop_null_labels}")

    print(
        "Exported market_features_daily CSV: "
        f"row_count={row_count} output_csv={output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
