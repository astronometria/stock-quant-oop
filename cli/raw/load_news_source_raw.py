#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.providers.news.news_source_loader import NewsSourceLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load news raw files through provider layer.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="CSV source path. Repeat this flag for multiple inputs.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Optional end date YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit.")
    return parser.parse_args()


def parse_date(value: str | None):
    if value is None:
        return None
    from datetime import date
    return date.fromisoformat(value)


def main() -> int:
    args = parse_args()
    loader = NewsSourceLoader(source_paths=args.sources)

    frame = loader.fetch_news_frame(
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        limit=args.limit,
    )

    result = {
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "sample": frame.head(10).to_dict(orient="records"),
    }
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
