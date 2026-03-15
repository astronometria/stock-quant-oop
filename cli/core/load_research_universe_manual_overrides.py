#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.research_universe_schema import ResearchUniverseSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_bool(value: str) -> bool:
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n"}:
        return False
    raise ValueError(f"invalid boolean value: {value}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load research universe manual overrides from CSV.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--csv-path",
        default="~/stock-quant-oop/config/manual_overrides/research_universe_manual_overrides.csv",
        help="CSV source path for manual overrides.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing manual overrides before insert.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def load_rows(csv_path: Path) -> list[tuple[str, bool, str]]:
    rows: list[tuple[str, bool, str]] = []
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"symbol", "include_in_research_universe", "override_reason"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(f"CSV missing required columns: {sorted(required)}")

        for record in reader:
            symbol = str(record.get("symbol", "")).strip().upper()
            if not symbol:
                continue
            include_value = parse_bool(record.get("include_in_research_universe", ""))
            reason = str(record.get("override_reason", "")).strip()
            if not reason:
                raise ValueError(f"override_reason is required for symbol={symbol}")
            rows.append((symbol, include_value, reason))
    return rows


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"csv path not found: {csv_path}")

    rows = load_rows(csv_path)

    if args.verbose:
        print(f"[load_research_universe_manual_overrides] project_root={config.project_root}")
        print(f"[load_research_universe_manual_overrides] db_path={config.db_path}")
        print(f"[load_research_universe_manual_overrides] csv_path={csv_path}")
        print(f"[load_research_universe_manual_overrides] rows_input={len(rows)}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchUniverseSchemaManager(uow).initialize()
        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.replace:
            con.execute("DELETE FROM research_universe_manual_overrides")

        if rows:
            con.executemany(
                """
                INSERT INTO research_universe_manual_overrides (
                    symbol,
                    include_in_research_universe,
                    override_reason,
                    created_at
                )
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                rows,
            )

        total_rows = int(
            con.execute("SELECT COUNT(*) FROM research_universe_manual_overrides").fetchone()[0]
        )

    print(
        json.dumps(
            {
                "rows_input": len(rows),
                "rows_written": len(rows),
                "table_total_rows": total_rows,
                "csv_path": str(csv_path),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
