#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import duckdb

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable

from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager


EXPECTED_HEADER_5 = "Date|Symbol|ShortVolume|TotalVolume|Market"
EXPECTED_HEADER_6 = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"
ALLOWED_MARKET_DIRS = {"CNMS", "FNQC", "FNRA", "FNSQ", "FNYX"}
BATCH_SIZE = 100


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load FINRA daily short volume raw files into DuckDB using robust historical format handling."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Source path. Can be a file, directory, glob, or manifest CSV. Repeat this flag.",
    )
    parser.add_argument(
        "--manifest",
        action="append",
        dest="manifests",
        default=[],
        help="Manifest CSV path. Repeat this flag.",
    )
    parser.add_argument(
        "--source-name",
        default="finra_daily_non_otc",
        help="Source name written to DB.",
    )
    return parser.parse_args()


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_file_list(paths: list[str]) -> str:
    return "[" + ", ".join(_sql_quote(p) for p in paths) + "]"


def _chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _looks_like_manifest(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".csv" and "manifest" in path.name.lower()


def _iter_manifest_paths(path: Path) -> list[Path]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT path
            FROM read_csv_auto(?, header=TRUE)
            WHERE lower(trim(status)) = 'downloaded'
            """,
            [str(path)],
        ).fetchall()
    finally:
        con.close()

    out: list[Path] = []
    for row in rows:
        candidate = Path(str(row[0])).expanduser().resolve()
        if candidate.is_file():
            out.append(candidate)
    return out


def _expand_source_item(item: str) -> list[Path]:
    if any(ch in item for ch in ["*", "?", "["]):
        return [p.resolve() for p in sorted(Path().glob(item)) if p.is_file()]

    path = Path(item).expanduser().resolve()

    if path.is_dir():
        return sorted(p.resolve() for p in path.rglob("*") if p.is_file())

    if _looks_like_manifest(path):
        return _iter_manifest_paths(path)

    if path.is_file():
        return [path]

    return []


def discover_files(sources: Iterable[str], manifests: Iterable[str]) -> list[Path]:
    discovered: list[Path] = []

    for item in sources:
        discovered.extend(_expand_source_item(item))

    for item in manifests:
        discovered.extend(_iter_manifest_paths(Path(item).expanduser().resolve()))

    filtered: list[Path] = []
    seen: set[str] = set()

    for path in discovered:
        name = path.name.lower()
        parent = path.parent.name.upper()

        if parent not in ALLOWED_MARKET_DIRS:
            continue
        if not (name.endswith(".txt") or name.endswith(".csv")):
            continue

        key = str(path.resolve())
        if key in seen:
            continue

        seen.add(key)
        filtered.append(path.resolve())

    return sorted(filtered)


def _nonempty_lines(path: Path, limit: int = 3) -> list[str]:
    lines: list[str] = []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            value = line.strip()
            if value:
                lines.append(value)
                if len(lines) >= limit:
                    break
    return lines


def _looks_like_valid_payload_date(token: str) -> bool:
    token = token.strip()
    return len(token) == 8 and token.isdigit()


def split_files(paths: list[Path]) -> tuple[list[str], list[str], list[str], list[str], int, int]:
    header_5_files: list[str] = []
    header_6_files: list[str] = []
    no_header_5_files: list[str] = []
    no_header_6_files: list[str] = []
    empty_files = 0
    skipped_bad_payload_files = 0

    for path in tqdm(paths, desc="probe_finra", unit="file", dynamic_ncols=True):
        lines = _nonempty_lines(path, limit=3)
        if not lines:
            empty_files += 1
            continue

        first = lines[0]
        second = lines[1] if len(lines) > 1 else ""

        if first == EXPECTED_HEADER_5:
            if not second or not _looks_like_valid_payload_date(second.split("|")[0]):
                skipped_bad_payload_files += 1
                continue
            header_5_files.append(str(path))
            continue

        if first == EXPECTED_HEADER_6:
            if not second or not _looks_like_valid_payload_date(second.split("|")[0]):
                skipped_bad_payload_files += 1
                continue
            header_6_files.append(str(path))
            continue

        first_parts = [p.strip() for p in first.split("|")]

        if len(first_parts) == 5 and _looks_like_valid_payload_date(first_parts[0]):
            no_header_5_files.append(str(path))
            continue

        if len(first_parts) == 6 and _looks_like_valid_payload_date(first_parts[0]):
            no_header_6_files.append(str(path))
            continue

        skipped_bad_payload_files += 1

    return (
        header_5_files,
        header_6_files,
        no_header_5_files,
        no_header_6_files,
        empty_files,
        skipped_bad_payload_files,
    )


def _create_stage(con: duckdb.DuckDBPyConnection, stage_name: str) -> None:
    con.execute(f"DROP TABLE IF EXISTS {stage_name}")
    con.execute(
        f"""
        CREATE TEMP TABLE {stage_name} (
            trade_date DATE,
            symbol VARCHAR,
            short_volume DOUBLE,
            short_exempt_volume DOUBLE,
            total_volume DOUBLE,
            market_code VARCHAR,
            source_file VARCHAR,
            publication_date DATE,
            available_at TIMESTAMP,
            source_name VARCHAR
        )
        """
    )


def _insert_from_sql(con: duckdb.DuckDBPyConnection, sql: str, stage_name: str) -> int:
    before = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    con.execute(sql)
    after = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    return int(after - before)


def _insert_batch_header5(con, stage_name: str, files: list[str], source_name: str) -> int:
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(strptime(Date, '%Y%m%d') AS DATE) AS trade_date,
        upper(trim(Symbol)) AS symbol,
        CAST(ShortVolume AS DOUBLE) AS short_volume,
        NULL::DOUBLE AS short_exempt_volume,
        CAST(TotalVolume AS DOUBLE) AS total_volume,
        upper(trim(Market)) AS market_code,
        regexp_extract(filename, '[^/\\\\]+$', 0) AS source_file,
        CAST(strptime(regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1), '%Y%m%d') AS DATE) AS publication_date,
        CURRENT_TIMESTAMP AS available_at,
        {_sql_quote(source_name)} AS source_name
    FROM read_csv(
        {_sql_file_list(files)},
        delim='|',
        header=true,
        filename=true,
        all_varchar=true,
        ignore_errors=true,
        union_by_name=true
    )
    WHERE trim(coalesce(Symbol, '')) <> ''
    """
    return _insert_from_sql(con, sql, stage_name)


def _insert_batch_header6(con, stage_name: str, files: list[str], source_name: str) -> int:
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(strptime(Date, '%Y%m%d') AS DATE) AS trade_date,
        upper(trim(Symbol)) AS symbol,
        CAST(ShortVolume AS DOUBLE) AS short_volume,
        CAST(ShortExemptVolume AS DOUBLE) AS short_exempt_volume,
        CAST(TotalVolume AS DOUBLE) AS total_volume,
        upper(trim(Market)) AS market_code,
        regexp_extract(filename, '[^/\\\\]+$', 0) AS source_file,
        CAST(strptime(regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1), '%Y%m%d') AS DATE) AS publication_date,
        CURRENT_TIMESTAMP AS available_at,
        {_sql_quote(source_name)} AS source_name
    FROM read_csv(
        {_sql_file_list(files)},
        delim='|',
        header=true,
        filename=true,
        all_varchar=true,
        ignore_errors=true,
        union_by_name=true
    )
    WHERE trim(coalesce(Symbol, '')) <> ''
    """
    return _insert_from_sql(con, sql, stage_name)


def _insert_batch_no_header5(con, stage_name: str, files: list[str], source_name: str) -> int:
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(strptime(column0, '%Y%m%d') AS DATE) AS trade_date,
        upper(trim(column1)) AS symbol,
        CAST(column2 AS DOUBLE) AS short_volume,
        NULL::DOUBLE AS short_exempt_volume,
        CAST(column3 AS DOUBLE) AS total_volume,
        upper(trim(column4)) AS market_code,
        regexp_extract(filename, '[^/\\\\]+$', 0) AS source_file,
        CAST(strptime(regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1), '%Y%m%d') AS DATE) AS publication_date,
        CURRENT_TIMESTAMP AS available_at,
        {_sql_quote(source_name)} AS source_name
    FROM read_csv(
        {_sql_file_list(files)},
        delim='|',
        header=false,
        filename=true,
        all_varchar=true,
        ignore_errors=true,
        union_by_name=true
    )
    WHERE trim(coalesce(column1, '')) <> ''
    """
    return _insert_from_sql(con, sql, stage_name)


def _insert_batch_no_header6(con, stage_name: str, files: list[str], source_name: str) -> int:
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(strptime(column0, '%Y%m%d') AS DATE) AS trade_date,
        upper(trim(column1)) AS symbol,
        CAST(column2 AS DOUBLE) AS short_volume,
        CAST(column3 AS DOUBLE) AS short_exempt_volume,
        CAST(column4 AS DOUBLE) AS total_volume,
        upper(trim(column5)) AS market_code,
        regexp_extract(filename, '[^/\\\\]+$', 0) AS source_file,
        CAST(strptime(regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1), '%Y%m%d') AS DATE) AS publication_date,
        CURRENT_TIMESTAMP AS available_at,
        {_sql_quote(source_name)} AS source_name
    FROM read_csv(
        {_sql_file_list(files)},
        delim='|',
        header=false,
        filename=true,
        all_varchar=true,
        ignore_errors=true,
        union_by_name=true
    )
    WHERE trim(coalesce(column1, '')) <> ''
    """
    return _insert_from_sql(con, sql, stage_name)


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    source_items = list(args.sources or [])
    manifest_items = list(args.manifests or [])

    if not source_items and not manifest_items:
        raise SystemExit("At least one --source or --manifest is required.")

    discovered = discover_files(source_items, manifest_items)
    if not discovered:
        raise SystemExit("No FINRA daily short volume files discovered.")

    con = duckdb.connect(db_path)
    try:
        schema = ShortDataSchemaManager()
        schema.ensure_all(con)

        rows_before = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0])

        stage_name = "tmp_finra_daily_short_volume_stage"
        _create_stage(con, stage_name)

        h5, h6, nh5, nh6, empty_files, skipped_bad = split_files(discovered)

        inserted = 0
        for batch in tqdm(list(_chunked(h5, BATCH_SIZE)), desc="load_h5", dynamic_ncols=True):
            inserted += _insert_batch_header5(con, stage_name, batch, args.source_name)
        for batch in tqdm(list(_chunked(h6, BATCH_SIZE)), desc="load_h6", dynamic_ncols=True):
            inserted += _insert_batch_header6(con, stage_name, batch, args.source_name)
        for batch in tqdm(list(_chunked(nh5, BATCH_SIZE)), desc="load_nh5", dynamic_ncols=True):
            inserted += _insert_batch_no_header5(con, stage_name, batch, args.source_name)
        for batch in tqdm(list(_chunked(nh6, BATCH_SIZE)), desc="load_nh6", dynamic_ncols=True):
            inserted += _insert_batch_no_header6(con, stage_name, batch, args.source_name)

        con.execute("DELETE FROM finra_daily_short_volume_source_raw")
        con.execute(
            f"""
            INSERT INTO finra_daily_short_volume_source_raw (
                trade_date,
                symbol,
                short_volume,
                short_exempt_volume,
                total_volume,
                market_code,
                source_file,
                publication_date,
                available_at,
                source_name
            )
            SELECT
                trade_date,
                symbol,
                short_volume,
                short_exempt_volume,
                total_volume,
                market_code,
                source_file,
                publication_date,
                available_at,
                source_name
            FROM {stage_name}
            """
        )

        con.execute("DELETE FROM finra_daily_short_volume_sources")
        con.execute(
            """
            INSERT INTO finra_daily_short_volume_sources (
                source_file,
                publication_date,
                market_code,
                row_count,
                loaded_at,
                source_name
            )
            SELECT
                source_file,
                publication_date,
                market_code,
                COUNT(*) AS row_count,
                CURRENT_TIMESTAMP AS loaded_at,
                MAX(source_name) AS source_name
            FROM finra_daily_short_volume_source_raw
            GROUP BY 1,2,3
            """
        )

        rows_after = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0])
        source_rows_after = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_sources").fetchone()[0])

        print(f"[load_finra_daily_short_volume_raw] db_path={db_path}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] discovered_files={len(discovered)}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] empty_files={empty_files}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] skipped_bad_payload_files={skipped_bad}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] rows_before={rows_before}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] rows_written={rows_after}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] source_rows_after={source_rows_after}", flush=True)
        print(
            json.dumps(
                {
                    "db_path": db_path,
                    "discovered_files": len(discovered),
                    "header5_files": len(h5),
                    "header6_files": len(h6),
                    "no_header5_files": len(nh5),
                    "no_header6_files": len(nh6),
                    "empty_files": empty_files,
                    "skipped_bad_payload_files": skipped_bad,
                    "stage_inserted_rows": inserted,
                    "final_raw_rows": rows_after,
                    "final_source_rows": source_rows_after,
                },
                default=str,
            ),
            flush=True,
        )
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
