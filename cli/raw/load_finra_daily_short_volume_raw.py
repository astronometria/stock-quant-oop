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

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


EXPECTED_HEADER_5 = "Date|Symbol|ShortVolume|TotalVolume|Market"
EXPECTED_HEADER_6 = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"
ALLOWED_MARKET_DIRS = {"CNMS", "FNQC", "FNRA", "FNSQ", "FNYX"}
BATCH_SIZE = 250


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load FINRA daily short volume raw files into DuckDB using robust historical format handling."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
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
        return sorted(Path().glob(item))

    path = Path(item).expanduser().resolve()

    if path.is_dir():
        return sorted(p for p in path.rglob("*") if p.is_file())

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

    for path in tqdm(paths, desc="probe_finra", unit="file"):
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
            total_volume DOUBLE
        )
        """
    )


def _insert_from_sql(con: duckdb.DuckDBPyConnection, sql: str, stage_name: str) -> int:
    before = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    con.execute(sql)
    after = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    return int(after - before)


def _load_batch_header_5(con: duckdb.DuckDBPyConnection, files: list[str], stage_name: str) -> int:
    if not files:
        return 0
    file_list_sql = _sql_file_list(files)
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(TRY_STRPTIME(trim("Date"), '%Y%m%d') AS DATE) AS trade_date,
        UPPER(trim("Symbol")) AS symbol,
        TRY_CAST("ShortVolume" AS DOUBLE) AS short_volume,
        TRY_CAST("TotalVolume" AS DOUBLE) AS total_volume
    FROM read_csv(
        {file_list_sql},
        delim='|',
        header=TRUE,
        columns={{
            'Date':'VARCHAR',
            'Symbol':'VARCHAR',
            'ShortVolume':'VARCHAR',
            'TotalVolume':'VARCHAR',
            'Market':'VARCHAR'
        }},
        strict_mode=FALSE,
        ignore_errors=TRUE,
        null_padding=TRUE
    )
    """
    return _insert_from_sql(con, sql, stage_name)


def _load_batch_header_6(con: duckdb.DuckDBPyConnection, files: list[str], stage_name: str) -> int:
    if not files:
        return 0
    file_list_sql = _sql_file_list(files)
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(TRY_STRPTIME(trim("Date"), '%Y%m%d') AS DATE) AS trade_date,
        UPPER(trim("Symbol")) AS symbol,
        TRY_CAST("ShortVolume" AS DOUBLE) AS short_volume,
        TRY_CAST("TotalVolume" AS DOUBLE) AS total_volume
    FROM read_csv(
        {file_list_sql},
        delim='|',
        header=TRUE,
        columns={{
            'Date':'VARCHAR',
            'Symbol':'VARCHAR',
            'ShortVolume':'VARCHAR',
            'ShortExemptVolume':'VARCHAR',
            'TotalVolume':'VARCHAR',
            'Market':'VARCHAR'
        }},
        strict_mode=FALSE,
        ignore_errors=TRUE,
        null_padding=TRUE
    )
    """
    return _insert_from_sql(con, sql, stage_name)


def _load_batch_no_header_5(con: duckdb.DuckDBPyConnection, files: list[str], stage_name: str) -> int:
    if not files:
        return 0
    file_list_sql = _sql_file_list(files)
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(TRY_STRPTIME(trim(column0), '%Y%m%d') AS DATE) AS trade_date,
        UPPER(trim(column1)) AS symbol,
        TRY_CAST(column2 AS DOUBLE) AS short_volume,
        TRY_CAST(column3 AS DOUBLE) AS total_volume
    FROM read_csv(
        {file_list_sql},
        delim='|',
        header=FALSE,
        columns={{
            'column0':'VARCHAR',
            'column1':'VARCHAR',
            'column2':'VARCHAR',
            'column3':'VARCHAR',
            'column4':'VARCHAR'
        }},
        strict_mode=FALSE,
        ignore_errors=TRUE,
        null_padding=TRUE
    )
    """
    return _insert_from_sql(con, sql, stage_name)


def _load_batch_no_header_6(con: duckdb.DuckDBPyConnection, files: list[str], stage_name: str) -> int:
    if not files:
        return 0
    file_list_sql = _sql_file_list(files)
    sql = f"""
    INSERT INTO {stage_name}
    SELECT
        CAST(TRY_STRPTIME(trim(column0), '%Y%m%d') AS DATE) AS trade_date,
        UPPER(trim(column1)) AS symbol,
        TRY_CAST(column2 AS DOUBLE) AS short_volume,
        TRY_CAST(column4 AS DOUBLE) AS total_volume
    FROM read_csv(
        {file_list_sql},
        delim='|',
        header=FALSE,
        columns={{
            'column0':'VARCHAR',
            'column1':'VARCHAR',
            'column2':'VARCHAR',
            'column3':'VARCHAR',
            'column4':'VARCHAR',
            'column5':'VARCHAR'
        }},
        strict_mode=FALSE,
        ignore_errors=TRUE,
        null_padding=TRUE
    )
    """
    return _insert_from_sql(con, sql, stage_name)


def _load_grouped_files(
    con: duckdb.DuckDBPyConnection,
    files: list[str],
    loader,
    stage_name: str,
    desc: str,
) -> int:
    inserted = 0
    if not files:
        return inserted

    batches = list(_chunked(files, BATCH_SIZE))
    for batch in tqdm(batches, desc=desc, unit="batch"):
        inserted += loader(con=con, files=batch, stage_name=stage_name)
    return inserted


def build_into_db(
    con: duckdb.DuckDBPyConnection,
    paths: list[Path],
    source_name: str,
) -> dict[str, int]:
    (
        header_5_files,
        header_6_files,
        no_header_5_files,
        no_header_6_files,
        empty_files,
        skipped_bad_payload_files,
    ) = split_files(paths)

    _create_stage(con, "tmp_finra_stage")

    scanned_header_5_rows = _load_grouped_files(
        con=con,
        files=header_5_files,
        loader=_load_batch_header_5,
        stage_name="tmp_finra_stage",
        desc="load_header_5",
    )
    scanned_header_6_rows = _load_grouped_files(
        con=con,
        files=header_6_files,
        loader=_load_batch_header_6,
        stage_name="tmp_finra_stage",
        desc="load_header_6",
    )
    scanned_no_header_5_rows = _load_grouped_files(
        con=con,
        files=no_header_5_files,
        loader=_load_batch_no_header_5,
        stage_name="tmp_finra_stage",
        desc="load_no_header_5",
    )
    scanned_no_header_6_rows = _load_grouped_files(
        con=con,
        files=no_header_6_files,
        loader=_load_batch_no_header_6,
        stage_name="tmp_finra_stage",
        desc="load_no_header_6",
    )

    con.execute("DROP TABLE IF EXISTS tmp_finra_clean")
    con.execute(
        """
        CREATE TEMP TABLE tmp_finra_clean AS
        SELECT
            trade_date,
            symbol,
            short_volume,
            total_volume
        FROM tmp_finra_stage
        WHERE trade_date IS NOT NULL
          AND symbol IS NOT NULL
          AND trim(symbol) <> ''
          AND short_volume IS NOT NULL
          AND total_volume IS NOT NULL
          AND short_volume >= 0
          AND total_volume >= 0
        """
    )

    con.execute("DELETE FROM finra_daily_short_volume_source_raw")
    con.execute(
        """
        INSERT INTO finra_daily_short_volume_source_raw (
            symbol,
            trade_date,
            short_volume,
            total_volume,
            source_name,
            ingested_at
        )
        SELECT
            symbol,
            trade_date,
            SUM(short_volume) AS short_volume,
            SUM(total_volume) AS total_volume,
            ? AS source_name,
            CURRENT_TIMESTAMP AS ingested_at
        FROM tmp_finra_clean
        GROUP BY symbol, trade_date
        ORDER BY symbol, trade_date
        """,
        [source_name],
    )

    written = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0])

    return {
        "files_discovered": len(paths),
        "files_header_5": len(header_5_files),
        "files_header_6": len(header_6_files),
        "files_no_header_5": len(no_header_5_files),
        "files_no_header_6": len(no_header_6_files),
        "files_empty": empty_files,
        "files_skipped_bad_payload": skipped_bad_payload_files,
        "rows_scanned_header_5": scanned_header_5_rows,
        "rows_scanned_header_6": scanned_header_6_rows,
        "rows_scanned_no_header_5": scanned_no_header_5_rows,
        "rows_scanned_no_header_6": scanned_no_header_6_rows,
        "rows_scanned_total": (
            scanned_header_5_rows
            + scanned_header_6_rows
            + scanned_no_header_5_rows
            + scanned_no_header_6_rows
        ),
        "rows_written": written,
    }


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    files = discover_files(args.sources, args.manifests)

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema = ShortDataSchemaManager(uow)
        schema.initialize()

        if uow.connection is None:
            raise RuntimeError("active DB connection is required")

        metrics = build_into_db(
            con=uow.connection,
            paths=files,
            source_name=args.source_name,
        )

    payload = {
        "files_discovered": metrics["files_discovered"],
        "files_header_5": metrics["files_header_5"],
        "files_header_6": metrics["files_header_6"],
        "files_no_header_5": metrics["files_no_header_5"],
        "files_no_header_6": metrics["files_no_header_6"],
        "files_empty": metrics["files_empty"],
        "files_skipped_bad_payload": metrics["files_skipped_bad_payload"],
        "rows_scanned_header_5": metrics["rows_scanned_header_5"],
        "rows_scanned_header_6": metrics["rows_scanned_header_6"],
        "rows_scanned_no_header_5": metrics["rows_scanned_no_header_5"],
        "rows_scanned_no_header_6": metrics["rows_scanned_no_header_6"],
        "rows_scanned_total": metrics["rows_scanned_total"],
        "rows_written": metrics["rows_written"],
        "source_name": args.source_name,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
