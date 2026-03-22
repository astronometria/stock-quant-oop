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
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable

from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager


EXPECTED_HEADER_5 = "Date|Symbol|ShortVolume|TotalVolume|Market"
EXPECTED_HEADER_6_A = "Date|Symbol|ShortExemptVolume|ShortVolume|TotalVolume|Market"
EXPECTED_HEADER_6_B = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"

ALLOWED_MARKET_DIRS = {"CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF"}
BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fast incremental loader for FINRA daily short volume raw files."
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
        "--default-root",
        default="~/stock-quant-oop-raw/data/raw/finra/daily_short_sale_volume",
        help="Default disk root scanned when no explicit --source is supplied.",
    )
    parser.add_argument(
        "--source-name",
        default="finra_disk_raw",
        help="Source name written into DB.",
    )
    parser.add_argument("--memory-limit", default="24GB", help="DuckDB memory limit.")
    parser.add_argument("--threads", type=int, default=6, help="DuckDB threads.")
    parser.add_argument(
        "--temp-dir",
        default="~/stock-quant-oop-runtime/tmp",
        help="DuckDB temp directory.",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def log(message: str, verbose: bool) -> None:
    if verbose:
        print(message, flush=True)


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_file_list(paths: list[str]) -> str:
    return "[" + ", ".join(sql_quote(path) for path in paths) + "]"


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def looks_like_manifest(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() == ".csv" and "manifest" in path.name.lower()


def iter_manifest_paths(path: Path) -> list[Path]:
    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT COALESCE(local_path, path) AS p
            FROM read_csv_auto(?, header=TRUE)
            WHERE lower(trim(COALESCE(status, ''))) IN ('downloaded', 'already_present')
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


def expand_source_item(item: str) -> list[Path]:
    if any(ch in item for ch in ["*", "?", "["]):
        return [p.resolve() for p in sorted(Path().glob(item)) if p.is_file()]

    path = Path(item).expanduser().resolve()

    if path.is_dir():
        return sorted(p.resolve() for p in path.rglob("*") if p.is_file())

    if looks_like_manifest(path):
        return iter_manifest_paths(path)

    if path.is_file():
        return [path]

    return []


def discover_files(sources: Iterable[str], manifests: Iterable[str], default_root: str) -> list[Path]:
    discovered: list[Path] = []

    for item in sources:
        discovered.extend(expand_source_item(item))

    for item in manifests:
        discovered.extend(iter_manifest_paths(Path(item).expanduser().resolve()))

    if not discovered:
        default_path = Path(default_root).expanduser().resolve()
        if default_path.exists():
            discovered.extend(sorted(p.resolve() for p in default_path.rglob("*") if p.is_file()))

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


def nonempty_lines(path: Path, limit: int = 20) -> list[str]:
    lines: list[str] = []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                value = line.strip()
                if value:
                    lines.append(value)
                if len(lines) >= limit:
                    break
    except Exception:
        return []
    return lines


def looks_like_valid_payload_date(token: str) -> bool:
    token = token.strip()
    return len(token) == 8 and token.isdigit()


def first_real_payload_line(lines: list[str]) -> str | None:
    for line in lines:
        if line == "0":
            continue
        return line
    return None


def split_files(paths: list[Path]) -> tuple[list[str], list[str], list[str], list[str], int, int]:
    header_5_files: list[str] = []
    header_6a_files: list[str] = []
    header_6b_files: list[str] = []
    no_header_5_files: list[str] = []
    no_header_6_files: list[str] = []
    empty_files = 0
    skipped_bad_payload_files = 0

    for path in tqdm(paths, desc="probe_finra", unit="file", dynamic_ncols=True):
        lines = nonempty_lines(path, limit=20)
        if not lines:
            empty_files += 1
            continue

        first = lines[0]
        remaining = lines[1:]

        if first == EXPECTED_HEADER_5:
            payload = first_real_payload_line(remaining)
            if payload is None:
                empty_files += 1
                continue
            first_token = payload.split("|")[0].strip()
            if not looks_like_valid_payload_date(first_token):
                skipped_bad_payload_files += 1
                continue
            header_5_files.append(str(path))
            continue

        if first == EXPECTED_HEADER_6_A:
            payload = first_real_payload_line(remaining)
            if payload is None:
                empty_files += 1
                continue
            first_token = payload.split("|")[0].strip()
            if not looks_like_valid_payload_date(first_token):
                skipped_bad_payload_files += 1
                continue
            header_6a_files.append(str(path))
            continue

        if first == EXPECTED_HEADER_6_B:
            payload = first_real_payload_line(remaining)
            if payload is None:
                empty_files += 1
                continue
            first_token = payload.split("|")[0].strip()
            if not looks_like_valid_payload_date(first_token):
                skipped_bad_payload_files += 1
                continue
            header_6b_files.append(str(path))
            continue

        payload = first_real_payload_line(lines)
        if payload is None:
            empty_files += 1
            continue

        parts = [p.strip() for p in payload.split("|")]

        if len(parts) == 5 and looks_like_valid_payload_date(parts[0]):
            no_header_5_files.append(str(path))
            continue

        if len(parts) == 6 and looks_like_valid_payload_date(parts[0]):
            no_header_6_files.append(str(path))
            continue

        skipped_bad_payload_files += 1

    header_6_files = header_6a_files + header_6b_files

    return (
        header_5_files,
        header_6_files,
        no_header_5_files,
        no_header_6_files,
        empty_files,
        skipped_bad_payload_files,
    )


def ensure_incremental_support(con: duckdb.DuckDBPyConnection) -> None:
    ShortDataSchemaManager().ensure_all(con)

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_daily_short_volume_source_raw_source_file
        ON finra_daily_short_volume_source_raw(source_name, source_file)
        """
    )

    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_finra_daily_short_volume_sources_source_file
        ON finra_daily_short_volume_sources(source_name, source_file)
        """
    )


def already_loaded_files(con: duckdb.DuckDBPyConnection, source_name: str) -> set[str]:
    rows = con.execute(
        """
        SELECT DISTINCT source_file
        FROM finra_daily_short_volume_source_raw
        WHERE source_name = ?
        """,
        [source_name],
    ).fetchall()

    return {str(row[0]) for row in rows if row and row[0] is not None}


def create_stage(con: duckdb.DuckDBPyConnection, stage_name: str) -> None:
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


def insert_from_sql(con: duckdb.DuckDBPyConnection, sql: str, stage_name: str) -> int:
    before = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    con.execute(sql)
    after = con.execute(f"SELECT COUNT(*) FROM {stage_name}").fetchone()[0]
    return int(after - before)


def read_csv_common_options() -> str:
    return """
        delim='|',
        filename=true,
        all_varchar=true,
        ignore_errors=true,
        union_by_name=true,
        strict_mode=false,
        null_padding=true,
        max_line_size=10000000
    """


def insert_batch_header5_sql(stage_name: str, files: list[str], source_name: str) -> str:
    return f"""
        INSERT INTO {stage_name}
        SELECT
            CAST(strptime(Date, '%Y%m%d') AS DATE) AS trade_date,
            upper(trim(Symbol)) AS symbol,
            CAST(ShortVolume AS DOUBLE) AS short_volume,
            NULL::DOUBLE AS short_exempt_volume,
            CAST(TotalVolume AS DOUBLE) AS total_volume,
            upper(trim(Market)) AS market_code,
            filename AS source_file,
            CAST(
                strptime(
                    regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1),
                    '%Y%m%d'
                ) AS DATE
            ) AS publication_date,
            CURRENT_TIMESTAMP AS available_at,
            {sql_quote(source_name)} AS source_name
        FROM read_csv(
            {sql_file_list(files)},
            header=true,
            {read_csv_common_options()}
        )
        WHERE regexp_matches(trim(coalesce(Date, '')), '^[0-9]{{8}}$')
          AND trim(coalesce(Symbol, '')) <> ''
    """


def insert_batch_header6_sql(stage_name: str, files: list[str], source_name: str) -> str:
    return f"""
        INSERT INTO {stage_name}
        SELECT
            CAST(strptime(Date, '%Y%m%d') AS DATE) AS trade_date,
            upper(trim(Symbol)) AS symbol,
            CAST(ShortVolume AS DOUBLE) AS short_volume,
            CAST(ShortExemptVolume AS DOUBLE) AS short_exempt_volume,
            CAST(TotalVolume AS DOUBLE) AS total_volume,
            upper(trim(Market)) AS market_code,
            filename AS source_file,
            CAST(
                strptime(
                    regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1),
                    '%Y%m%d'
                ) AS DATE
            ) AS publication_date,
            CURRENT_TIMESTAMP AS available_at,
            {sql_quote(source_name)} AS source_name
        FROM read_csv(
            {sql_file_list(files)},
            header=true,
            {read_csv_common_options()}
        )
        WHERE regexp_matches(trim(coalesce(Date, '')), '^[0-9]{{8}}$')
          AND trim(coalesce(Symbol, '')) <> ''
    """


def insert_batch_no_header5_sql(stage_name: str, files: list[str], source_name: str) -> str:
    return f"""
        INSERT INTO {stage_name}
        SELECT
            CAST(strptime(column0, '%Y%m%d') AS DATE) AS trade_date,
            upper(trim(column1)) AS symbol,
            CAST(column2 AS DOUBLE) AS short_volume,
            NULL::DOUBLE AS short_exempt_volume,
            CAST(column3 AS DOUBLE) AS total_volume,
            upper(trim(column4)) AS market_code,
            filename AS source_file,
            CAST(
                strptime(
                    regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1),
                    '%Y%m%d'
                ) AS DATE
            ) AS publication_date,
            CURRENT_TIMESTAMP AS available_at,
            {sql_quote(source_name)} AS source_name
        FROM read_csv(
            {sql_file_list(files)},
            header=false,
            {read_csv_common_options()}
        )
        WHERE regexp_matches(trim(coalesce(column0, '')), '^[0-9]{{8}}$')
          AND trim(coalesce(column1, '')) <> ''
    """


def insert_batch_no_header6_sql(stage_name: str, files: list[str], source_name: str) -> str:
    return f"""
        INSERT INTO {stage_name}
        SELECT
            CAST(strptime(column0, '%Y%m%d') AS DATE) AS trade_date,
            upper(trim(column1)) AS symbol,
            CAST(column2 AS DOUBLE) AS short_volume,
            CAST(column3 AS DOUBLE) AS short_exempt_volume,
            CAST(column4 AS DOUBLE) AS total_volume,
            upper(trim(column5)) AS market_code,
            filename AS source_file,
            CAST(
                strptime(
                    regexp_extract(regexp_extract(filename, '[^/\\\\]+$', 0), '(\\d{{8}})', 1),
                    '%Y%m%d'
                ) AS DATE
            ) AS publication_date,
            CURRENT_TIMESTAMP AS available_at,
            {sql_quote(source_name)} AS source_name
        FROM read_csv(
            {sql_file_list(files)},
            header=false,
            {read_csv_common_options()}
        )
        WHERE regexp_matches(trim(coalesce(column0, '')), '^[0-9]{{8}}$')
          AND trim(coalesce(column1, '')) <> ''
    """


def insert_with_fallback(
    con: duckdb.DuckDBPyConnection,
    stage_name: str,
    batch: list[str],
    batch_sql_builder,
    source_name: str,
    bad_files: list[dict],
) -> int:
    try:
        sql = batch_sql_builder(stage_name, batch, source_name)
        return insert_from_sql(con, sql, stage_name)
    except Exception:
        inserted = 0
        for path in batch:
            try:
                sql = batch_sql_builder(stage_name, [path], source_name)
                inserted += insert_from_sql(con, sql, stage_name)
            except Exception as single_exc:
                bad_files.append(
                    {
                        "file": path,
                        "error": str(single_exc),
                    }
                )
        return inserted


def refresh_sources_for_new_files(con: duckdb.DuckDBPyConnection, source_name: str, stage_name: str) -> None:
    con.execute(
        f"""
        DELETE FROM finra_daily_short_volume_sources
        WHERE source_name = ?
          AND source_file IN (
              SELECT DISTINCT source_file
              FROM {stage_name}
          )
        """,
        [source_name],
    )

    con.execute(
        f"""
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
        FROM {stage_name}
        GROUP BY 1, 2, 3
        """
    )


def insert_stage_into_raw(con: duckdb.DuckDBPyConnection, stage_name: str) -> int:
    before = con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]

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
        WHERE symbol IS NOT NULL
          AND trade_date IS NOT NULL
        """
    )

    after = con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
    return int(after - before)


def main() -> int:
    args = parse_args()

    db_path = str(Path(args.db_path).expanduser().resolve())
    repo_root = Path(__file__).resolve().parents[2]
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    bad_files_log = logs_dir / "finra_daily_short_volume_bad_files.json"

    source_items = list(args.sources or [])
    manifest_items = list(args.manifests or [])

    discovered = discover_files(source_items, manifest_items, args.default_root)
    if not discovered:
        raise SystemExit("No FINRA daily short volume files discovered.")

    con = duckdb.connect(db_path)
    try:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory={sql_quote(str(Path(args.temp_dir).expanduser().resolve()))}")

        ensure_incremental_support(con)

        loaded = already_loaded_files(con, args.source_name)
        to_load = [str(path.resolve()) for path in discovered if str(path.resolve()) not in loaded]

        print(f"[load_finra_daily_short_volume_raw] discovered_files={len(discovered)}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] already_loaded_files={len(loaded)}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] files_to_load={len(to_load)}", flush=True)

        if not to_load:
            print(
                json.dumps(
                    {
                        "db_path": db_path,
                        "discovered_files": len(discovered),
                        "already_loaded_files": len(loaded),
                        "files_to_load": 0,
                        "stage_inserted_rows": 0,
                        "final_raw_rows": int(
                            con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
                        ),
                    },
                    default=str,
                ),
                flush=True,
            )
            return 0

        h5, h6, nh5, nh6, empty_files, skipped_bad = split_files([Path(p) for p in to_load])

        stage_name = "tmp_finra_daily_short_volume_stage"
        create_stage(con, stage_name)

        bad_files: list[dict] = []
        inserted_stage_rows = 0

        for batch in tqdm(list(chunked(h5, BATCH_SIZE)), desc="load_h5", dynamic_ncols=True):
            inserted_stage_rows += insert_with_fallback(
                con, stage_name, batch, insert_batch_header5_sql, args.source_name, bad_files
            )

        for batch in tqdm(list(chunked(h6, BATCH_SIZE)), desc="load_h6", dynamic_ncols=True):
            inserted_stage_rows += insert_with_fallback(
                con, stage_name, batch, insert_batch_header6_sql, args.source_name, bad_files
            )

        for batch in tqdm(list(chunked(nh5, BATCH_SIZE)), desc="load_nh5", dynamic_ncols=True):
            inserted_stage_rows += insert_with_fallback(
                con, stage_name, batch, insert_batch_no_header5_sql, args.source_name, bad_files
            )

        for batch in tqdm(list(chunked(nh6, BATCH_SIZE)), desc="load_nh6", dynamic_ncols=True):
            inserted_stage_rows += insert_with_fallback(
                con, stage_name, batch, insert_batch_no_header6_sql, args.source_name, bad_files
            )

        print("[phase] start insert_stage_into_raw", flush=True)
        inserted_raw_rows = insert_stage_into_raw(con, stage_name)
        print("[phase] done insert_stage_into_raw", flush=True)

        print("[phase] start refresh_sources_for_new_files", flush=True)
        refresh_sources_for_new_files(con, args.source_name, stage_name)
        print("[phase] done refresh_sources_for_new_files", flush=True)

        final_raw_rows = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0])
        final_source_rows = int(con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_sources").fetchone()[0])

        bad_files_log.write_text(
            json.dumps(
                {
                    "bad_file_count": len(bad_files),
                    "bad_files": bad_files,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        print(f"[load_finra_daily_short_volume_raw] db_path={db_path}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] empty_files={empty_files}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] skipped_bad_payload_files={skipped_bad}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] batch_parse_failures={len(bad_files)}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] stage_inserted_rows={inserted_stage_rows}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] inserted_raw_rows={inserted_raw_rows}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] final_raw_rows={final_raw_rows}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] final_source_rows={final_source_rows}", flush=True)
        print(f"[load_finra_daily_short_volume_raw] bad_files_log={bad_files_log}", flush=True)

        print(
            json.dumps(
                {
                    "db_path": db_path,
                    "discovered_files": len(discovered),
                    "already_loaded_files": len(loaded),
                    "files_to_load": len(to_load),
                    "header5_files": len(h5),
                    "header6_files": len(h6),
                    "no_header5_files": len(nh5),
                    "no_header6_files": len(nh6),
                    "empty_files": empty_files,
                    "skipped_bad_payload_files": skipped_bad,
                    "bad_file_count": len(bad_files),
                    "stage_inserted_rows": inserted_stage_rows,
                    "inserted_raw_rows": inserted_raw_rows,
                    "final_raw_rows": final_raw_rows,
                    "final_source_rows": final_source_rows,
                    "bad_files_log": str(bad_files_log),
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
