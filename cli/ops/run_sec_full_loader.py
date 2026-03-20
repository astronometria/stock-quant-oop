#!/usr/bin/env python3
from __future__ import annotations

"""
Full SEC loader orchestration.

Objectif
--------
Orchestrer un backfill SEC "complet" de manière observable et robuste:

1) init_sec_foundation
2) download + extract bulk SEC archives
   - submissions.zip
   - companyfacts.zip
3) load raw SEC bulk files into DuckDB
   - sec_filing_raw_index
   - sec_xbrl_fact_raw
4) optional SEC company tickers refresh if script exists
5) build_sec_filings
6) build_sec_fact_normalized
7) build_fundamentals

Pourquoi ce script
------------------
Le repo contient déjà les briques bulk SEC, mais pas un orchestrateur
unique, stable et verbeux pour piloter tout le flux historique.

Principes
---------
- Python mince pour l'orchestration
- SQL-first conservé dans les étapes aval
- beaucoup de commentaires pour aider les autres développeurs
- tqdm pour garder une progression propre
- logs explicites pour audit
- checks avant action
"""

import argparse
import importlib
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import duckdb

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _now_ts() -> str:
    """Retourne un timestamp UTC simple pour les métriques finales."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _bool_env(name: str, default: bool = False) -> bool:
    """Parse un booléen simple depuis une variable d'environnement."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run full SEC bulk loader + normalized facts + fundamentals."
    )

    # ------------------------------------------------------------------
    # Paths / runtime
    # ------------------------------------------------------------------
    parser.add_argument(
        "--project-root",
        default=str(PROJECT_ROOT),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=str(PROJECT_ROOT / "market.duckdb"),
        help="DuckDB path.",
    )
    parser.add_argument(
        "--data-root",
        default=str(PROJECT_ROOT / "data"),
        help="Data root used by SEC bulk provider/loader.",
    )
    parser.add_argument(
        "--temp-dir",
        default=str(PROJECT_ROOT / "tmp"),
        help="DuckDB temp directory passed to child steps that support it.",
    )
    parser.add_argument(
        "--memory-limit",
        default="24GB",
        help="DuckDB memory limit passed to child steps that support it.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=6,
        help="DuckDB threads passed to child steps that support it.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logs in this orchestrator and supported child steps.",
    )

    # ------------------------------------------------------------------
    # SEC bulk control
    # ------------------------------------------------------------------
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compliant User-Agent, ideally with contact email.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip archive download/extract and reuse files already present on disk.",
    )
    parser.add_argument(
        "--skip-raw-load",
        action="store_true",
        help="Skip raw bulk disk -> DuckDB load.",
    )
    parser.add_argument(
        "--skip-symbol-refresh",
        action="store_true",
        help="Skip optional SEC company ticker refresh + symbol reference rebuild.",
    )
    parser.add_argument(
        "--skip-build-sec-filings",
        action="store_true",
        help="Skip build_sec_filings.",
    )
    parser.add_argument(
        "--skip-build-sec-fact-normalized",
        action="store_true",
        help="Skip build_sec_fact_normalized.",
    )
    parser.add_argument(
        "--skip-build-fundamentals",
        action="store_true",
        help="Skip build_fundamentals.",
    )

    parser.add_argument(
        "--refresh-existing-archives",
        action="store_true",
        help="Force redownload of bulk submissions/companyfacts archives.",
    )
    parser.add_argument(
        "--truncate-before-load",
        action="store_true",
        help="Delete existing sec_filing_raw_index / sec_xbrl_fact_raw before raw bulk load.",
    )
    parser.add_argument(
        "--limit-ciks",
        type=int,
        default=None,
        help="Optional dry-run limit for number of CIK JSON files loaded from disk.",
    )

    # ------------------------------------------------------------------
    # Child-step compatibility toggles
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-init-sec-foundation",
        action="store_true",
        help="Skip init_sec_foundation child step.",
    )

    return parser.parse_args()


def _print_header(title: str) -> None:
    print(f"\n===== {title} =====", flush=True)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def _safe_rel(path: Path, project_root: Path) -> str:
    try:
        return str(path.resolve().relative_to(project_root.resolve()))
    except Exception:
        return str(path.resolve())


def _probe_repo_files(project_root: Path) -> dict[str, bool]:
    """
    Sonde la présence des scripts attendus avant de lancer quoi que ce soit.

    On ne veut pas "assumer" qu'un script existe; on le vérifie.
    """
    expected = {
        "cli/core/init_sec_foundation.py": (project_root / "cli/core/init_sec_foundation.py").exists(),
        "cli/core/build_sec_filings.py": (project_root / "cli/core/build_sec_filings.py").exists(),
        "cli/core/build_sec_fact_normalized.py": (project_root / "cli/core/build_sec_fact_normalized.py").exists(),
        "cli/core/build_fundamentals.py": (project_root / "cli/core/build_fundamentals.py").exists(),
        "cli/raw/fetch_sec_company_tickers_raw.py": (project_root / "cli/raw/fetch_sec_company_tickers_raw.py").exists(),
        "cli/core/build_symbol_reference.py": (project_root / "cli/core/build_symbol_reference.py").exists(),
    }
    return expected


def _assert_script_exists(project_root: Path, rel_path: str) -> Path:
    path = project_root / rel_path
    if not path.exists():
        raise FileNotFoundError(f"missing child script: {path}")
    return path


def _build_child_cmd(
    *,
    project_root: Path,
    script_rel: str,
    db_path: Path | None,
    verbose: bool,
    memory_limit: str | None = None,
    threads: int | None = None,
    temp_dir: Path | None = None,
    support_runtime_flags: bool = False,
) -> list[str]:
    """
    Construit une commande child de façon uniforme.

    support_runtime_flags=False
        -> ne passe que --db-path et --verbose
    support_runtime_flags=True
        -> passe aussi --memory-limit / --threads / --temp-dir
    """
    script_path = _assert_script_exists(project_root, script_rel)
    cmd = [sys.executable, str(script_path)]

    if db_path is not None:
        cmd.extend(["--db-path", str(db_path)])

    if support_runtime_flags:
        if memory_limit is not None:
            cmd.extend(["--memory-limit", str(memory_limit)])
        if threads is not None:
            cmd.extend(["--threads", str(int(threads))])
        if temp_dir is not None:
            cmd.extend(["--temp-dir", str(temp_dir)])

    if verbose:
        cmd.append("--verbose")

    return cmd


def _run_subprocess_step(
    *,
    name: str,
    cmd: list[str],
    cwd: Path,
) -> dict[str, Any]:
    """
    Lance une étape child en streaming stdout/stderr.

    On évite capture_output=True pour laisser tqdm et les logs vivre proprement.
    """
    _print_header(f"STEP {name}")
    print("COMMAND:", " ".join(shlex.quote(part) for part in cmd), flush=True)

    started = time.time()
    completed = subprocess.run(cmd, cwd=cwd, check=False)
    duration = round(time.time() - started, 3)

    result = {
        "step": name,
        "command": cmd,
        "returncode": int(completed.returncode),
        "duration_seconds": duration,
    }

    if completed.returncode != 0:
        raise RuntimeError(_json_dumps(result))

    print(f"[step_ok] {name} duration_seconds={duration}", flush=True)
    return result


def _import_symbol(import_path: str, symbol_name: str) -> Any:
    """
    Import dynamique d'une classe/fonction du repo.

    Permet d'utiliser les providers bulk existants sans hardcoder un autre wrapper.
    """
    module = importlib.import_module(import_path)
    return getattr(module, symbol_name)


def _run_bulk_download(
    *,
    data_root: Path,
    user_agent: str,
    refresh_existing_archives: bool,
    verbose: bool,
) -> dict[str, Any]:
    """
    Télécharge et extrait submissions + companyfacts depuis SEC.

    Cette étape utilise directement le provider bulk déjà présent dans le repo.
    """
    _print_header("STEP BULK DOWNLOAD")

    SecBulkArchiveProvider = _import_symbol(
        "stock_quant.infrastructure.providers.sec.sec_bulk_archive_provider",
        "SecBulkArchiveProvider",
    )

    provider = SecBulkArchiveProvider(
        data_root=data_root,
        user_agent=user_agent,
    )

    started = time.time()
    result = provider.download_and_extract(
        include_submissions=True,
        include_companyfacts=True,
        refresh_existing=bool(refresh_existing_archives),
    )
    duration = round(time.time() - started, 3)

    wrapped = {
        "step": "bulk_download",
        "duration_seconds": duration,
        "result": result,
    }

    if verbose:
        print(_json_dumps(wrapped), flush=True)
    else:
        print(
            "[bulk_download] "
            f"archives_requested={result.get('archives_requested')} "
            f"archives_downloaded={result.get('archives_downloaded')} "
            f"archives_extracted={result.get('archives_extracted')} "
            f"total_extracted_files={result.get('total_extracted_files')}",
            flush=True,
        )

    return wrapped


def _run_bulk_raw_load(
    *,
    db_path: Path,
    data_root: Path,
    truncate_before_load: bool,
    limit_ciks: int | None,
    verbose: bool,
) -> dict[str, Any]:
    """
    Charge les fichiers SEC bulk extraits vers les tables raw DuckDB.
    """
    _print_header("STEP BULK RAW LOAD")

    SecBulkDiskLoader = _import_symbol(
        "stock_quant.infrastructure.providers.sec.sec_bulk_disk_loader",
        "SecBulkDiskLoader",
    )

    con = duckdb.connect(str(db_path))
    try:
        loader = SecBulkDiskLoader(
            con=con,
            data_root=data_root,
        )

        started = time.time()
        metrics = loader.load_raw_from_disk(
            load_submissions=True,
            load_companyfacts=True,
            truncate_before_load=bool(truncate_before_load),
            limit_ciks=limit_ciks,
        )
        duration = round(time.time() - started, 3)

        wrapped = {
            "step": "bulk_raw_load",
            "duration_seconds": duration,
            "metrics": {
                "submission_files_seen": int(metrics.submission_files_seen),
                "submission_rows_written": int(metrics.submission_rows_written),
                "companyfacts_files_seen": int(metrics.companyfacts_files_seen),
                "xbrl_fact_rows_written": int(metrics.xbrl_fact_rows_written),
            },
        }

        if verbose:
            print(_json_dumps(wrapped), flush=True)
        else:
            print(
                "[bulk_raw_load] "
                f"submission_files_seen={metrics.submission_files_seen} "
                f"submission_rows_written={metrics.submission_rows_written} "
                f"companyfacts_files_seen={metrics.companyfacts_files_seen} "
                f"xbrl_fact_rows_written={metrics.xbrl_fact_rows_written}",
                flush=True,
            )

        return wrapped
    finally:
        con.close()


def _probe_db_tables(db_path: Path) -> dict[str, Any]:
    """
    Sonde les tables SEC principales pour donner une vue d'ensemble rapide.
    """
    con = duckdb.connect(str(db_path))
    try:
        table_names = [
            "symbol_reference",
            "sec_filing_raw_index",
            "sec_filing",
            "sec_xbrl_fact_raw",
            "sec_fact_normalized",
            "fundamental_ttm",
        ]

        presence_rows = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()
        present = {str(row[0]) for row in presence_rows}

        probe: dict[str, Any] = {
            "table_presence": {name: (name in present) for name in table_names},
            "counts": {},
        }

        if "symbol_reference" in present:
            probe["counts"]["symbol_reference"] = con.execute(
                """
                SELECT COUNT(*) AS rows, COUNT(DISTINCT cik) AS ciks
                FROM symbol_reference
                """
            ).fetchone()

        if "sec_filing_raw_index" in present:
            probe["counts"]["sec_filing_raw_index"] = con.execute(
                """
                SELECT COUNT(*) AS rows, COUNT(DISTINCT cik) AS ciks
                FROM sec_filing_raw_index
                """
            ).fetchone()

        if "sec_filing" in present:
            probe["counts"]["sec_filing"] = con.execute(
                """
                SELECT COUNT(*) AS rows, COUNT(DISTINCT cik) AS ciks
                FROM sec_filing
                """
            ).fetchone()

        if "sec_xbrl_fact_raw" in present:
            probe["counts"]["sec_xbrl_fact_raw"] = con.execute(
                """
                SELECT COUNT(*) AS rows, COUNT(DISTINCT cik) AS ciks
                FROM sec_xbrl_fact_raw
                """
            ).fetchone()

        if "sec_fact_normalized" in present:
            probe["counts"]["sec_fact_normalized"] = con.execute(
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT cik) AS ciks,
                    COUNT(DISTINCT company_id) AS company_ids
                FROM sec_fact_normalized
                """
            ).fetchone()

        if "fundamental_ttm" in present:
            probe["counts"]["fundamental_ttm"] = con.execute(
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT cik) AS ciks,
                    COUNT(DISTINCT company_id) AS company_ids
                FROM fundamental_ttm
                """
            ).fetchone()

        return probe
    finally:
        con.close()


def main() -> int:
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = Path(args.db_path).expanduser().resolve()
    data_root = Path(args.data_root).expanduser().resolve()
    temp_dir = Path(args.temp_dir).expanduser().resolve()

    temp_dir.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)

    _print_header("FULL SEC LOADER START")
    print(f"[full_sec_loader] project_root={project_root}", flush=True)
    print(f"[full_sec_loader] db_path={db_path}", flush=True)
    print(f"[full_sec_loader] data_root={data_root}", flush=True)
    print(f"[full_sec_loader] temp_dir={temp_dir}", flush=True)
    print(f"[full_sec_loader] memory_limit={args.memory_limit}", flush=True)
    print(f"[full_sec_loader] threads={args.threads}", flush=True)
    print(f"[full_sec_loader] truncate_before_load={args.truncate_before_load}", flush=True)
    print(f"[full_sec_loader] refresh_existing_archives={args.refresh_existing_archives}", flush=True)
    print(f"[full_sec_loader] limit_ciks={args.limit_ciks}", flush=True)
    print(f"[full_sec_loader] verbose={args.verbose}", flush=True)

    repo_probe = _probe_repo_files(project_root)
    print(f"[full_sec_loader] repo_probe={_json_dumps(repo_probe)}", flush=True)

    step_results: list[dict[str, Any]] = []

    planned_steps: list[str] = []
    if not args.skip_init_sec_foundation:
        planned_steps.append("init_sec_foundation")
    if not args.skip_download:
        planned_steps.append("bulk_download")
    if not args.skip_raw_load:
        planned_steps.append("bulk_raw_load")
    if not args.skip_symbol_refresh:
        if repo_probe.get("cli/raw/fetch_sec_company_tickers_raw.py", False):
            planned_steps.append("fetch_sec_company_tickers_raw")
        if repo_probe.get("cli/core/build_symbol_reference.py", False):
            planned_steps.append("build_symbol_reference")
    if not args.skip_build_sec_filings:
        planned_steps.append("build_sec_filings")
    if not args.skip_build_sec_fact_normalized:
        planned_steps.append("build_sec_fact_normalized")
    if not args.skip_build_fundamentals:
        planned_steps.append("build_fundamentals")

    for step_name in tqdm(planned_steps, desc="sec full loader", unit="step", dynamic_ncols=True):
        if step_name == "init_sec_foundation":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/core/init_sec_foundation.py",
                db_path=db_path,
                verbose=args.verbose,
                memory_limit=None,
                threads=None,
                temp_dir=None,
                support_runtime_flags=False,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        if step_name == "bulk_download":
            step_results.append(
                _run_bulk_download(
                    data_root=data_root,
                    user_agent=args.user_agent,
                    refresh_existing_archives=args.refresh_existing_archives,
                    verbose=args.verbose,
                )
            )
            continue

        if step_name == "bulk_raw_load":
            step_results.append(
                _run_bulk_raw_load(
                    db_path=db_path,
                    data_root=data_root,
                    truncate_before_load=args.truncate_before_load,
                    limit_ciks=args.limit_ciks,
                    verbose=args.verbose,
                )
            )
            continue

        if step_name == "fetch_sec_company_tickers_raw":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/raw/fetch_sec_company_tickers_raw.py",
                db_path=None,
                verbose=args.verbose,
                support_runtime_flags=False,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        if step_name == "build_symbol_reference":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/core/build_symbol_reference.py",
                db_path=db_path,
                verbose=args.verbose,
                support_runtime_flags=False,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        if step_name == "build_sec_filings":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/core/build_sec_filings.py",
                db_path=db_path,
                verbose=args.verbose,
                support_runtime_flags=False,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        if step_name == "build_sec_fact_normalized":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/core/build_sec_fact_normalized.py",
                db_path=db_path,
                verbose=args.verbose,
                memory_limit=args.memory_limit,
                threads=args.threads,
                temp_dir=temp_dir,
                support_runtime_flags=True,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        if step_name == "build_fundamentals":
            cmd = _build_child_cmd(
                project_root=project_root,
                script_rel="cli/core/build_fundamentals.py",
                db_path=db_path,
                verbose=args.verbose,
                memory_limit=args.memory_limit,
                threads=args.threads,
                temp_dir=temp_dir,
                support_runtime_flags=True,
            )
            step_results.append(
                _run_subprocess_step(name=step_name, cmd=cmd, cwd=project_root)
            )
            continue

        raise RuntimeError(f"unknown planned step: {step_name}")

    db_probe = _probe_db_tables(db_path)

    final_payload = {
        "status": "SUCCESS",
        "finished_at": _now_ts(),
        "project_root": str(project_root),
        "db_path": str(db_path),
        "data_root": str(data_root),
        "temp_dir": str(temp_dir),
        "planned_steps": planned_steps,
        "executed_steps": step_results,
        "db_probe": db_probe,
    }

    _print_header("FULL SEC LOADER DONE")
    print(_json_dumps(final_payload), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
