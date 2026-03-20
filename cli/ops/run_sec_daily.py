#!/usr/bin/env python3
from __future__ import annotations

"""
SEC daily orchestrator.

Objectif
--------
Orchestrer proprement la chaîne SEC/fundamentaux canonique du repo :

1) load_sec_filing_raw_index            (optionnel)
2) build_sec_filings
3) build_sec_fact_normalized
4) build_fundamentals
5) pipeline_status                      (optionnel, simple probe final)

Pourquoi ce script existe
-------------------------
Le repo contient maintenant des builders SQL-first robustes dans `cli/core`,
et la logique d'orchestration doit rester mince, lisible, homogène avec les
autres wrappers `cli/ops/run_*`.

Principes
---------
- Python mince pour l'orchestration
- propagation cohérente des flags runtime quand les scripts enfants les supportent
- beaucoup de commentaires pour aider les autres développeurs
- tqdm pour une progression propre
- arrêt immédiat si une étape échoue
- pas de logique métier SEC dupliquée ici
"""

import argparse
import json
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable


# -----------------------------------------------------------------------------
# Constantes de repo
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]


# -----------------------------------------------------------------------------
# Helpers généraux
# -----------------------------------------------------------------------------

def _now_ts() -> str:
    """Retourne un timestamp UTC simple pour les résumés de log."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _json_default(value: Any) -> Any:
    """Serializer JSON minimaliste pour Path et autres objets non natifs."""
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _script_help_text(script_path: Path, cwd: Path) -> str:
    """
    Lit le help d'un script enfant.

    Pourquoi on fait ça:
    - certains scripts supportent `--memory-limit`, `--threads`, `--temp-dir`,
      `--verbose`
    - d'autres non
    - on veut rester homogène sans casser les appels
    """
    result = subprocess.run(
        [sys.executable, str(script_path), "-h"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    return (result.stdout or "") + "\n" + (result.stderr or "")


def _supports_flag(script_path: Path, cwd: Path, flag_name: str) -> bool:
    """Détecte si un script enfant expose un flag donné dans son aide."""
    help_text = _script_help_text(script_path, cwd)
    return flag_name in help_text


def _build_base_command(
    project_root: Path,
    script_rel: str,
    db_path: str | None,
    verbose: bool,
    memory_limit: str | None,
    threads: int | None,
    temp_dir: str | None,
) -> tuple[list[str], dict[str, bool]]:
    """
    Construit une commande enfant en propagant seulement les flags supportés.

    Retourne:
    - la commande
    - un petit dict de capacités observées, utile pour debug/audit
    """
    script_path = project_root / script_rel
    supported = {
        "verbose": False,
        "memory_limit": False,
        "threads": False,
        "temp_dir": False,
        "db_path": True,  # convention repo, supposé supporté pour les scripts visés
    }

    cmd: list[str] = [sys.executable, str(script_path)]

    if db_path:
        cmd.extend(["--db-path", str(db_path)])

    # On sonde les flags avant de les propager.
    # Cela évite de re-casser les wrappers si un enfant diverge.
    supported["verbose"] = _supports_flag(script_path, project_root, "--verbose")
    supported["memory_limit"] = _supports_flag(script_path, project_root, "--memory-limit")
    supported["threads"] = _supports_flag(script_path, project_root, "--threads")
    supported["temp_dir"] = _supports_flag(script_path, project_root, "--temp-dir")

    if verbose and supported["verbose"]:
        cmd.append("--verbose")

    if memory_limit and supported["memory_limit"]:
        cmd.extend(["--memory-limit", str(memory_limit)])

    if threads is not None and supported["threads"]:
        cmd.extend(["--threads", str(threads)])

    if temp_dir and supported["temp_dir"]:
        cmd.extend(["--temp-dir", str(temp_dir)])

    return cmd, supported


def _run_step(step_name: str, cmd: list[str], cwd: Path) -> dict[str, Any]:
    """
    Exécute une étape enfant.

    On ne capture pas stdout/stderr ici pour laisser tqdm et les logs temps réel
    s'afficher proprement dans le terminal et dans `tee`.
    """
    started_at = _now_ts()
    start = time.perf_counter()

    print(f"\n===== RUN STEP: {step_name} =====", flush=True)
    print("COMMAND:", " ".join(shlex.quote(part) for part in cmd), flush=True)

    completed = subprocess.run(cmd, cwd=str(cwd), check=False)

    duration_seconds = round(time.perf_counter() - start, 6)
    finished_at = _now_ts()

    result = {
        "step_name": step_name,
        "command": cmd,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration_seconds,
        "returncode": int(completed.returncode),
    }

    if completed.returncode != 0:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "step": step_name,
                    "returncode": int(completed.returncode),
                    "duration_seconds": duration_seconds,
                },
                indent=2,
                default=_json_default,
                sort_keys=True,
            ),
            flush=True,
        )
        raise SystemExit(f"Step failed: {step_name} (exit={completed.returncode})")

    print(f"===== STEP OK: {step_name} =====", flush=True)
    return result


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run canonical SEC daily flow: raw index load -> filings -> normalized facts -> fundamentals."
    )

    # -------------------------------------------------------------------------
    # Paths
    # -------------------------------------------------------------------------
    parser.add_argument(
        "--project-root",
        default=str(PROJECT_ROOT),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to child steps.",
    )
    parser.add_argument(
        "--temp-dir",
        default=str(PROJECT_ROOT / "tmp"),
        help="DuckDB temp directory propagated to child steps when supported.",
    )

    # -------------------------------------------------------------------------
    # Runtime
    # -------------------------------------------------------------------------
    parser.add_argument(
        "--memory-limit",
        default="24GB",
        help="DuckDB memory limit propagated to child steps when supported.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=6,
        help="DuckDB thread count propagated to child steps when supported.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose child command output when supported.",
    )

    # -------------------------------------------------------------------------
    # Raw SEC sources
    # -------------------------------------------------------------------------
    parser.add_argument(
        "--sec-source",
        action="append",
        dest="sec_sources",
        default=[],
        help="SEC raw index CSV source path. Repeat this flag for multiple files.",
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip raw SEC filing index load and rebuild from already-loaded raw tables.",
    )

    # -------------------------------------------------------------------------
    # Optional tail probe
    # -------------------------------------------------------------------------
    parser.add_argument(
        "--skip-status",
        action="store_true",
        help="Skip final pipeline_status step.",
    )

    return parser.parse_args()


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    db_path = str(Path(args.db_path).expanduser().resolve()) if args.db_path else None
    temp_dir = str(Path(args.temp_dir).expanduser().resolve()) if args.temp_dir else None

    steps: list[tuple[str, list[str]]] = []
    capability_probe: dict[str, dict[str, bool]] = {}

    # -------------------------------------------------------------------------
    # Step 0 (optionnel): raw filing index load
    # -------------------------------------------------------------------------
    if not args.skip_load:
        load_cmd, load_caps = _build_base_command(
            project_root=project_root,
            script_rel="cli/raw/load_sec_filing_raw_index.py",
            db_path=db_path,
            verbose=args.verbose,
            memory_limit=args.memory_limit,
            threads=args.threads,
            temp_dir=temp_dir,
        )
        capability_probe["load_sec_filing_raw_index"] = load_caps

        for src in args.sec_sources:
            load_cmd.extend(["--source", str(Path(src).expanduser().resolve())])

        steps.append(("load_sec_filing_raw_index", load_cmd))

    # -------------------------------------------------------------------------
    # Step 1: build_sec_filings
    # -------------------------------------------------------------------------
    build_sec_cmd, sec_caps = _build_base_command(
        project_root=project_root,
        script_rel="cli/core/build_sec_filings.py",
        db_path=db_path,
        verbose=args.verbose,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_dir=temp_dir,
    )
    capability_probe["build_sec_filings"] = sec_caps
    steps.append(("build_sec_filings", build_sec_cmd))

    # -------------------------------------------------------------------------
    # Step 2: build_sec_fact_normalized
    # -------------------------------------------------------------------------
    build_norm_cmd, norm_caps = _build_base_command(
        project_root=project_root,
        script_rel="cli/core/build_sec_fact_normalized.py",
        db_path=db_path,
        verbose=args.verbose,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_dir=temp_dir,
    )
    capability_probe["build_sec_fact_normalized"] = norm_caps
    steps.append(("build_sec_fact_normalized", build_norm_cmd))

    # -------------------------------------------------------------------------
    # Step 3: build_fundamentals
    # -------------------------------------------------------------------------
    build_fund_cmd, fund_caps = _build_base_command(
        project_root=project_root,
        script_rel="cli/core/build_fundamentals.py",
        db_path=db_path,
        verbose=args.verbose,
        memory_limit=args.memory_limit,
        threads=args.threads,
        temp_dir=temp_dir,
    )
    capability_probe["build_fundamentals"] = fund_caps
    steps.append(("build_fundamentals", build_fund_cmd))

    # -------------------------------------------------------------------------
    # Step 4 (optionnel): pipeline_status
    # -------------------------------------------------------------------------
    if not args.skip_status:
        status_cmd, status_caps = _build_base_command(
            project_root=project_root,
            script_rel="cli/pipeline_status.py",
            db_path=db_path,
            verbose=False,  # en général inutile pour ce probe
            memory_limit=args.memory_limit,
            threads=args.threads,
            temp_dir=temp_dir,
        )
        capability_probe["pipeline_status"] = status_caps
        steps.append(("pipeline_status", status_cmd))

    # -------------------------------------------------------------------------
    # Header de run
    # -------------------------------------------------------------------------
    print("===== RUN SEC DAILY START =====", flush=True)
    print(f"PROJECT ROOT: {project_root}", flush=True)
    print(f"DB PATH: {db_path or '(default)'}", flush=True)
    print(f"TEMP DIR: {temp_dir}", flush=True)
    print(f"MEMORY LIMIT: {args.memory_limit}", flush=True)
    print(f"THREADS: {args.threads}", flush=True)
    print(f"VERBOSE: {args.verbose}", flush=True)
    print(f"SEC SOURCES: {len(args.sec_sources)}", flush=True)
    print(f"SKIP LOAD: {args.skip_load}", flush=True)
    print(f"SKIP STATUS: {args.skip_status}", flush=True)
    print(
        "CAPABILITY PROBE: "
        + json.dumps(capability_probe, indent=2, sort_keys=True, default=_json_default),
        flush=True,
    )

    # -------------------------------------------------------------------------
    # Exécution
    # -------------------------------------------------------------------------
    started_at = _now_ts()
    step_results: list[dict[str, Any]] = []

    for step_name, step_cmd in tqdm(
        steps,
        desc="sec daily pipeline",
        unit="step",
        dynamic_ncols=True,
    ):
        step_result = _run_step(step_name, step_cmd, project_root)
        step_results.append(step_result)

    finished_at = _now_ts()

    summary = {
        "status": "SUCCESS",
        "pipeline": "run_sec_daily",
        "started_at": started_at,
        "finished_at": finished_at,
        "project_root": project_root,
        "db_path": db_path,
        "temp_dir": temp_dir,
        "memory_limit": args.memory_limit,
        "threads": args.threads,
        "verbose": args.verbose,
        "skip_load": args.skip_load,
        "skip_status": args.skip_status,
        "sec_sources_count": len(args.sec_sources),
        "steps": step_results,
    }

    print("\n===== RUN SEC DAILY DONE =====", flush=True)
    print(json.dumps(summary, indent=2, sort_keys=True, default=_json_default), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
