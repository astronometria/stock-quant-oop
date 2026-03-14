#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from stock_quant.app.services.pipeline_run_service import PipelineRunService
from stock_quant.app.services.subprocess_json_capture_service import SubprocessJsonCaptureService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.research_schema import ResearchSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_pipeline_run_repository import DuckDbPipelineRunRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full daily research pipeline with pipeline_runs logging.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", default="research_dataset_v1")
    parser.add_argument("--dataset-version", default="v1")
    parser.add_argument("--experiment-name", default="momentum_experiment_v1")
    parser.add_argument("--backtest-name", default="momentum_backtest_v1")
    parser.add_argument("--llm-run-name", default="news_llm_enrichment_v1")
    parser.add_argument("--model-name", default="placeholder")
    parser.add_argument("--prompt-version", default="v1")
    parser.add_argument("--skip-cleanup", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def run_and_log_step(
    project_root: Path,
    db_path: str,
    command: list[str],
    pipeline_name: str,
    config_payload: dict,
) -> int:
    capture = SubprocessJsonCaptureService()
    payload_builder = PipelineRunService()

    completed = subprocess.run(command, text=True, capture_output=True)
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""

    if stdout:
        print(stdout, end="")
    if stderr:
        print(stderr, end="", file=sys.stderr)

    result_json = capture.extract_last_json_object(stdout)
    if result_json is None:
        result_json = {}

    status = "SUCCESS" if completed.returncode == 0 else "FAILED"
    rows_read = int(result_json.get("rows_read", 0) or 0)
    rows_written = int(result_json.get("rows_written", 0) or 0)
    metrics = result_json.get("metrics", {}) or {}
    error_message = result_json.get("error_message")

    session_factory = DuckDbSessionFactory(db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchSchemaManager(uow).initialize()
        repo = DuckDbPipelineRunRepository(uow)
        payload = payload_builder.build_finished_payload(
            pipeline_name=pipeline_name,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            metrics=metrics,
            config=config_payload,
            error_message=error_message,
        )
        repo.insert_pipeline_run(payload)

    return completed.returncode


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    project_root = Path(config.project_root)
    db_path = config.db_path
    python = sys.executable

    if not args.skip_cleanup:
        cleanup_cmd = [
            python,
            str(project_root / "cli" / "core" / "cleanup_research_runs.py"),
            "--db-path", db_path,
            "--dataset-name", args.dataset_name,
            "--dataset-version", args.dataset_version,
            "--experiment-name", args.experiment_name,
            "--backtest-name", args.backtest_name,
            "--llm-run-name", args.llm_run_name,
        ]
        if args.verbose:
            print("===== RUN cleanup_research_runs =====")
            print(" ".join(cleanup_cmd))
        cleanup_rc = subprocess.run(cleanup_cmd).returncode
        if cleanup_rc != 0:
            return cleanup_rc

    llm_cmd = [
        python,
        str(project_root / "cli" / "core" / "register_llm_run.py"),
        "--db-path", db_path,
        "--run-name", args.llm_run_name,
        "--model-name", args.model_name,
        "--prompt-version", args.prompt_version,
    ]
    if args.verbose:
        llm_cmd.append("--verbose")
        print("===== RUN register_llm_run =====")
        print(" ".join(llm_cmd))
    llm_rc = subprocess.run(llm_cmd).returncode
    if llm_rc != 0:
        return llm_rc

    steps = [
        (
            "build_feature_engine",
            [
                python,
                str(project_root / "cli" / "core" / "build_feature_engine.py"),
                "--db-path", db_path,
            ] + (["--verbose"] if args.verbose else []),
            {"mode": "daily"},
        ),
        (
            "build_label_engine",
            [
                python,
                str(project_root / "cli" / "core" / "build_label_engine.py"),
                "--db-path", db_path,
            ] + (["--verbose"] if args.verbose else []),
            {"mode": "daily"},
        ),
        (
            "build_dataset_builder",
            [
                python,
                str(project_root / "cli" / "core" / "build_dataset_builder.py"),
                "--db-path", db_path,
                "--dataset-name", args.dataset_name,
                "--dataset-version", args.dataset_version,
            ] + (["--verbose"] if args.verbose else []),
            {"mode": "daily", "dataset_name": args.dataset_name, "dataset_version": args.dataset_version},
        ),
        (
            "build_backtest",
            [
                python,
                str(project_root / "cli" / "core" / "build_backtest.py"),
                "--db-path", db_path,
                "--dataset-name", args.dataset_name,
                "--dataset-version", args.dataset_version,
                "--experiment-name", args.experiment_name,
                "--backtest-name", args.backtest_name,
            ] + (["--verbose"] if args.verbose else []),
            {
                "mode": "daily",
                "dataset_name": args.dataset_name,
                "dataset_version": args.dataset_version,
                "experiment_name": args.experiment_name,
                "backtest_name": args.backtest_name,
            },
        ),
    ]

    for pipeline_name, command, config_payload in steps:
        print(f"===== RUN {pipeline_name} =====")
        print(" ".join(command))
        rc = run_and_log_step(
            project_root=project_root,
            db_path=db_path,
            command=command,
            pipeline_name=pipeline_name,
            config_payload=config_payload,
        )
        if rc != 0:
            print(f"FAILED at step: {pipeline_name} (exit={rc})")
            return rc

    print("===== DAILY RESEARCH PIPELINE DONE =====")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
