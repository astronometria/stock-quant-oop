from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class ResearchStep:
    name: str
    command: list[str]


class ResearchPipelineOrchestrator:
    def __init__(self, project_root: Path, db_path: str) -> None:
        self.project_root = Path(project_root)
        self.db_path = str(db_path)

    def build_steps(
        self,
        dataset_name: str,
        dataset_version: str,
        experiment_name: str,
        backtest_name: str,
        verbose: bool = False,
    ) -> list[ResearchStep]:
        python = str(sys.executable)

        steps = [
            ResearchStep(
                name="build_feature_engine",
                command=[
                    python,
                    str(self.project_root / "cli" / "core" / "build_feature_engine.py"),
                    "--db-path",
                    self.db_path,
                ] + (["--verbose"] if verbose else []),
            ),
            ResearchStep(
                name="build_label_engine",
                command=[
                    python,
                    str(self.project_root / "cli" / "core" / "build_label_engine.py"),
                    "--db-path",
                    self.db_path,
                ] + (["--verbose"] if verbose else []),
            ),
            ResearchStep(
                name="build_dataset_builder",
                command=[
                    python,
                    str(self.project_root / "cli" / "core" / "build_dataset_builder.py"),
                    "--db-path",
                    self.db_path,
                    "--dataset-name",
                    str(dataset_name),
                    "--dataset-version",
                    str(dataset_version),
                ] + (["--verbose"] if verbose else []),
            ),
            ResearchStep(
                name="build_backtest",
                command=[
                    python,
                    str(self.project_root / "cli" / "core" / "build_backtest.py"),
                    "--db-path",
                    self.db_path,
                    "--dataset-name",
                    str(dataset_name),
                    "--dataset-version",
                    str(dataset_version),
                    "--experiment-name",
                    str(experiment_name),
                    "--backtest-name",
                    str(backtest_name),
                ] + (["--verbose"] if verbose else []),
            ),
        ]
        return steps

    def run(
        self,
        dataset_name: str,
        dataset_version: str,
        experiment_name: str,
        backtest_name: str,
        verbose: bool = False,
    ) -> int:
        steps = self.build_steps(
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            experiment_name=experiment_name,
            backtest_name=backtest_name,
            verbose=verbose,
        )

        for step in steps:
            command = [str(part) for part in step.command]
            print(f"===== RUN {step.name} =====")
            print(" ".join(command))
            completed = subprocess.run(command)
            if completed.returncode != 0:
                print(f"FAILED at step: {step.name} (exit={completed.returncode})")
                return completed.returncode

        print("===== RESEARCH PIPELINE DONE =====")
        return 0
