from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import subprocess
import sys

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


@dataclass(slots=True)
class CorePipelineStep:
    name: str
    relative_script: str
    extra_args: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CorePipelineOrchestrator:
    project_root: Path
    db_path: str | None = None
    verbose: bool = False

    def script_exists(self, relative_script: str) -> bool:
        return (self.project_root / relative_script).is_file()

    def build_command(self, relative_script: str, extra_args: list[str] | None = None) -> list[str]:
        cmd = [sys.executable, str(self.project_root / relative_script)]
        if self.db_path:
            cmd.extend(["--db-path", self.db_path])
        if self.verbose:
            cmd.append("--verbose")
        if extra_args:
            cmd.extend(extra_args)
        return cmd

    def run_steps(self, steps: list[CorePipelineStep]) -> int:
        if not steps:
            print("No pipeline steps selected.")
            return 0

        for step in tqdm(steps, desc="core-pipeline", unit="step"):
            cmd = self.build_command(
                relative_script=step.relative_script,
                extra_args=step.extra_args,
            )
            print(f"\n===== RUN {step.name} =====")
            print(" ".join(cmd))
            completed = subprocess.run(cmd, cwd=self.project_root)
            if completed.returncode != 0:
                print(f"\nFAILED at step: {step.name} (exit={completed.returncode})")
                return int(completed.returncode)

        print("\n===== CORE PIPELINE DONE =====")
        return 0
