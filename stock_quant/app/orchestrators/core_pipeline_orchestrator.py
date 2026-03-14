from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class CorePipelineOrchestrator:
    def run(self) -> None:
        raise NotImplementedError("CorePipelineOrchestrator sera branché à la passe 5.")
