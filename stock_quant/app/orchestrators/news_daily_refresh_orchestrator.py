from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class NewsDailyRefreshOrchestrator:
    def run(self) -> None:
        raise NotImplementedError("NewsDailyRefreshOrchestrator sera branché à la passe 5.")
