from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PriceDailyRefreshOrchestrator:
    def run(self) -> None:
        raise NotImplementedError("PriceDailyRefreshOrchestrator sera branché à la passe 5.")
