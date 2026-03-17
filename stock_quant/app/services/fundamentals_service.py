from __future__ import annotations

from typing import Any


class FundamentalsService:
    """
    Canonical fundamentals service.

    Design rules
    ------------
    - sec_fact_normalized is the only normalized upstream source
    - market visibility must use available_at
    - no fallback to period_end_date for PIT visibility
    """

    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def load_sec_facts(self) -> list[dict[str, Any]]:
        return self.repository.load_sec_fact_normalized_rows()

    def build_snapshots(self) -> int:
        rows = self.load_sec_facts()
        if not rows:
            return 0
        return self.repository.insert_snapshots(rows)

    def build_features(self) -> int:
        return self.repository.build_daily_features()
