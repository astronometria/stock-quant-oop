from __future__ import annotations

from stock_quant.domain.entities.universe import UniverseEntry


class UniverseConflictResolutionPolicy:
    _EXCHANGE_PRIORITY = {
        "NASDAQ": 100,
        "NYSE": 95,
        "NYSE_ARCA": 85,
        "NYSE_AMERICAN": 80,
        "BATS": 70,
        "IEX": 60,
        "OTC": 1,
    }

    def score(self, entry: UniverseEntry) -> tuple[int, int, int, int, int]:
        include_score = 1 if entry.include_in_universe else 0
        exchange_score = self._EXCHANGE_PRIORITY.get(entry.exchange_normalized or "", 0)
        cik_score = 1 if entry.cik else 0
        company_score = 1 if entry.company_name else 0
        adr_penalty = 0 if entry.is_adr else 1
        return (include_score, exchange_score, cik_score, company_score, adr_penalty)

    def choose_best(self, entries: list[UniverseEntry]) -> UniverseEntry:
        if not entries:
            raise ValueError("cannot choose best entry from empty list")
        return max(entries, key=self.score)
