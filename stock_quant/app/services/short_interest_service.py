from __future__ import annotations


class ShortInterestService:

    def __init__(self, repository):

        self.repository = repository


    def load_raw(self):

        rows = self.repository.load_raw()

        return len(rows)


    def build_history(self):

        raw = self.repository.load_raw()

        normalized = []

        for r in raw:

            normalized.append(
                {
                    "symbol": r["symbol"],
                    "as_of_date": r["as_of_date"],
                    "short_interest": r["short_interest"],
                    "short_volume": r.get("short_volume"),
                    "available_at": r["available_at"],
                }
            )

        return self.repository.insert_history(normalized)


    def refresh_latest(self):

        return self.repository.rebuild_latest()
