from __future__ import annotations


class FundamentalsService:

    """
    Service responsable de construire les snapshots fondamentaux.

    Règle critique :

    Toute disponibilité des données doit être filtrée par
    available_at afin d'éviter le look-ahead bias.
    """

    def __init__(self, repository):

        self.repository = repository


    def load_sec_facts(self):

        return self.repository.load_sec_fact_normalized_rows()


    def build_snapshots(self):

        facts = self.load_sec_facts()

        snapshots = []

        for row in facts:

            snapshots.append(
                {
                    "company_id": row["company_id"],
                    "cik": row["cik"],
                    "period_end_date": row["period_end_date"],
                    "available_at": row["available_at"],
                    "concept": row["concept"],
                    "value": row["value"],
                }
            )

        return self.repository.insert_snapshots(snapshots)


    def build_features(self):

        return self.repository.build_daily_features()
