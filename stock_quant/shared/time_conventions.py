from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any


TIME_FIELD_DESCRIPTIONS: dict[str, str] = {
    "event_date": "Date économique réelle de l'événement.",
    "period_end_date": "Date de fin de période comptable ou analytique.",
    "published_at": "Timestamp de publication par la source.",
    "accepted_at": "Timestamp d'acceptation officielle, par exemple SEC.",
    "effective_at": "Timestamp à partir duquel l'information devient valable.",
    "available_at": "Timestamp à partir duquel la donnée est exploitable dans le moteur.",
    "ingested_at": "Timestamp d'ingestion dans le système.",
    "as_of_date": "Date de connaissance simulée pour la recherche point-in-time.",
}


@dataclass(frozen=True, slots=True)
class TimeContext:
    event_date: date | None = None
    period_end_date: date | None = None
    published_at: datetime | None = None
    accepted_at: datetime | None = None
    effective_at: datetime | None = None
    available_at: datetime | None = None
    ingested_at: datetime | None = None
    as_of_date: date | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_date": self.event_date,
            "period_end_date": self.period_end_date,
            "published_at": self.published_at,
            "accepted_at": self.accepted_at,
            "effective_at": self.effective_at,
            "available_at": self.available_at,
            "ingested_at": self.ingested_at,
            "as_of_date": self.as_of_date,
        }


def time_field_catalog() -> list[dict[str, str]]:
    return [
        {"field_name": key, "description": value}
        for key, value in TIME_FIELD_DESCRIPTIONS.items()
    ]
