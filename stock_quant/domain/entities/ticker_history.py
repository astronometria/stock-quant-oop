from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class TickerHistoryEntry:
    instrument_id: str
    symbol: str
    exchange: str | None
    valid_from: date | None
    valid_to: date | None = None
    is_current: bool = True
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class IdentifierMapEntry:
    instrument_id: str
    company_id: str
    identifier_type: str
    identifier_value: str
    is_primary: bool = False
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
