from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class RawUniverseCandidate:
    symbol: str | None
    company_name: str | None
    cik: str | None
    exchange_raw: str | None
    security_type_raw: str | None
    source_name: str
    as_of_date: date | None = None


@dataclass(slots=True)
class UniverseEntry:
    symbol: str
    company_name: str | None
    cik: str | None
    exchange_raw: str | None
    exchange_normalized: str | None
    security_type: str
    include_in_universe: bool
    exclusion_reason: str | None
    is_common_stock: bool
    is_adr: bool
    is_etf: bool
    is_preferred: bool
    is_warrant: bool
    is_right: bool
    is_unit: bool
    source_name: str
    as_of_date: date | None
    created_at: datetime


@dataclass(slots=True)
class UniverseConflict:
    symbol: str
    chosen_source: str
    rejected_source: str
    reason: str
    payload_json: str
    created_at: datetime
