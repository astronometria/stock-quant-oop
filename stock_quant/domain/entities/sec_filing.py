from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class SecFilingRawIndexEntry:
    cik: str
    company_name: str | None
    form_type: str
    filing_date: date | None
    accepted_at: datetime | None
    accession_number: str
    primary_document: str | None
    filing_url: str | None
    source_name: str | None = "sec"
    ingested_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecFiling:
    filing_id: str
    company_id: str | None
    cik: str
    form_type: str
    filing_date: date | None
    accepted_at: datetime | None
    accession_number: str
    filing_url: str | None
    primary_document: str | None
    available_at: datetime | None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecFilingDocument:
    filing_id: str
    document_type: str
    document_url: str | None
    document_text: str | None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecFactNormalized:
    filing_id: str
    company_id: str | None
    cik: str
    taxonomy: str | None
    concept: str
    period_end_date: date | None
    unit: str | None
    value_text: str | None
    value_numeric: float | None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
