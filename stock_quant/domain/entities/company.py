from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class CompanyMaster:
    company_id: str
    company_name: str | None
    cik: str | None
    issuer_name_normalized: str | None
    country_code: str | None = "US"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
