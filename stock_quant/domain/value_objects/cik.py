from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Cik:
    value: str

    @classmethod
    def from_raw(cls, raw: str | None) -> "Cik | None":
        if raw is None:
            return None
        value = "".join(ch for ch in raw.strip() if ch.isdigit())
        if not value:
            return None
        return cls(value=value.zfill(10))
