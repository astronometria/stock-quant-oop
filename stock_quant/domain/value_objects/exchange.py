from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Exchange:
    raw_value: str | None
    normalized_value: str | None = None
