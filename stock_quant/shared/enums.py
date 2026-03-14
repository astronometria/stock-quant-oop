from __future__ import annotations

from enum import Enum


class PipelineStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class SourceMarket(str, Enum):
    REGULAR = "regular"
    OTC = "otc"
    BOTH = "both"


class RunMode(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
