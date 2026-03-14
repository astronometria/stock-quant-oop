from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def build_run_summary(
    pipeline_name: str,
    status: str,
    metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "pipeline_name": pipeline_name,
        "status": status,
        "metrics": dict(metrics or {}),
    }
