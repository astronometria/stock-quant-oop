from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class PipelineRunService:
    def build_started_payload(
        self,
        pipeline_name: str,
        config: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = datetime.utcnow()
        return {
            "pipeline_name": pipeline_name,
            "status": "RUNNING",
            "started_at": now,
            "finished_at": None,
            "rows_read": 0,
            "rows_written": 0,
            "metrics_json": json.dumps({}, sort_keys=True),
            "config_json": json.dumps(config or {}, sort_keys=True),
            "error_message": None,
            "created_at": now,
        }

    def build_finished_payload(
        self,
        pipeline_name: str,
        status: str,
        rows_read: int,
        rows_written: int,
        metrics: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        now = datetime.utcnow()
        return {
            "pipeline_name": pipeline_name,
            "status": status,
            "started_at": now,
            "finished_at": now,
            "rows_read": int(rows_read),
            "rows_written": int(rows_written),
            "metrics_json": json.dumps(metrics or {}, sort_keys=True, default=str),
            "config_json": json.dumps(config or {}, sort_keys=True, default=str),
            "error_message": error_message,
            "created_at": now,
        }
