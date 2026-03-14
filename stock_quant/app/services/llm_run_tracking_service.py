from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class LlmRunTrackingService:
    def build_llm_run_payload(
        self,
        run_name: str,
        model_name: str,
        prompt_version: str,
        input_table: str,
        output_table: str,
        row_count: int,
        status: str = "SUCCESS",
    ) -> dict[str, Any]:
        return {
            "run_name": run_name,
            "model_name": model_name,
            "prompt_version": prompt_version,
            "status": status,
            "input_table": input_table,
            "output_table": output_table,
            "started_at": datetime.utcnow(),
            "finished_at": datetime.utcnow(),
            "metrics_json": json.dumps({"row_count": row_count}, sort_keys=True),
            "config_json": json.dumps({}, sort_keys=True),
            "error_message": None,
            "created_at": datetime.utcnow(),
        }
