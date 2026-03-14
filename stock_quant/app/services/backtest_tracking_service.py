from __future__ import annotations

import json
from datetime import datetime
from typing import Any


class BacktestTrackingService:
    def build_experiment_run_payload(
        self,
        experiment_name: str,
        dataset_version_id: int | None,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "experiment_name": experiment_name,
            "status": "SUCCESS",
            "dataset_version_id": dataset_version_id,
            "started_at": datetime.utcnow(),
            "finished_at": datetime.utcnow(),
            "metrics_json": json.dumps(metrics, sort_keys=True),
            "config_json": json.dumps({}, sort_keys=True),
            "error_message": None,
            "created_at": datetime.utcnow(),
        }

    def build_backtest_run_payload(
        self,
        backtest_name: str,
        dataset_version_id: int | None,
        summary_metrics: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "backtest_name": backtest_name,
            "status": "SUCCESS",
            "dataset_version_id": dataset_version_id,
            "started_at": datetime.utcnow(),
            "finished_at": datetime.utcnow(),
            "metrics_json": json.dumps(summary_metrics, sort_keys=True),
            "config_json": json.dumps({}, sort_keys=True),
            "error_message": None,
            "created_at": datetime.utcnow(),
        }
