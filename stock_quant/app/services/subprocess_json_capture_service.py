from __future__ import annotations

import json
from typing import Any


class SubprocessJsonCaptureService:
    def extract_last_json_object(self, text: str) -> dict[str, Any] | None:
        decoder = json.JSONDecoder()
        candidate_positions = [idx for idx, ch in enumerate(text) if ch == "{"]

        for start in reversed(candidate_positions):
            snippet = text[start:].strip()
            try:
                obj, end = decoder.raw_decode(snippet)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
        return None
