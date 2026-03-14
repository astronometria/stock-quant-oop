from __future__ import annotations

import json
from typing import Any


class SubprocessJsonCaptureService:
    def extract_last_json_object(self, text: str) -> dict[str, Any] | None:
        lines = text.splitlines()

        # Essaye d'abord de parser des suffixes multi-lignes en partant de la fin.
        for start in range(len(lines)):
            idx = len(lines) - 1 - start
            candidate = "\n".join(lines[idx:]).strip()
            if not candidate:
                continue
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict) and "pipeline_name" in obj and "status" in obj:
                    return obj
            except Exception:
                pass

        # Fallback plus permissif: cherche un objet JSON complet par équilibrage d'accolades.
        starts = [i for i, ch in enumerate(text) if ch == "{"]
        for start in starts:
            depth = 0
            in_string = False
            escape = False

            for end in range(start, len(text)):
                ch = text[end]

                if in_string:
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_string = False
                    continue

                if ch == '"':
                    in_string = True
                elif ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = text[start:end + 1].strip()
                        try:
                            obj = json.loads(candidate)
                            if isinstance(obj, dict) and "pipeline_name" in obj and "status" in obj:
                                return obj
                        except Exception:
                            pass
                        break

        return None
