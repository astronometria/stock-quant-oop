#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import duckdb
import requests
from tqdm import tqdm


def parse_args():
    p = argparse.ArgumentParser(description="Run vLLM enrichment on news.")
    p.add_argument("--db-path", required=True)
    p.add_argument("--vllm-url", default="http://127.0.0.1:8000/v1/chat/completions")
    p.add_argument("--model", default="Qwen/Qwen2.5-7B-Instruct")
    p.add_argument("--prompt-version", default="v1")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--debug-dir", default="~/stock-quant-oop/logs/llm_debug")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


PROMPT = """You are a financial analyst.

Return exactly one JSON object.
Do not use markdown.
Do not use code fences.
Do not add commentary before or after the JSON.

Required JSON schema:
{{
  "sentiment_label": "positive|neutral|negative",
  "sentiment_score": 0.0,
  "relevance_score": 0.0,
  "event_type": "string",
  "materiality_score": 0.0,
  "horizon_label": "intraday|short|medium|long",
  "summary": "string",
  "confidence": 0.0
}}

Article title:
{title}
"""


def strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z0-9_+-]*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def extract_first_json_object(text: str) -> str:
    text = strip_code_fences(text)

    start = text.find("{")
    if start == -1:
        raise ValueError("no JSON object start found")

    depth = 0
    in_string = False
    escape = False

    for i in range(start, len(text)):
        ch = text[i]

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
                return text[start:i + 1]

    raise ValueError("no complete JSON object found")


def normalize_output(out: dict) -> dict:
    def fnum(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return float(default)

    sentiment_label = str(out.get("sentiment_label", "neutral")).strip().lower()
    if sentiment_label not in {"positive", "neutral", "negative"}:
        sentiment_label = "neutral"

    horizon_label = str(out.get("horizon_label", "medium")).strip().lower()
    if horizon_label not in {"intraday", "short", "medium", "long"}:
        horizon_label = "medium"

    return {
        "sentiment_label": sentiment_label,
        "sentiment_score": max(-1.0, min(1.0, fnum(out.get("sentiment_score", 0.0), 0.0))),
        "relevance_score": max(0.0, min(1.0, fnum(out.get("relevance_score", 0.0), 0.0))),
        "event_type": str(out.get("event_type", "unknown")).strip() or "unknown",
        "materiality_score": max(0.0, min(1.0, fnum(out.get("materiality_score", 0.0), 0.0))),
        "horizon_label": horizon_label,
        "summary": str(out.get("summary", "")).strip(),
        "confidence": max(0.0, min(1.0, fnum(out.get("confidence", 0.0), 0.0))),
    }


def call_vllm(url, model, prompt):
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0,
    }

    r = requests.post(url, json=payload, timeout=180)
    r.raise_for_status()

    data = r.json()
    content = data["choices"][0]["message"]["content"]
    return content


def main():
    args = parse_args()
    con = duckdb.connect(args.db_path)

    debug_dir = Path(args.debug_dir).expanduser().resolve()
    debug_dir.mkdir(parents=True, exist_ok=True)

    q = """
    SELECT
        a.raw_id,
        a.symbol,
        a.published_at,
        a.title
    FROM news_articles_effective a
    LEFT JOIN news_llm_enrichment e
      ON a.raw_id = e.raw_id
     AND a.symbol = e.symbol
     AND e.model_name = ?
     AND e.prompt_version = ?
    WHERE e.raw_id IS NULL
    ORDER BY a.raw_id, a.symbol
    """

    params = [args.model, args.prompt_version]
    if args.limit:
        q += f" LIMIT {args.limit}"

    rows = con.execute(q, params).fetchall()

    for raw_id, symbol, published_at, title in tqdm(rows):
        prompt = PROMPT.format(title=title)

        try:
            raw_content = call_vllm(args.vllm_url, args.model, prompt)

            raw_path = debug_dir / f"{raw_id}_{symbol}.txt"
            raw_path.write_text(raw_content, encoding="utf-8")

            json_text = extract_first_json_object(raw_content)
            out = normalize_output(json.loads(json_text))

        except Exception as e:
            print(f"LLM error raw_id={raw_id} symbol={symbol}: {e}")
            continue

        con.execute(
            """
            INSERT INTO news_llm_enrichment (
                raw_id,
                symbol,
                published_at,
                model_name,
                prompt_version,
                sentiment_label,
                sentiment_score,
                relevance_score,
                event_type,
                materiality_score,
                horizon_label,
                summary,
                confidence,
                inference_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                raw_id,
                symbol,
                published_at,
                args.model,
                args.prompt_version,
                out["sentiment_label"],
                out["sentiment_score"],
                out["relevance_score"],
                out["event_type"],
                out["materiality_score"],
                out["horizon_label"],
                out["summary"],
                out["confidence"],
            ],
        )

        if args.verbose:
            print(f"[llm ok] raw_id={raw_id} symbol={symbol}")

    con.close()


if __name__ == "__main__":
    main()
