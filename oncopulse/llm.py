import json
import re
from typing import Any

import requests

from .config import LLM_TIMEOUT_SECONDS, ONCOPULSE_LLM_MODEL, OPENAI_API_BASE, OPENAI_API_KEY
from .text_utils import clean_text


REQUIRED_KEYS = [
    "Study type / phase",
    "Population",
    "Intervention vs comparator",
    "Endpoints mentioned",
    "Key finding",
    "Supporting snippets",
    "Why it matters",
]


def _extract_numbers(text: str) -> set[str]:
    return set(re.findall(r"\b\d+(?:\.\d+)?%?\b", text or ""))


def _has_prescriptive_language(text: str) -> bool:
    banned = [
        "should use",
        "preferred regimen",
        "must use",
        "recommend using",
        "first-line choice",
        "best treatment",
    ]
    lower = (text or "").lower()
    return any(p in lower for p in banned)


def _parse_lines(summary_text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in (summary_text or "").splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        out[clean_text(k)] = clean_text(v)
    return out


def _is_valid_structured_summary(text: str) -> bool:
    parsed = _parse_lines(text)
    return all(k in parsed for k in REQUIRED_KEYS)


def _format_ordered(parsed: dict[str, str]) -> str:
    return "\n".join(f"{k}: {clean_text(parsed.get(k, 'Not stated'))}" for k in REQUIRED_KEYS)


def polish_summary_strict(
    deterministic_summary: str,
    source_text: str,
    support_snippets: list[str] | None = None,
) -> str | None:
    if not OPENAI_API_KEY:
        return None

    source = clean_text(source_text)
    snippets = [clean_text(s) for s in (support_snippets or []) if clean_text(s)]
    evidence_blob = "\n".join([source] + snippets)
    if not source:
        return None

    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    prompt = (
        "Rewrite the oncology structured summary for readability only. "
        "Do NOT add facts. Do NOT add or change numbers. "
        "Keep exactly these keys and order:\n"
        "- Study type / phase\n"
        "- Population\n"
        "- Intervention vs comparator\n"
        "- Endpoints mentioned\n"
        "- Key finding\n"
        "- Supporting snippets\n"
        "- Why it matters\n"
        "Why it matters must be conservative and non-prescriptive.\n"
        "Return plain text only with one key-value per line."
    )

    payload: dict[str, Any] = {
        "model": ONCOPULSE_LLM_MODEL,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Source evidence:\n{evidence_blob}\n\n"
                    f"Current summary:\n{deterministic_summary}"
                ),
            },
        ],
    }

    try:
        resp = requests.post(
            f"{OPENAI_API_BASE}/chat/completions",
            headers=headers,
            json=payload,
            timeout=LLM_TIMEOUT_SECONDS,
        )
        resp.raise_for_status()
        data = resp.json()
        text = clean_text(data["choices"][0]["message"]["content"])
    except Exception:
        return None

    if not text or not _is_valid_structured_summary(text):
        return None
    if _has_prescriptive_language(text):
        return None

    allowed_numbers = _extract_numbers(evidence_blob + "\n" + deterministic_summary)
    out_numbers = _extract_numbers(text)
    if not out_numbers.issubset(allowed_numbers):
        return None

    parsed = _parse_lines(text)
    return _format_ordered(parsed)
