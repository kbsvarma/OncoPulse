import re
from typing import Any
from . import llm
from .text_utils import clean_text

NO_INFO_WHY_IT_MATTERS = "Why it matters: Not enough info in abstract."
BANNED_PRESCRIPTIVE_PHRASES = [
    "should use",
    "preferred regimen",
    "must use",
    "recommend using",
    "first-line choice",
    "best treatment",
]


def _has_numeric(text: str) -> bool:
    return bool(re.search(r"\b\d+(?:\.\d+)?%?\b", text))


def _detect_study_type(text: str) -> str:
    t = text.lower()
    if "meta-analysis" in t or "systematic review" in t:
        return "Meta-analysis / systematic review"
    if "randomized" in t or "rct" in t:
        return "Randomized trial"
    if "phase iii" in t or "phase 3" in t:
        return "Phase III trial"
    if "phase ii" in t or "phase 2" in t:
        return "Phase II trial"
    return "Not stated"


def _extract_population(text: str) -> str:
    patterns = [
        r"(patients?[^.]{0,180}\.)",
        r"(adults?[^.]{0,180}\.)",
        r"((?:men|women)[^.]{0,180}\.)",
        r"((?:participants|subjects)[^.]{0,180}\.)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            candidate = m.group(1).strip()
            # Trust mode guardrail: omit numeric population claims unless explicitly validated elsewhere.
            if _has_numeric(candidate):
                return "Not stated"
            return candidate
    return "Not stated"


def _extract_intervention(text: str) -> str:
    m = re.search(r"([^.]{0,120}(?:compared with|versus|vs\.?)[^.]{0,120}\.)", text, re.IGNORECASE)
    if m:
        candidate = m.group(1).strip()
        if _has_numeric(candidate):
            return "Not stated"
        return candidate
    m2 = re.search(r"(received[^.]{0,140}\.)", text, re.IGNORECASE)
    if m2:
        candidate = m2.group(1).strip()
        if _has_numeric(candidate):
            return "Not stated"
        return candidate
    return "Not stated"


def _extract_endpoints(text: str) -> str:
    endpoints = []
    lower = text.lower()
    for token in ["overall survival", "os", "progression-free survival", "pfs", "orr", "toxicity", "adverse event"]:
        if token in lower:
            endpoints.append(token.upper() if token in {"os", "pfs", "orr"} else token)
    return ", ".join(sorted(set(endpoints))) if endpoints else "Not stated"


def _extract_key_finding(text: str) -> str:
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    cues = [
        "significant",
        "improved",
        "reduced",
        "increased",
        "no difference",
        "met primary endpoint",
        "superior",
        "non-inferior",
        "did not meet",
    ]
    for s in sentences:
        ls = s.lower()
        if any(c in ls for c in cues):
            candidate = s.strip()
            if _has_numeric(candidate):
                return "Not explicitly stated in provided text"
            return candidate
    return "Not explicitly stated in provided text"


def _clean_sentences(text: str, max_sentences: int = 3) -> list[str]:
    raw = re.split(r"(?<=[.!?])\s+", text.strip())
    cleaned: list[str] = []
    for s in raw:
        s2 = " ".join(s.split()).strip()
        if not s2:
            continue
        if len(s2) < 30:
            continue
        cleaned.append(s2)
        if len(cleaned) >= max_sentences:
            break
    return cleaned


def _has_safety_signal(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in ["toxicity", "adverse event", "adverse events", "pneumonitis", "safety"])


def _sanitize_why_text(text: str) -> str:
    out = text
    for phrase in BANNED_PRESCRIPTIVE_PHRASES:
        out = re.sub(re.escape(phrase), " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _build_why_it_matters(
    item: dict[str, Any],
    study: str,
    endpoints: str,
    population: str,
    key_finding: str,
    text: str,
) -> str:
    signals: list[str] = []
    if study != "Not stated":
        signals.append(f"Study signal: {study.lower()}.")
    if endpoints != "Not stated":
        signals.append(f"Reported endpoints include {endpoints}.")
    if population != "Not stated":
        signals.append("Population is described in the abstract.")
    status = clean_text(item.get("status"))
    if status:
        signals.append(f"Trial status update: {status}.")
    if _has_safety_signal(text):
        signals.append("Safety-related language is present and may affect monitoring context.")
    if key_finding not in {"Not explicitly stated in provided text", "Key finding: No abstract available"}:
        signals.append("The abstract reports a directional result that may guide evidence tracking.")

    if not signals:
        return NO_INFO_WHY_IT_MATTERS

    # Keep concise and non-prescriptive: 1-2 short lines.
    why = "Why it matters: " + " ".join(signals[:2])
    why = _sanitize_why_text(why)
    if not why.strip() or why.strip().lower() == "why it matters:":
        return NO_INFO_WHY_IT_MATTERS
    return why


def summarize_item(item: dict[str, Any], llm_polish: bool = False) -> str:
    text = clean_text(item.get("full_text_text") or item.get("abstract_or_text"))
    support_snippets = item.get("support_snippets") or []
    if not text:
        summary = (
            "Study type / phase: Not stated\n"
            "Population: Not stated\n"
            "Intervention vs comparator: Not stated\n"
            "Endpoints mentioned: Not stated\n"
            "Key finding: No abstract available\n"
            "Supporting snippets: Not available\n"
            f"{NO_INFO_WHY_IT_MATTERS}"
        )
        return summary

    study = _detect_study_type(text)
    pop = _extract_population(text)
    intervention = _extract_intervention(text)
    endpoints = _extract_endpoints(text)
    key = _extract_key_finding(text)
    why = _build_why_it_matters(item, study, endpoints, pop, key, text)
    snippets_text = " | ".join([clean_text(s) for s in support_snippets[:3] if clean_text(s)])
    if not snippets_text:
        snippets_text = "Not available"
    summary = (
        f"Study type / phase: {study}\n"
        f"Population: {pop}\n"
        f"Intervention vs comparator: {intervention}\n"
        f"Endpoints mentioned: {endpoints}\n"
        f"Key finding: {key}\n"
        f"Supporting snippets: {snippets_text}\n"
        f"{why}"
    )

    if not llm_polish:
        return summary
    polished = llm.polish_summary_strict(summary, source_text=text, support_snippets=support_snippets)
    return polished or summary
