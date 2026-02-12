import re
from typing import Any
from .text_utils import clean_text


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
            return m.group(1).strip()
    return "Not stated"


def _extract_intervention(text: str) -> str:
    m = re.search(r"([^.]{0,120}(?:compared with|versus|vs\.?)[^.]{0,120}\.)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"(received[^.]{0,140}\.)", text, re.IGNORECASE)
    if m2:
        return m2.group(1).strip()
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
            return s.strip()
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


def summarize_item(item: dict[str, Any]) -> str:
    text = clean_text(item.get("abstract_or_text"))
    if not text:
        return (
            "Study type / phase: Not stated\n"
            "Population: Not stated\n"
            "Intervention vs comparator: Not stated\n"
            "Endpoints mentioned: Not stated\n"
            "Key finding: No abstract available\n"
            "Why it matters: Evidence signal exists, but source text is insufficient for interpretation."
        )

    study = _detect_study_type(text)
    pop = _extract_population(text)
    intervention = _extract_intervention(text)
    endpoints = _extract_endpoints(text)
    key = _extract_key_finding(text)
    return (
        f"Study type / phase: {study}\n"
        f"Population: {pop}\n"
        f"Intervention vs comparator: {intervention}\n"
        f"Endpoints mentioned: {endpoints}\n"
        f"Key finding: {key}\n"
        "Why it matters: This may inform current evidence awareness; clinical action requires full-text and guideline context."
    )
