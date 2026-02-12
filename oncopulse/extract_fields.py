import re

from .text_utils import clean_text


def detect_phase(text: str) -> str:
    t = clean_text(text).lower()
    if "phase iii" in t or "phase 3" in t:
        return "Phase III"
    if "phase ii" in t or "phase 2" in t:
        return "Phase II"
    if "phase i" in t or "phase 1" in t:
        return "Phase I"
    if "phase iv" in t or "phase 4" in t:
        return "Phase IV"
    return "Unknown"


def detect_study_type(text: str) -> str:
    t = clean_text(text).lower()
    if "meta-analysis" in t or "systematic review" in t:
        return "Meta-analysis/Systematic review"
    if "randomized" in t or "rct" in t:
        return "Randomized trial"
    if "retrospective" in t:
        return "Retrospective study"
    if "prospective" in t:
        return "Prospective study"
    if "single-arm" in t or "single arm" in t:
        return "Single-arm study"
    return "Unknown"


def detect_endpoints(text: str) -> str:
    t = clean_text(text).lower()
    endpoints: list[str] = []
    mapping = [
        ("overall survival", "OS"),
        ("progression-free survival", "PFS"),
        ("objective response rate", "ORR"),
        ("orr", "ORR"),
        ("disease-free survival", "DFS"),
        ("toxicity", "Toxicity"),
        ("adverse event", "Adverse events"),
    ]
    for needle, label in mapping:
        if needle in t and label not in endpoints:
            endpoints.append(label)
    return ", ".join(endpoints) if endpoints else "Unknown"


def detect_sample_size(text: str) -> str:
    t = clean_text(text).lower()
    matches = re.findall(r"\b(?:n\s*=\s*|enrolled\s*=\s*|patients?\s*=\s*|participants?\s*=\s*)(\d{2,5})\b", t)
    if not matches:
        return "Unknown"
    values = [int(m) for m in matches]
    max_n = max(values)
    return f"N~{max_n}"
