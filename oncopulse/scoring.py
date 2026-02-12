import json
import math
import re
from typing import Any


BOOSTS = [
    (6, ["phase iii", "phase 3"]),
    (5, ["randomized", "rct"]),
    (4, ["meta-analysis", "systematic review"]),
    (3, ["phase ii", "phase 2"]),
]


def _has_any(text: str, terms: list[str]) -> bool:
    t = text.lower()
    return any(term.lower() in t for term in terms)


def _sample_size_boost(text: str) -> bool:
    nums = re.findall(r"\b(?:n\s*=\s*|enrolled\s*=\s*|patients?\s*=\s*)(\d{2,5})\b", text.lower())
    values = [int(n) for n in nums]
    return any(v >= 200 for v in values)


def score_item(item: dict[str, Any], pack_rules: dict[str, Any]) -> tuple[int, list[str]]:
    title = (item.get("title") or "").lower()
    text = (item.get("abstract_or_text") or "").lower()
    venue = (item.get("venue") or "").lower()
    blob = " ".join([title, text])

    score = 0
    explain: list[str] = []

    for points, terms in BOOSTS:
        if _has_any(blob, terms):
            score += points
            explain.append(f"+{points} {terms[0]}")

    if _has_any(blob, ["overall survival", " os "]):
        score += 2
        explain.append("+2 overall survival")

    if _has_any(blob, ["progression-free survival", " pfs "]):
        score += 2
        explain.append("+2 progression-free survival")

    if _sample_size_boost(blob):
        score += 1
        explain.append("+1 sample size >=200")

    journals = [j.lower() for j in pack_rules.get("major_journals", [])]
    if any(j in venue for j in journals):
        score += 1
        explain.append("+1 major journal")

    citations = item.get("citations")
    if isinstance(citations, int) and citations >= 0:
        c_bonus = int(math.log1p(citations))
        if c_bonus > 0:
            score += c_bonus
            explain.append(f"+{c_bonus} citations bonus")

    penalties = [
        (-4, ["mouse", "murine", "cell line", "in vitro"], "preclinical signal"),
        (-3, ["case report"], "case report"),
        (-2, pack_rules.get("global_penalty_terms", []), "global penalty"),
    ]
    for p, terms, label in penalties:
        if terms and _has_any(blob, [t.lower() for t in terms]):
            score += p
            explain.append(f"{p} {label}")

    for term in pack_rules.get("include_terms", []):
        if term.lower() in blob:
            score += 1
            explain.append(f"+1 include term: {term}")

    for term in pack_rules.get("exclude_terms", []):
        if term.lower() in blob:
            score -= 1
            explain.append(f"-1 exclude term: {term}")

    return score, explain


def score_and_attach(item: dict[str, Any], pack_rules: dict[str, Any]) -> dict[str, Any]:
    score, explain = score_item(item, pack_rules)
    item["score"] = score
    item["score_explain"] = explain
    item["score_explain_json"] = json.dumps(explain)
    return item
