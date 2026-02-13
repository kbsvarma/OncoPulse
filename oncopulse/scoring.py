import json
from datetime import datetime, timezone
import math
import re
from typing import Any


DEFAULT_WEIGHTS: dict[str, float] = {
    "phase_iii": 6,
    "randomized": 5,
    "meta_analysis": 4,
    "phase_ii": 3,
    "overall_survival": 2,
    "progression_free_survival": 2,
    "sample_size": 1,
    "major_journal": 1,
    "citations_multiplier": 1.0,
    "preclinical_penalty": -4,
    "case_report_penalty": -3,
    "global_penalty": -2,
    "include_term": 1,
    "exclude_term": -1,
    "query_exact_phrase": 8,
    "query_concept": 3,
    "query_keyword": 1,
    "query_coverage": 2,
}

BOOSTS = [
    ("phase_iii", ["phase iii", "phase 3"]),
    ("randomized", ["randomized", "rct"]),
    ("meta_analysis", ["meta-analysis", "systematic review"]),
    ("phase_ii", ["phase ii", "phase 2"]),
]


def _has_any(text: str, terms: list[str]) -> bool:
    t = text.lower()
    return any(term.lower() in t for term in terms)


def _sample_size_boost(text: str) -> bool:
    nums = re.findall(r"\b(?:n\s*=\s*|enrolled\s*=\s*|patients?\s*=\s*)(\d{2,5})\b", text.lower())
    values = [int(n) for n in nums]
    return any(v >= 200 for v in values)


def _resolved_weights(overrides: dict[str, float] | None) -> dict[str, float]:
    weights = dict(DEFAULT_WEIGHTS)
    if overrides:
        for k, v in overrides.items():
            if k in weights:
                try:
                    weights[k] = float(v)
                except (TypeError, ValueError):
                    continue
    return weights

def _contains_term(blob: str, term: str) -> bool:
    t = (term or "").strip().lower()
    if not t:
        return False
    if re.fullmatch(r"[a-z0-9]+", t):
        return re.search(rf"\b{re.escape(t)}\b", blob) is not None
    return t in blob


def _query_relevance_boost(blob: str, query_context: dict[str, Any] | None, w: dict[str, float]) -> tuple[int, list[str]]:
    if not query_context:
        return 0, []

    explain: list[str] = []
    score = 0

    raw_query = str(query_context.get("raw_query") or query_context.get("query_text") or "").strip().lower()
    if raw_query and len(raw_query) >= 12 and raw_query in blob:
        points = int(w["query_exact_phrase"])
        score += points
        explain.append(f"+{points} query phrase match")

    concepts_raw = query_context.get("concepts") or []
    concept_hits = 0
    for group in concepts_raw:
        if not isinstance(group, list):
            continue
        terms = [str(t).strip().lower() for t in group if str(t).strip()]
        if terms and any(_contains_term(blob, t) for t in terms):
            concept_hits += 1
    if concept_hits:
        points = int(w["query_concept"]) * concept_hits
        score += points
        explain.append(f"+{points} query concept match ({concept_hits})")

    keywords_raw = query_context.get("keywords") or []
    keywords = [str(k).strip().lower() for k in keywords_raw if str(k).strip()]
    keyword_hits = [k for k in keywords if _contains_term(blob, k)]
    if keyword_hits:
        counted = min(len(set(keyword_hits)), 6)
        points = int(w["query_keyword"]) * counted
        score += points
        explain.append(f"+{points} query keyword match ({counted})")

        unique_kw = len(set(keywords))
        if unique_kw >= 3 and (len(set(keyword_hits)) / unique_kw) >= 0.5:
            points = int(w["query_coverage"])
            score += points
            explain.append(f"+{points} query coverage")

    return score, explain


def _parse_pub_date(value: str | None) -> datetime | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def citations_per_year(item: dict[str, Any], now: datetime | None = None) -> float | None:
    citations = item.get("citations")
    if not isinstance(citations, int) or citations < 0:
        return None
    dt = _parse_pub_date(item.get("published_at") or item.get("updated_at"))
    if not dt:
        return None
    ref = now or datetime.now(timezone.utc)
    age_years = max((ref - dt).total_seconds() / (365.25 * 86400.0), 1 / 12.0)
    return round(citations / age_years, 2)


def hot_score(item: dict[str, Any], now: datetime | None = None) -> float:
    # Blend recency with citation momentum so very old highly-cited papers don't always dominate.
    dt = _parse_pub_date(item.get("published_at") or item.get("updated_at"))
    ref = now or datetime.now(timezone.utc)
    age_days = (ref - dt).days if dt else 3650
    recency_component = 1.0 / (1.0 + max(age_days, 0) / 30.0)
    citation_rate = citations_per_year(item, now=ref) or 0.0
    return round(0.55 * math.log1p(max(citation_rate, 0.0)) + 0.45 * recency_component, 4)


def score_item(
    item: dict[str, Any],
    pack_rules: dict[str, Any],
    weight_overrides: dict[str, float] | None = None,
) -> tuple[int, list[str]]:
    title = (item.get("title") or "").lower()
    text = (item.get("abstract_or_text") or "").lower()
    venue = (item.get("venue") or "").lower()
    blob = " ".join([title, text])
    w = _resolved_weights(weight_overrides)

    score = 0
    explain: list[str] = []

    for weight_key, terms in BOOSTS:
        if _has_any(blob, terms):
            points = int(w[weight_key])
            score += points
            explain.append(f"+{points} {terms[0]}")

    if _has_any(blob, ["overall survival", " os "]):
        points = int(w["overall_survival"])
        score += points
        explain.append(f"+{points} overall survival")

    if _has_any(blob, ["progression-free survival", " pfs "]):
        points = int(w["progression_free_survival"])
        score += points
        explain.append(f"+{points} progression-free survival")

    if _sample_size_boost(blob):
        points = int(w["sample_size"])
        score += points
        explain.append(f"+{points} sample size >=200")

    journals = [j.lower() for j in pack_rules.get("major_journals", [])]
    if any(j in venue for j in journals):
        points = int(w["major_journal"])
        score += points
        explain.append(f"+{points} major journal")

    citations = item.get("citations")
    if isinstance(citations, int) and citations >= 0:
        c_bonus = int(math.log1p(citations) * float(w["citations_multiplier"]))
        if c_bonus > 0:
            score += c_bonus
            explain.append(f"+{c_bonus} citations bonus")

    penalties = [
        (int(w["preclinical_penalty"]), ["mouse", "murine", "cell line", "in vitro"], "preclinical signal"),
        (int(w["case_report_penalty"]), ["case report"], "case report"),
        (int(w["global_penalty"]), pack_rules.get("global_penalty_terms", []), "global penalty"),
    ]
    for p, terms, label in penalties:
        if terms and _has_any(blob, [t.lower() for t in terms]):
            score += p
            explain.append(f"{p} {label}")

    for term in pack_rules.get("include_terms", []):
        if term.lower() in blob:
            points = int(w["include_term"])
            score += points
            explain.append(f"+{points} include term: {term}")

    for term in pack_rules.get("exclude_terms", []):
        if term.lower() in blob:
            points = int(w["exclude_term"])
            score += points
            explain.append(f"{points} exclude term: {term}")

    q_points, q_explain = _query_relevance_boost(blob, pack_rules.get("search_query_context"), w)
    score += q_points
    explain.extend(q_explain)

    return score, explain


def score_and_attach(
    item: dict[str, Any],
    pack_rules: dict[str, Any],
    weight_overrides: dict[str, float] | None = None,
) -> dict[str, Any]:
    score, explain = score_item(item, pack_rules, weight_overrides=weight_overrides)
    item["score"] = score
    item["score_explain"] = explain
    item["score_explain_json"] = json.dumps(explain)
    return item
