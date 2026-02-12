import re


STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "with",
    "without",
    "vs",
    "versus",
    "study",
    "trial",
    "trials",
    "cancer",
    "oncology",
}


CONCEPT_PATTERNS: list[tuple[re.Pattern[str], list[str]]] = [
    (re.compile(r"\bnsclc\b|\bnon[-\s]?small cell lung cancer\b", re.I), ["NSCLC", "non-small cell lung cancer"]),
    (re.compile(r"\bsclc\b|\bsmall cell lung cancer\b", re.I), ["SCLC", "small cell lung cancer"]),
    (re.compile(r"\bpd-?1\b", re.I), ["PD-1", "programmed death-1"]),
    (re.compile(r"\bpd-?l1\b", re.I), ["PD-L1", "programmed death-ligand 1"]),
    (re.compile(r"\bcheckpoint inhibitor", re.I), ["checkpoint inhibitor", "immune checkpoint blockade"]),
    (re.compile(r"\bcar[-\s]?t\b", re.I), ["CAR-T", "chimeric antigen receptor T cell"]),
    (re.compile(r"\bio\b|\bimmunotherapy\b", re.I), ["immunotherapy", "immune therapy"]),
    (re.compile(r"\bpneumonitis\b", re.I), ["pneumonitis", "immune-related adverse event"]),
    (re.compile(r"\bir?ae\b|\bimmune[-\s]?related adverse", re.I), ["immune-related adverse event", "irAE"]),
    (re.compile(r"\bos\b|\boverall survival\b", re.I), ["overall survival", "OS"]),
    (re.compile(r"\bpfs\b|\bprogression[-\s]?free survival\b", re.I), ["progression-free survival", "PFS"]),
    (re.compile(r"\borr\b|\bobjective response rate\b", re.I), ["objective response rate", "ORR"]),
    (re.compile(r"\btriple[-\s]?negative\b|\btnbc\b", re.I), ["triple-negative breast cancer", "TNBC"]),
    (re.compile(r"\bher2\b", re.I), ["HER2", "ERBB2"]),
    (re.compile(r"\bcrc\b|\bcolorectal\b", re.I), ["colorectal cancer", "CRC"]),
]


def _normalize_token(token: str) -> str:
    return re.sub(r"[^a-z0-9\-\+]", "", token.lower()).strip()


def extract_keywords(query: str, max_terms: int = 10) -> list[str]:
    words = re.findall(r"[A-Za-z0-9\-\+]{3,}", query.lower())
    out: list[str] = []
    seen: set[str] = set()
    for w in words:
        t = _normalize_token(w)
        if not t or t in STOPWORDS or t.isdigit():
            continue
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= max_terms:
            break
    return out


def _concept_groups(query: str) -> list[list[str]]:
    groups: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for pattern, terms in CONCEPT_PATTERNS:
        if pattern.search(query):
            norm_terms = tuple(dict.fromkeys([t.strip() for t in terms if t.strip()]))
            if norm_terms and norm_terms not in seen:
                groups.append(list(norm_terms))
                seen.add(norm_terms)
    return groups


def _pubmed_group(term_group: list[str]) -> str:
    if len(term_group) == 1:
        return f"({term_group[0]})"
    return "(" + " OR ".join(term_group) + ")"


def build_search_queries(user_query: str) -> dict[str, object]:
    raw = (user_query or "").strip()
    if not raw:
        return {"paper_query": "", "trial_query": "", "keywords": [], "concepts": []}

    concepts = _concept_groups(raw)
    keywords = extract_keywords(raw, max_terms=10)
    keyword_groups = [[k] for k in keywords[:6]]

    merged_groups = concepts + keyword_groups
    if not merged_groups:
        merged_groups = [[raw]]

    # PubMed / Europe PMC query: concept-aware boolean.
    paper_query = " AND ".join(_pubmed_group(group) for group in merged_groups[:8])

    # Trial APIs generally work better with natural keyword strings.
    trial_terms: list[str] = []
    for group in merged_groups[:8]:
        trial_terms.append(group[0])
    trial_query = " ".join(dict.fromkeys(trial_terms))

    return {
        "paper_query": paper_query,
        "trial_query": trial_query if trial_query else raw,
        "keywords": keywords,
        "concepts": concepts,
    }
