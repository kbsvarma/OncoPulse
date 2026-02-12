from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from ..config import EUROPE_PMC_BASE, REQUEST_TIMEOUT


def _terms_from_query(query: str) -> list[str]:
    raw = query.replace("(", " ").replace(")", " ").replace('"', " ").replace("AND", " ").replace("OR", " ")
    terms = [t.strip().lower() for t in raw.split() if len(t.strip()) >= 4]
    # Keep unique order
    seen: set[str] = set()
    out: list[str] = []
    for t in terms:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out[:20]


def _matches_query(text: str, terms: list[str]) -> bool:
    if not terms:
        return True
    lower = text.lower()
    return any(t in lower for t in terms)


def search(query: str, days_back: int = 30, limit: int = 100, preprint_only: bool = False) -> list[dict[str, Any]]:
    if not query.strip() or limit <= 0:
        return []

    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    epmc_query = f"({query}) AND FIRST_PDATE:[{start_date} TO *]"
    if preprint_only:
        epmc_query += " AND SRC:PPR"
    else:
        epmc_query += " AND (SRC:MED OR SRC:PPR)"

    params = {
        "query": epmc_query,
        "format": "json",
        "resultType": "core",
        "pageSize": min(limit, 1000),
    }
    resp = requests.get(EUROPE_PMC_BASE, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    results = resp.json().get("resultList", {}).get("result", []) or []

    terms = _terms_from_query(query)
    items: list[dict[str, Any]] = []
    for r in results:
        title = (r.get("title") or "").strip()
        abstract = (r.get("abstractText") or "").strip()
        if not _matches_query(f"{title} {abstract}", terms):
            continue

        source = "preprint" if r.get("source") == "PPR" else "europepmc"
        doi = (r.get("doi") or "").strip() or None
        pmid = (r.get("pmid") or "").strip() or None
        pmcid = (r.get("pmcid") or "").strip() or None

        if doi:
            url = f"https://doi.org/{doi}"
        elif pmid:
            url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
        elif pmcid:
            url = f"https://europepmc.org/article/PMC/{pmcid}"
        else:
            url = "https://europepmc.org"

        items.append(
            {
                "source": source,
                "title": title or "Untitled article",
                "url": url,
                "published_at": r.get("firstPublicationDate") or r.get("pubYear"),
                "updated_at": r.get("dateOfRevision"),
                "pmid": pmid,
                "doi": doi,
                "nct_id": None,
                "venue": r.get("journalTitle") or "Europe PMC",
                "authors": r.get("authorString"),
                "abstract_or_text": abstract,
            }
        )

        if len(items) >= limit:
            break

    return items


def fetch_abstract_by_ids(pmid: str | None = None, doi: str | None = None) -> str | None:
    clauses = []
    if pmid:
        clauses.append(f"EXT_ID:{pmid}")
    if doi:
        clauses.append(f'DOI:"{doi}"')
    if not clauses:
        return None

    query = " OR ".join(clauses)
    params = {
        "query": query,
        "format": "json",
        "resultType": "core",
        "pageSize": 1,
    }
    try:
        resp = requests.get(EUROPE_PMC_BASE, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        result = (resp.json().get("resultList", {}).get("result", []) or [])
        if not result:
            return None
        abstract = (result[0].get("abstractText") or "").strip()
        return abstract or None
    except Exception:
        return None
