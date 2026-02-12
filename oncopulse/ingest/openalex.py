from datetime import datetime, timedelta, timezone
import re
import time
from typing import Optional
from urllib.parse import quote

import requests

from ..config import BACKOFF_SECONDS, CITATION_CACHE_TTL_DAYS, MAX_RETRIES, OPENALEX_BASE, REQUEST_TIMEOUT
from ..db import get_cached_citation, set_cached_citation


def _is_fresh(fetched_at: str, ttl_days: int) -> bool:
    try:
        dt = datetime.fromisoformat(fetched_at)
    except ValueError:
        return False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - dt <= timedelta(days=ttl_days)


def _normalize_doi(doi: str) -> str:
    d = doi.strip().lower()
    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", d)
    d = re.sub(r"^doi:", "", d).strip()
    return d


def get_citations(conn, doi: str | None, ttl_days: int = CITATION_CACHE_TTL_DAYS) -> Optional[int]:
    if not doi:
        return None

    doi_norm = _normalize_doi(doi)
    if not doi_norm:
        return None
    cached = get_cached_citation(conn, doi_norm)
    if cached and _is_fresh(cached.get("fetched_at", ""), ttl_days):
        return cached.get("cited_by_count")

    work_id = quote(f"https://doi.org/{doi_norm}", safe="")
    url = f"{OPENALEX_BASE}/{work_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT, params={"mailto": "oncopulse@example.com"})
            if resp.status_code == 404:
                set_cached_citation(conn, doi_norm, None)
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            resp.raise_for_status()
            count = resp.json().get("cited_by_count")
            normalized_count = int(count) if isinstance(count, int) or (isinstance(count, str) and count.isdigit()) else None
            set_cached_citation(conn, doi_norm, normalized_count)
            return normalized_count
        except Exception:  # noqa: BLE001
            time.sleep(BACKOFF_SECONDS * attempt)

    if cached:
        return cached.get("cited_by_count")
    return None
