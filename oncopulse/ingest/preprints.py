from datetime import datetime, timedelta, timezone
import time
from typing import Any

import requests

from ..config import BACKOFF_SECONDS, BIORXIV_BASE, MAX_RETRIES, MEDRXIV_BASE, REQUEST_TIMEOUT


def _terms_from_query(query: str) -> list[str]:
    raw = query.replace("(", " ").replace(")", " ").replace('"', " ").replace("AND", " ").replace("OR", " ")
    terms = [t.strip().lower() for t in raw.split() if len(t.strip()) >= 4]
    seen: set[str] = set()
    out: list[str] = []
    for t in terms:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out[:20]


def _fetch_server(server: str, start_date: str, end_date: str, limit: int) -> list[dict[str, Any]]:
    base = BIORXIV_BASE if server == "biorxiv" else MEDRXIV_BASE
    cursor = 0
    page_size = min(limit, 100)
    out: list[dict[str, Any]] = []

    while len(out) < limit:
        url = f"{base}/details/{server}/{start_date}/{end_date}/{cursor}"
        payload: dict[str, Any] | None = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(BACKOFF_SECONDS * attempt)
                    continue
                resp.raise_for_status()
                payload = resp.json()
                break
            except Exception:  # noqa: BLE001
                time.sleep(BACKOFF_SECONDS * attempt)

        # Fail soft for this server/page and keep the overall pipeline alive.
        if not payload:
            break

        collection = payload.get("collection", []) or []
        if not collection:
            break
        out.extend(collection)
        if len(collection) < page_size:
            break
        cursor += page_size

    return out[:limit]


def search(query: str, days_back: int = 30, limit: int = 100) -> list[dict[str, Any]]:
    if limit <= 0:
        return []

    start = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    terms = _terms_from_query(query)

    raw_records = _fetch_server("biorxiv", start, end, limit) + _fetch_server("medrxiv", start, end, limit)

    items: list[dict[str, Any]] = []
    for r in raw_records:
        title = (r.get("title") or "").strip()
        abstract = (r.get("abstract") or "").strip()
        blob = f"{title} {abstract}".lower()
        if terms and not any(t in blob for t in terms):
            continue

        doi = (r.get("doi") or "").strip() or None
        server = (r.get("server") or "preprint").lower()

        items.append(
            {
                "source": "preprint",
                "title": title or "Untitled preprint",
                "url": f"https://doi.org/{doi}" if doi else "https://www.medrxiv.org",
                "published_at": r.get("date"),
                "updated_at": None,
                "pmid": None,
                "doi": doi,
                "nct_id": None,
                "venue": server,
                "authors": r.get("authors"),
                "abstract_or_text": abstract,
            }
        )

        if len(items) >= limit:
            break

    return items
