from datetime import datetime, timedelta, timezone
import time
from typing import Any

import requests

from ..config import BACKOFF_SECONDS, FDA_DRUGS_BASE, MAX_RETRIES, REQUEST_TIMEOUT


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


def search(query: str, days_back: int = 30, limit: int = 100) -> list[dict[str, Any]]:
    if limit <= 0:
        return []

    start_date = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d")
    params = {
        "limit": min(limit, 100),
        "sort": "submissions.submission_status_date:desc",
        "search": f"submissions.submission_status_date:[{start_date}+TO+99991231]",
    }

    data: list[dict[str, Any]] = []
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(FDA_DRUGS_BASE, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            resp.raise_for_status()
            data = resp.json().get("results", []) or []
            break
        except Exception:  # noqa: BLE001
            time.sleep(BACKOFF_SECONDS * attempt)

    # Fallback: openFDA can intermittently return HTTP 500 for filtered date windows.
    if not data:
        fallback_params = {
            "limit": min(limit, 100),
            "sort": "submissions.submission_status_date:desc",
        }
        try:
            resp = requests.get(FDA_DRUGS_BASE, params=fallback_params, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 400:
                return []
            data = resp.json().get("results", []) or []
        except Exception:  # noqa: BLE001
            return []

    terms = _terms_from_query(query)
    items: list[dict[str, Any]] = []

    for r in data:
        sponsors = r.get("sponsor_name") or "FDA"
        products = r.get("products", []) or []
        submissions = r.get("submissions", []) or []

        primary_product = products[0] if products else {}
        brand = primary_product.get("brand_name") or primary_product.get("drug_name") or "Unknown product"
        active = primary_product.get("active_ingredients", []) or []
        actives = ", ".join([a.get("name", "") for a in active if a.get("name")])

        latest_sub = submissions[0] if submissions else {}
        status = latest_sub.get("submission_status") or "Status not stated"
        status_date = latest_sub.get("submission_status_date")
        app_num = r.get("application_number")
        if status_date:
            try:
                dt = datetime.strptime(status_date[:8], "%Y%m%d").replace(tzinfo=timezone.utc)
                if dt < (datetime.now(timezone.utc) - timedelta(days=days_back)):
                    continue
            except ValueError:
                pass

        text = f"{brand} {actives} {status} {sponsors}".lower()
        if terms and not any(t in text for t in terms):
            continue

        item_title = f"FDA update: {brand}"
        url = f"https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo={app_num}" if app_num else "https://www.fda.gov/drugs"
        body = f"Sponsor: {sponsors}. Status: {status}. Active ingredients: {actives or 'Not stated'}."

        items.append(
            {
                "source": "fda",
                "title": item_title,
                "url": url,
                "published_at": None,
                "updated_at": status_date,
                "pmid": None,
                "doi": None,
                "nct_id": None,
                "venue": "FDA",
                "authors": None,
                "abstract_or_text": body,
            }
        )
        if len(items) >= limit:
            break

    return items
