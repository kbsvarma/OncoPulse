import time
from typing import Optional

import requests

from ..config import (
    BACKOFF_SECONDS,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    SEMANTIC_SCHOLAR_API_KEY,
    SEMANTIC_SCHOLAR_BASE,
)


def get_citations_by_pmid(pmid: str | None) -> Optional[int]:
    if not pmid:
        return None
    pmid_clean = str(pmid).strip()
    if not pmid_clean:
        return None

    headers = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    url = f"{SEMANTIC_SCHOLAR_BASE}/PMID:{pmid_clean}"
    params = {"fields": "citationCount"}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            resp.raise_for_status()
            count = resp.json().get("citationCount")
            return int(count) if isinstance(count, int) else None
        except Exception:  # noqa: BLE001
            time.sleep(BACKOFF_SECONDS * attempt)

    return None
