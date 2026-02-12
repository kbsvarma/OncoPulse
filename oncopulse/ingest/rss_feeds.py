import email.utils
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

import requests

from ..config import JOURNAL_RSS_FEEDS, REQUEST_TIMEOUT


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


def _parse_rss_date(date_text: str | None) -> str | None:
    if not date_text:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(date_text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.date().isoformat()
    except Exception:  # noqa: BLE001
        return None


def search(query: str, limit: int = 100) -> list[dict[str, Any]]:
    terms = _terms_from_query(query)
    items: list[dict[str, Any]] = []

    for feed_url in JOURNAL_RSS_FEEDS:
        try:
            resp = requests.get(feed_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
        except Exception:  # noqa: BLE001
            continue

        for node in root.findall(".//item"):
            title = (node.findtext("title") or "").strip()
            link = (node.findtext("link") or "").strip()
            desc = (node.findtext("description") or "").strip()
            pub_date = _parse_rss_date(node.findtext("pubDate"))
            source = (node.findtext("source") or "RSS Journal").strip()

            blob = f"{title} {desc}".lower()
            if terms and not any(t in blob for t in terms):
                continue

            items.append(
                {
                    "source": "journal_rss",
                    "title": title or "Untitled journal item",
                    "url": link or feed_url,
                    "published_at": pub_date,
                    "updated_at": None,
                    "pmid": None,
                    "doi": None,
                    "nct_id": None,
                    "venue": source,
                    "authors": None,
                    "abstract_or_text": desc,
                }
            )
            if len(items) >= limit:
                return items

    return items[:limit]
