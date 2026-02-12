import hashlib
import re
from datetime import datetime
from typing import Any


def normalize_title(title: str) -> str:
    t = title.lower().strip()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t


def year_bucket(date_text: str | None) -> str:
    if not date_text:
        return "unknown"
    m = re.search(r"(19|20)\d{2}", date_text)
    return m.group(0) if m else "unknown"


def fingerprint_item(item: dict[str, Any]) -> str:
    for key in ("doi", "pmid", "nct_id"):
        value = (item.get(key) or "").strip().lower()
        if value:
            return f"{key}:{value}"

    tnorm = normalize_title(item.get("title", ""))
    y = year_bucket(item.get("published_at") or item.get("updated_at"))
    raw = f"title:{tnorm}|year:{y}"
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"titleyear:{h}"


def deduplicate(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []

    for item in items:
        fp = item.get("fingerprint") or fingerprint_item(item)
        item["fingerprint"] = fp
        if fp in seen:
            continue
        seen.add(fp)
        result.append(item)

    return result
