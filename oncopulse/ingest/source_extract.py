import json
import re
from typing import Optional

import requests

from ..config import REQUEST_TIMEOUT
from ..text_utils import clean_text


def _extract_meta(content: str, name: str) -> str | None:
    patterns = [
        rf'<meta[^>]+name=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+property=["\']{re.escape(name)}["\'][^>]+content=["\']([^"\']+)["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']{re.escape(name)}["\']',
        rf'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']{re.escape(name)}["\']',
    ]
    for p in patterns:
        m = re.search(p, content, re.IGNORECASE)
        if m:
            val = clean_text(m.group(1))
            if len(val) >= 40:
                return val
    return None


def _extract_jsonld_description(content: str) -> str | None:
    scripts = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', content, re.IGNORECASE | re.DOTALL)
    for raw in scripts:
        txt = raw.strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        stack = [data]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                desc = node.get("description")
                if isinstance(desc, str):
                    val = clean_text(desc)
                    if len(val) >= 40:
                        return val
                for v in node.values():
                    stack.append(v)
            elif isinstance(node, list):
                stack.extend(node)
    return None


def extract_abstract_from_url(url: str | None) -> Optional[str]:
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": "OncoPulse/1.0"})
        if resp.status_code >= 400:
            return None
        html = resp.text
    except Exception:
        return None

    for key in ["citation_abstract", "dc.description", "og:description", "description", "twitter:description"]:
        val = _extract_meta(html, key)
        if val:
            return val

    val = _extract_jsonld_description(html)
    if val:
        return val

    return None
