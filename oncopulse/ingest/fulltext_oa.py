from datetime import datetime, timedelta, timezone
import re
import xml.etree.ElementTree as ET
from typing import Any

import requests

from .. import db
from ..config import (
    EUROPE_PMC_REST_BASE,
    FULLTEXT_CACHE_TTL_DAYS,
    NCBI_API_KEY,
    NCBI_BASE,
    PMC_IDCONV_BASE,
    PMC_OA_BASE,
    REQUEST_TIMEOUT,
)
from ..text_utils import clean_text


def _normalize_pmcid(pmcid: str | None) -> str | None:
    if not pmcid:
        return None
    m = re.search(r"(PMC\d+)", str(pmcid).upper())
    return m.group(1) if m else None


def _cache_key(item: dict[str, Any], pmcid: str | None) -> str | None:
    if pmcid:
        return f"pmcid:{pmcid.upper()}"
    doi = clean_text(item.get("doi"))
    if doi:
        return f"doi:{doi.lower()}"
    pmid = clean_text(item.get("pmid"))
    if pmid:
        return f"pmid:{pmid}"
    return None


def _is_cache_fresh(fetched_at: str | None, ttl_days: int = FULLTEXT_CACHE_TTL_DAYS) -> bool:
    if not fetched_at:
        return False
    try:
        dt = datetime.fromisoformat(str(fetched_at))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= (datetime.now(timezone.utc) - timedelta(days=ttl_days))
    except Exception:
        return False


def _resolve_pmcid(item: dict[str, Any]) -> str | None:
    existing = _normalize_pmcid(item.get("pmcid"))
    if existing:
        return existing
    ids: list[str] = []
    pmid = clean_text(item.get("pmid"))
    doi = clean_text(item.get("doi"))
    if pmid:
        ids.append(pmid)
    if doi:
        ids.append(doi)
    if not ids:
        return None
    try:
        resp = requests.get(
            PMC_IDCONV_BASE,
            params={"ids": ",".join(ids), "format": "json"},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        records = resp.json().get("records", []) or []
        for rec in records:
            pmcid = _normalize_pmcid(rec.get("pmcid"))
            if pmcid:
                return pmcid
    except Exception:
        return None
    return None


def _is_pmc_oa(pmcid: str) -> bool:
    try:
        resp = requests.get(PMC_OA_BASE, params={"id": pmcid}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return "<error" not in resp.text.lower()
    except Exception:
        return False


def _fetch_pmc_xml(pmcid: str) -> str | None:
    params: dict[str, Any] = {"db": "pmc", "id": pmcid, "retmode": "xml"}
    if NCBI_API_KEY:
        params["api_key"] = NCBI_API_KEY
    try:
        resp = requests.get(f"{NCBI_BASE}/efetch.fcgi", params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        text = resp.text
        if "<article" in text.lower():
            return text
    except Exception:
        return None
    return None


def _fetch_epmc_xml(pmcid: str) -> str | None:
    pmc_numeric = pmcid.replace("PMC", "")
    url = f"{EUROPE_PMC_REST_BASE}/PMC/{pmc_numeric}/fullTextXML"
    try:
        resp = requests.get(url, params={"format": "xml"}, timeout=REQUEST_TIMEOUT)
        if resp.status_code >= 400:
            return None
        text = resp.text
        if "<article" in text.lower():
            return text
    except Exception:
        return None
    return None


def _extract_paragraphs(root: ET.Element, xpath: str, limit: int = 120) -> list[str]:
    out: list[str] = []
    for node in root.findall(xpath):
        text = clean_text(" ".join(node.itertext()))
        if not text:
            continue
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _extract_labeled_sections(root: ET.Element) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {
        "abstract": _extract_paragraphs(root, ".//abstract//p", limit=20),
        "methods": [],
        "results": [],
        "discussion": [],
        "conclusion": [],
        "captions": [],
    }

    for sec in root.findall(".//body//sec"):
        title = clean_text(" ".join((sec.findtext("title") or "").split())).lower()
        text_blocks = [clean_text(" ".join(p.itertext())) for p in sec.findall(".//p")]
        text_blocks = [t for t in text_blocks if t]
        if not text_blocks:
            continue
        joined = " ".join(text_blocks[:5]).lower()
        bucket = None
        if any(k in title or k in joined for k in ["method", "materials", "patients and methods"]):
            bucket = "methods"
        elif any(k in title or k in joined for k in ["result", "efficacy"]):
            bucket = "results"
        elif any(k in title or k in joined for k in ["discussion"]):
            bucket = "discussion"
        elif any(k in title or k in joined for k in ["conclusion", "concluding"]):
            bucket = "conclusion"
        if bucket:
            sections[bucket].extend(text_blocks[:20])

    for cap in root.findall(".//fig//caption") + root.findall(".//table-wrap//caption"):
        cap_text = clean_text(" ".join(cap.itertext()))
        if cap_text:
            sections["captions"].append(cap_text)
            if len(sections["captions"]) >= 20:
                break

    return sections


def _sections_to_text(sections: dict[str, list[str]]) -> str:
    ordered = ["abstract", "methods", "results", "discussion", "conclusion", "captions"]
    parts: list[str] = []
    for name in ordered:
        vals = sections.get(name) or []
        if not vals:
            continue
        parts.extend(vals[:15])
    return clean_text(" ".join(parts))


def _support_snippets(sections: dict[str, list[str]], max_items: int = 5) -> list[str]:
    out: list[str] = []
    for section_name in ["results", "conclusion", "discussion", "methods", "abstract", "captions"]:
        for txt in sections.get(section_name, []):
            if len(txt) < 40:
                continue
            out.append(txt)
            if len(out) >= max_items:
                return out
    return out


def enrich_item_from_oa_full_text(conn, item: dict[str, Any], ttl_days: int = FULLTEXT_CACHE_TTL_DAYS) -> None:
    pmcid = _resolve_pmcid(item)
    key = _cache_key(item, pmcid)
    if not key:
        return

    cached = db.get_cached_full_text(conn, key)
    if cached and _is_cache_fresh(cached.get("fetched_at"), ttl_days=ttl_days):
        sections = cached.get("sections") or {}
        item["full_text_source"] = cached.get("source")
        item["full_text_sections"] = sections
        item["full_text_text"] = _sections_to_text(sections)
        item["support_snippets"] = _support_snippets(sections)
        return

    if not pmcid:
        return

    xml_text = None
    source = None
    if _is_pmc_oa(pmcid):
        xml_text = _fetch_pmc_xml(pmcid)
        source = "PMC" if xml_text else None

    if not xml_text:
        xml_text = _fetch_epmc_xml(pmcid)
        source = "Europe PMC" if xml_text else None

    if not xml_text or not source:
        return

    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return

    sections = _extract_labeled_sections(root)
    full_text_text = _sections_to_text(sections)
    if not full_text_text:
        return

    item["full_text_source"] = source
    item["full_text_sections"] = sections
    item["full_text_text"] = full_text_text
    item["support_snippets"] = _support_snippets(sections)
    db.set_cached_full_text(
        conn,
        key,
        source=source,
        sections=sections,
        pmid=clean_text(item.get("pmid")) or None,
        doi=clean_text(item.get("doi")) or None,
        pmcid=pmcid,
        xml_text=xml_text,
    )
