import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from ..config import BACKOFF_SECONDS, MAX_RETRIES, NCBI_API_KEY, NCBI_BASE, NCBI_EMAIL, NCBI_TOOL, REQUEST_TIMEOUT

PUBMED_EFETCH_BATCH_SIZE = 200

MONTH_MAP = {
    "jan": "01",
    "feb": "02",
    "mar": "03",
    "apr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "aug": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dec": "12",
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": f"{NCBI_TOOL}/1.0 ({NCBI_EMAIL})"})
    return s


def _request_with_retry(url: str, params: dict[str, Any], session: requests.Session | None = None) -> requests.Response:
    sess = session or _session()
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = sess.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else BACKOFF_SECONDS * attempt
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"Failed request to {url}: {last_exc}")


def search(query: str, days_back: int = 14, retmax: int = 200) -> list[str]:
    if not query.strip() or retmax <= 0:
        return []

    now = datetime.now(timezone.utc)
    mindate = (now - timedelta(days=days_back)).strftime("%Y/%m/%d")
    maxdate = now.strftime("%Y/%m/%d")
    all_ids: list[str] = []
    retstart = 0
    session = _session()

    while len(all_ids) < retmax:
        step = min(200, retmax - len(all_ids))
        params: dict[str, Any] = {
            "db": "pubmed",
            "term": query,
            "retmode": "json",
            "retmax": step,
            "retstart": retstart,
            "datetype": "pdat",
            "mindate": mindate,
            "maxdate": maxdate,
            "sort": "pub date",
            "tool": NCBI_TOOL,
            "email": NCBI_EMAIL,
        }
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY

        resp = _request_with_retry(f"{NCBI_BASE}/esearch.fcgi", params, session=session)
        payload = resp.json().get("esearchresult", {})
        idlist = payload.get("idlist", [])
        if not idlist:
            break

        all_ids.extend(idlist)
        retstart += len(idlist)
        count = int(payload.get("count", 0) or 0)
        if retstart >= count:
            break

    seen: set[str] = set()
    deduped: list[str] = []
    for pmid in all_ids:
        if pmid in seen:
            continue
        seen.add(pmid)
        deduped.append(pmid)
    return deduped[:retmax]


def _extract_abstract(article: ET.Element) -> str:
    sections = article.findall(".//Abstract/AbstractText")
    parts: list[str] = []
    for sec in sections:
        label = sec.attrib.get("Label")
        text = "".join(sec.itertext()).strip()
        if not text:
            continue
        parts.append(f"{label}: {text}" if label else text)
    return " ".join(parts).strip()


def _extract_authors(article: ET.Element) -> str:
    authors = []
    for auth in article.findall(".//Author"):
        last = (auth.findtext("LastName") or "").strip()
        ini = (auth.findtext("Initials") or "").strip()
        coll = (auth.findtext("CollectiveName") or "").strip()
        if coll:
            authors.append(coll)
        elif last:
            authors.append(f"{last} {ini}".strip())
    return ", ".join(authors[:8])


def _extract_doi(article: ET.Element) -> str | None:
    for eloc in article.findall(".//ELocationID"):
        if eloc.attrib.get("EIdType", "").lower() == "doi":
            val = (eloc.text or "").strip()
            if val:
                return val
    for aid in article.findall(".//ArticleId"):
        if aid.attrib.get("IdType", "").lower() == "doi":
            val = (aid.text or "").strip()
            if val:
                return val
    return None


def _normalize_month(month: str | None) -> str | None:
    if not month:
        return None
    m = month.strip()
    if not m:
        return None
    if m.isdigit():
        val = int(m)
        if 1 <= val <= 12:
            return f"{val:02d}"
        return None
    return MONTH_MAP.get(m[:3].lower())


def _extract_pub_date(article: ET.Element) -> str | None:
    year = article.findtext(".//PubDate/Year")
    month = _normalize_month(article.findtext(".//PubDate/Month"))
    day_raw = article.findtext(".//PubDate/Day")
    day = day_raw.zfill(2) if day_raw and day_raw.isdigit() else None
    medline = article.findtext(".//PubDate/MedlineDate")
    if year:
        parts = [year.strip()]
        if month:
            parts.append(month)
        if day:
            parts.append(day)
        return "-".join(parts)
    return medline.strip() if medline else None


def parse_pubmed_xml(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    out: list[dict[str, Any]] = []
    for node in root.findall(".//PubmedArticle"):
        pmid = (node.findtext(".//PMID") or "").strip()
        article = node.find(".//Article")
        if article is None:
            continue

        title = " ".join("".join(article.find("ArticleTitle").itertext()).split()) if article.find("ArticleTitle") is not None else ""
        abstract = _extract_abstract(article)
        journal = (article.findtext(".//Journal/ISOAbbreviation") or article.findtext(".//Journal/Title") or "").strip()
        doi = _extract_doi(node)
        pub_date = _extract_pub_date(article)
        authors = _extract_authors(node)

        out.append(
            {
                "source": "pubmed",
                "title": title,
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else None,
                "published_at": pub_date,
                "updated_at": None,
                "pmid": pmid or None,
                "doi": doi,
                "nct_id": _extract_nct_id(abstract),
                "venue": journal,
                "authors": authors,
                "abstract_or_text": abstract,
            }
        )
    return out


def _extract_nct_id(text: str | None) -> str | None:
    if not text:
        return None
    m = re.search(r"\bNCT\d{8}\b", text)
    return m.group(0) if m else None


def fetch(pmids: list[str]) -> list[dict[str, Any]]:
    if not pmids:
        return []
    session = _session()
    parsed: list[dict[str, Any]] = []
    for i in range(0, len(pmids), PUBMED_EFETCH_BATCH_SIZE):
        batch = pmids[i : i + PUBMED_EFETCH_BATCH_SIZE]
        params: dict[str, Any] = {
            "db": "pubmed",
            "retmode": "xml",
            "id": ",".join(batch),
            "tool": NCBI_TOOL,
            "email": NCBI_EMAIL,
        }
        if NCBI_API_KEY:
            params["api_key"] = NCBI_API_KEY
        resp = _request_with_retry(f"{NCBI_BASE}/efetch.fcgi", params, session=session)
        parsed.extend(parse_pubmed_xml(resp.text))
    return parsed
