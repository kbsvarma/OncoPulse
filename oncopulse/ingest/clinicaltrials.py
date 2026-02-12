from typing import Any
import time

import requests

from ..config import BACKOFF_SECONDS, CTGOV_V2_BASE, MAX_RETRIES, REQUEST_TIMEOUT


def _safe_get(obj: dict[str, Any], *path: str, default: Any = None) -> Any:
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


def _request_with_retry(params: dict[str, Any]) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(CTGOV_V2_BASE, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(BACKOFF_SECONDS * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(BACKOFF_SECONDS * attempt)
    raise RuntimeError(f"ClinicalTrials request failed: {last_exc}")


def search(query: str, limit: int = 100) -> list[dict[str, Any]]:
    if not query.strip() or limit <= 0:
        return []

    page_size = min(100, limit)
    token: str | None = None
    items: list[dict[str, Any]] = []

    while len(items) < limit:
        params = {
            "query.term": query,
            "pageSize": page_size,
            "sort": "LastUpdatePostDate:desc",
            "format": "json",
        }
        if token:
            params["pageToken"] = token
        payload = _request_with_retry(params)
        studies = payload.get("studies", []) or []

        for s in studies:
            protocol = s.get("protocolSection", {})
            ident = protocol.get("identificationModule", {})
            status = protocol.get("statusModule", {})
            cond_mod = protocol.get("conditionsModule", {})
            arms_mod = protocol.get("armsInterventionsModule", {})
            desc_mod = protocol.get("descriptionModule", {})
            design_mod = protocol.get("designModule", {})
            outcomes_mod = protocol.get("outcomesModule", {})

            nct_id = ident.get("nctId")
            title = ident.get("briefTitle") or ident.get("officialTitle") or "Untitled study"
            updated = _safe_get(status, "lastUpdatePostDateStruct", "date")

            interventions: list[str] = []
            for iv in arms_mod.get("interventions", []) or []:
                name = iv.get("name")
                if name:
                    interventions.append(name)

            primary_endpoints = []
            for po in outcomes_mod.get("primaryOutcomes", []) or []:
                measure = po.get("measure")
                if measure:
                    primary_endpoints.append(measure)

            text_parts = []
            for field in ("briefSummary", "detailedDescription"):
                val = desc_mod.get(field)
                if val:
                    text_parts.append(val)

            phase_list = design_mod.get("phases", []) or []
            study_type = design_mod.get("studyType")
            status_text = status.get("overallStatus")

            items.append(
                {
                    "source": "clinicaltrials",
                    "title": title,
                    "url": f"https://clinicaltrials.gov/study/{nct_id}" if nct_id else "https://clinicaltrials.gov",
                    "published_at": None,
                    "updated_at": updated,
                    "pmid": None,
                    "doi": None,
                    "nct_id": nct_id,
                    "venue": "ClinicalTrials.gov",
                    "authors": None,
                    "abstract_or_text": "\n".join(text_parts).strip(),
                    "conditions": ", ".join(cond_mod.get("conditions", []) or []),
                    "interventions": ", ".join(interventions),
                    "study_type": study_type,
                    "phase": ", ".join(phase_list),
                    "status": status_text,
                    "primary_endpoints": ", ".join(primary_endpoints),
                }
            )
            if len(items) >= limit:
                break

        if len(items) >= limit:
            break

        token = payload.get("nextPageToken")
        if not token:
            break

    return items
