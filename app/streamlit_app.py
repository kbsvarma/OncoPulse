import inspect
import io
import json
import csv
from datetime import datetime, timedelta, timezone
import re
import time

import streamlit as st

from oncopulse import db, nlp, packs
from oncopulse.extract_fields import detect_endpoints, detect_phase, detect_sample_size, detect_study_type
from oncopulse.services.run_pipeline import (
    MODE_ALL,
    MODE_CLINICIAN,
    MODE_OPTIONS,
    RunOptions,
    build_sources_key,
    get_mode_preset,
    resolve_incremental_days_back,
    run_pipeline,
    run_pipeline_query,
)
from oncopulse.scoring import citations_per_year, hot_score
from oncopulse.text_utils import clean_text


st.set_page_config(page_title="OncoPulse", layout="wide")

conn = db.get_conn()
db.init_db(conn)

if "selected_mode" not in st.session_state:
    st.session_state["selected_mode"] = MODE_ALL
if "new_custom_name" not in st.session_state:
    st.session_state["new_custom_name"] = ""
if "clear_notice" not in st.session_state:
    st.session_state["clear_notice"] = ""
if "run_feedback" not in st.session_state:
    st.session_state["run_feedback"] = None
if "search_diagnostics" not in st.session_state:
    st.session_state["search_diagnostics"] = {}

header_l, header_r = st.columns([5, 1])
with header_l:
    st.title("OncoPulse - Oncology Research Inbox")
with header_r:
    with st.popover("Run settings"):
        default_full_text = bool(get_mode_preset(st.session_state.get("selected_mode", MODE_ALL)).get("use_full_text_oa", False))
        use_full_text_setting = st.toggle(
            "Use full text when available (PMC/Europe PMC OA)",
            value=bool(st.session_state.get("run_use_full_text_oa", default_full_text)),
            key="run_use_full_text_oa",
        )
        llm_polish_summary = st.toggle(
            "LLM polish summary (strict, citation-backed)",
            value=False,
            help="Optional readability rewrite with strict evidence/numeric guardrails.",
        )
        fast_mode = st.toggle("Fast mode", value=False)
        enrich_citations = st.toggle("Enrich citations", value=False)
        enable_semantic_scholar = st.toggle(
            "Semantic Scholar fallback (PMID)",
            value=False,
            help="Used only when DOI is missing and citation enrichment is enabled.",
            disabled=not enrich_citations,
        )
        force_full_refresh = st.toggle("Force full refresh", value=False, help="Ignore incremental window and use full selected time window.")
        st.caption("Maintenance")
        confirm_clear_cache = st.checkbox("Confirm clear local cache", value=False)
        if st.button("Clear local cache", disabled=not confirm_clear_cache):
            db.clear_all_local_cache(conn)
            st.session_state["has_run_once"] = False
            st.session_state["last_search_key"] = ""
            st.success("Local cache cleared.")
            st.rerun()
        st.caption("Mode profiles")
        with st.expander("Manage mode profiles", expanded=False):
            built_in_view = st.selectbox("View built-in profile", MODE_OPTIONS, key="rs_builtin_view")
            st.json(get_mode_preset(built_in_view))

            custom_profiles_rs = db.list_custom_mode_profiles(conn)
            st.caption(f"Custom profiles saved: {len(custom_profiles_rs)}/3")
            if custom_profiles_rs:
                chosen_custom = st.selectbox(
                    "View custom profile",
                    [p["name"] for p in custom_profiles_rs],
                    key="rs_custom_view_name",
                )
                selected_profile = next((p for p in custom_profiles_rs if p["name"] == chosen_custom), None)
                if selected_profile:
                    st.json(selected_profile.get("config", {}))
            else:
                st.caption("No custom profiles saved yet.")

            st.caption("Create or update custom profile")
            custom_name_rs = st.text_input(
                "Custom profile name",
                value=st.session_state.get("new_custom_name", ""),
                key="rs_new_custom_name",
                placeholder="e.g., Clinic Fast Track",
            )
            base_mode_rs = st.selectbox("Base mode", MODE_OPTIONS, index=MODE_OPTIONS.index(MODE_CLINICIAN), key="rs_custom_base_mode")
            base_rs = get_mode_preset(base_mode_rs)

            r1, r2, r3 = st.columns(3)
            with r1:
                rs_include_papers = st.checkbox("Include papers", value=bool(base_rs.get("include_papers", True)), key="rs_inc_papers")
                rs_include_trials = st.checkbox("Include trials", value=bool(base_rs.get("include_trials", True)), key="rs_inc_trials")
            with r2:
                rs_include_preprints = st.checkbox("Include preprints", value=bool(base_rs.get("include_preprints", False)), key="rs_inc_preprints")
                rs_include_rss = st.checkbox("Include journal RSS", value=bool(base_rs.get("include_journal_rss", False)), key="rs_inc_rss")
            with r3:
                rs_include_fda = st.checkbox("Include FDA approvals", value=bool(base_rs.get("include_fda_approvals", False)), key="rs_inc_fda")
                rs_phase = st.checkbox("Phase II/III only", value=bool(base_rs.get("phase_2_3_only", False)), key="rs_phase")
                rs_rct = st.checkbox("RCT/meta only", value=bool(base_rs.get("rct_meta_only", False)), key="rs_rct")
                rs_use_full_text_oa = st.checkbox(
                    "Use full text when available (PMC/Europe PMC OA)",
                    value=bool(base_rs.get("use_full_text_oa", False)),
                    key="rs_use_full_text_oa",
                )

            sw_base = dict(base_rs.get("scoring_weights", {}))
            s1, s2, s3 = st.columns(3)
            with s1:
                rs_w_phase3 = st.slider("Phase III", 0, 15, int(sw_base.get("phase_iii", 10)), key="rs_w_phase3")
                rs_w_rct = st.slider("RCT", 0, 15, int(sw_base.get("randomized", 8)), key="rs_w_rct")
            with s2:
                rs_w_os = st.slider("OS", 0, 12, int(sw_base.get("overall_survival", 5)), key="rs_w_os")
                rs_w_pfs = st.slider("PFS", 0, 12, int(sw_base.get("progression_free_survival", 4)), key="rs_w_pfs")
            with s3:
                rs_w_meta = st.slider("Meta-analysis", 0, 15, int(sw_base.get("meta_analysis", 5)), key="rs_w_meta")
                rs_w_cite = st.slider("Citations multiplier", 0.0, 3.0, float(sw_base.get("citations_multiplier", 1.0)), 0.1, key="rs_w_cite")

            if st.button("Save custom profile", key="rs_save_custom_profile"):
                name = (custom_name_rs or "").strip()
                if not name:
                    st.error("Enter a custom profile name.")
                elif len(custom_profiles_rs) >= 3 and name not in [p["name"] for p in custom_profiles_rs]:
                    st.error("Maximum 3 custom profiles allowed.")
                else:
                    cfg = {
                        "include_papers": rs_include_papers,
                        "include_trials": rs_include_trials,
                        "include_preprints": rs_include_preprints,
                        "include_journal_rss": rs_include_rss,
                        "include_fda_approvals": rs_include_fda,
                        "phase_2_3_only": rs_phase,
                        "rct_meta_only": rs_rct,
                        "use_full_text_oa": rs_use_full_text_oa,
                        "scoring_weights": {
                            "phase_iii": rs_w_phase3,
                            "randomized": rs_w_rct,
                            "overall_survival": rs_w_os,
                            "progression_free_survival": rs_w_pfs,
                            "meta_analysis": rs_w_meta,
                            "citations_multiplier": rs_w_cite,
                        },
                    }
                    db.upsert_custom_mode_profile(conn, name, cfg)
                    st.session_state["selected_mode"] = f"Custom: {name}"
                    st.session_state["new_custom_name"] = name
                    st.success(f"Saved custom profile: {name}")
                    st.rerun()

            if custom_profiles_rs:
                del_name = st.selectbox("Delete custom profile", [p["name"] for p in custom_profiles_rs], key="rs_delete_name")
                if st.button("Delete selected profile", key="rs_delete_custom_profile"):
                    db.delete_custom_mode_profile(conn, del_name)
                    if st.session_state.get("selected_mode") == f"Custom: {del_name}":
                        st.session_state["selected_mode"] = MODE_ALL
                    st.success(f"Deleted custom profile: {del_name}")
                    st.rerun()

if "has_run_once" not in st.session_state:
    st.session_state["has_run_once"] = False
if "last_search_key" not in st.session_state:
    st.session_state["last_search_key"] = ""

specialties = packs.list_specialties()
if not specialties:
    st.error("No packs found in /packs")
    st.stop()

SPECIALTY_LABELS = {
    "lung": "Lung",
    "breast": "Breast",
    "heme": "Hematologic Malignancies",
    "gi": "Gastrointestinal",
    "gu": "Genitourinary",
    "gyn": "Gynecologic",
    "cns": "CNS / Neuro-Oncology",
    "headneck": "Head & Neck",
    "melanoma": "Melanoma",
    "sarcoma": "Sarcoma",
    "pediatric": "Pediatric Oncology",
    "supportivecare": "Supportive Care",
    "general": "General Oncology",
}

SPECIALTY_GROUPS = {
    "Thoracic": {"lung"},
    "Breast": {"breast"},
    "Hematology": {"heme"},
    "Gastrointestinal": {"gi"},
    "Genitourinary": {"gu"},
    "Gynecologic": {"gyn"},
    "CNS": {"cns"},
    "Head & Neck": {"headneck"},
    "Melanoma / Skin": {"melanoma"},
    "Sarcoma": {"sarcoma"},
    "Pediatric": {"pediatric"},
    "Supportive / Survivorship": {"supportivecare"},
    "Cross-Disease": {"general"},
}


def _pretty_specialty(slug: str) -> str:
    if slug in SPECIALTY_LABELS:
        return SPECIALTY_LABELS[slug]
    return slug.replace("_", " ").replace("-", " ").title()


def _group_for_specialty(slug: str) -> str:
    for group_name, members in SPECIALTY_GROUPS.items():
        if slug in members:
            return group_name
    return "Other"


def _query_key(query: str) -> str:
    return f"query:{query.strip().lower()[:180]}"


def _refresh_results_view() -> None:
    st.session_state["_results_refresh_nonce"] = time.time()


def _parse_date(date_text: str | None) -> datetime | None:
    if not date_text:
        return None
    s = str(date_text).strip()
    if not s:
        return None

    for fmt in ("%Y-%m-%d", "%Y-%b-%d", "%Y-%B-%d", "%Y-%m", "%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _item_datetime(item: dict) -> datetime | None:
    return _parse_date(item.get("published_at")) or _parse_date(item.get("updated_at"))


def _is_within_window(item: dict, days: int) -> bool:
    dt = _item_datetime(item)
    if dt is None:
        return False
    return dt >= (datetime.now(timezone.utc) - timedelta(days=days))


def _result_filter(items: list[dict], phase_only: bool, rct_meta_only: bool) -> list[dict]:
    out: list[dict] = []
    for i in items:
        text = f"{i.get('title', '')} {i.get('abstract_or_text', '')}".lower()
        if phase_only and not any(t in text for t in ["phase ii", "phase 2", "phase iii", "phase 3"]):
            continue
        if rct_meta_only and not any(t in text for t in ["randomized", "rct", "meta-analysis", "systematic review"]):
            continue
        out.append(i)
    return out


def _evidence_label(item: dict) -> str:
    source = (item.get("source") or "").lower()
    if source == "preprint":
        return "Preprint"
    if source == "clinicaltrials":
        return "Trial registry"
    if source == "fda":
        return "Regulatory"
    if source == "journal_rss":
        return "Journal alert"
    if source == "guideline":
        return "Guideline"
    return "Peer-reviewed"


def _evidence_badge_html(label: str) -> str:
    styles = {
        "Peer-reviewed": ("#14532d", "#dcfce7"),
        "Preprint": ("#7c2d12", "#ffedd5"),
        "Trial registry": ("#1e3a8a", "#dbeafe"),
        "Regulatory": ("#7f1d1d", "#fee2e2"),
        "Journal alert": ("#3f3f46", "#f4f4f5"),
        "Guideline": ("#5b21b6", "#ede9fe"),
    }
    bg, fg = styles.get(label, ("#334155", "#f8fafc"))
    return (
        f"<span style='display:inline-block;padding:4px 10px;margin:2px 0;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:0.78rem;font-weight:600;'>Evidence: {label}</span>"
    )


def _confidence_label(item: dict) -> str:
    if item.get("full_text_source"):
        return "Full-text"
    source = (item.get("source") or "").lower()
    has_text = bool(clean_text(item.get("abstract_or_text")))
    has_lit_id = bool(item.get("pmid") or item.get("doi"))
    is_registry = source in {"clinicaltrials", "fda"}

    if not has_text:
        return "No abstract available"
    if is_registry and has_lit_id:
        return "Mixed (abstract + registry)"
    if is_registry:
        return "Registry-only"
    return "Abstract-only"


def _confidence_badge_html(label: str) -> str:
    styles = {
        "Full-text": ("#0c4a6e", "#e0f2fe"),
        "Abstract-only": ("#0f766e", "#ccfbf1"),
        "Registry-only": ("#1e3a8a", "#dbeafe"),
        "Mixed (abstract + registry)": ("#7c2d12", "#ffedd5"),
        "No abstract available": ("#52525b", "#f4f4f5"),
    }
    bg, fg = styles.get(label, ("#334155", "#f8fafc"))
    return (
        f"<span style='display:inline-block;padding:4px 10px;margin:2px 0;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:0.78rem;font-weight:600;'>Confidence: {label}</span>"
    )


def _score_badges(explain: list[str]) -> list[str]:
    badges: list[str] = []
    for x in explain:
        lx = x.lower()
        if "phase iii" in lx or "phase 3" in lx:
            badges.append("Phase III")
        elif "phase ii" in lx or "phase 2" in lx:
            badges.append("Phase II")
        elif "randomized" in lx or "rct" in lx:
            badges.append("RCT")
        elif "meta-analysis" in lx or "systematic review" in lx:
            badges.append("Meta-analysis")
        elif "overall survival" in lx:
            badges.append("OS")
        elif "progression-free survival" in lx:
            badges.append("PFS")
        elif "sample size" in lx:
            badges.append("N>=200")
        elif "major journal" in lx:
            badges.append("Major journal")
        elif "citations bonus" in lx:
            badges.append("Citations")
        elif "preclinical signal" in lx:
            badges.append("Preclinical penalty")
        elif "case report" in lx:
            badges.append("Case report penalty")
        elif "query phrase" in lx:
            badges.append("Query phrase")
        elif "query concept" in lx:
            badges.append("Concept match")
        elif "query keyword" in lx or "query coverage" in lx:
            badges.append("Keyword match")
    # Keep unique order.
    seen: set[str] = set()
    unique: list[str] = []
    for b in badges:
        if b in seen:
            continue
        seen.add(b)
        unique.append(b)
    return unique

def _query_term_in_blob(blob: str, term: str) -> bool:
    t = (term or "").strip().lower()
    if not t:
        return False
    if t.isalnum():
        return re.search(rf"\b{re.escape(t)}\b", blob) is not None
    return t in blob


def _build_search_match_context(query: str) -> dict:
    q = (query or "").strip()
    if not q:
        return {"raw_query": "", "keywords": [], "concepts": []}
    bundle = nlp.build_search_queries(q)
    return {
        "raw_query": q,
        "keywords": list(bundle.get("keywords") or []),
        "concepts": list(bundle.get("concepts") or []),
    }


def _search_match_for_item(item: dict, ctx: dict | None) -> dict | None:
    if not ctx:
        return None
    raw_query = str(ctx.get("raw_query") or "").strip().lower()
    keywords = [str(k).strip().lower() for k in (ctx.get("keywords") or []) if str(k).strip()]
    concepts = [g for g in (ctx.get("concepts") or []) if isinstance(g, list)]

    blob = " ".join(
        str(item.get(k) or "")
        for k in ["title", "abstract_or_text", "conditions", "interventions", "primary_endpoints", "study_type", "phase"]
    ).lower()

    exact_hit = bool(raw_query and len(raw_query) >= 4 and raw_query in blob)
    concept_hits = 0
    matched_terms: list[str] = []
    for group in concepts:
        gterms = [str(t).strip().lower() for t in group if str(t).strip()]
        if gterms and any(_query_term_in_blob(blob, t) for t in gterms):
            concept_hits += 1
            matched_terms.append(gterms[0])

    keyword_hits = [k for k in keywords if _query_term_in_blob(blob, k)]
    keyword_unique = list(dict.fromkeys(keyword_hits))
    matched_terms.extend(keyword_unique)
    matched_terms = list(dict.fromkeys(matched_terms))[:5]

    score = 0
    if exact_hit:
        score += 45
    if concepts:
        score += min(35, int(35 * (concept_hits / max(1, len(concepts)))))
    else:
        score += min(20, concept_hits * 10)
    if keywords:
        score += min(20, int(20 * (len(keyword_unique) / max(1, len(set(keywords))))))

    if score >= 70:
        level = "High"
        color = "#166534"
        text = "#dcfce7"
    elif score >= 40:
        level = "Medium"
        color = "#1e3a8a"
        text = "#dbeafe"
    else:
        level = "Low"
        color = "#7c2d12"
        text = "#ffedd5"

    reason = f"Matched: {', '.join(matched_terms)}" if matched_terms else "Matched weakly: low keyword/concept overlap"
    return {
        "score": max(0, min(100, score)),
        "level": level,
        "color": color,
        "text": text,
        "reason": reason,
    }


def _split_sentences(text: str) -> list[str]:
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", text) if s and len(s.strip()) >= 20]


def _snippet_terms(explain: list[str]) -> list[str]:
    terms = ["overall survival", "progression-free survival", "randomized", "rct", "phase", "meta-analysis"]
    joined = " ".join(explain).lower()
    if "toxicity" in joined:
        terms.append("toxicity")
    if "adverse" in joined:
        terms.append("adverse")
    return terms


def _pick_source_snippets(item: dict, explain: list[str], max_snippets: int = 3) -> list[str]:
    raw_snips = item.get("support_snippets_json")
    if raw_snips:
        try:
            parsed = json.loads(raw_snips) if isinstance(raw_snips, str) else raw_snips
            if isinstance(parsed, list):
                cleaned = [clean_text(s) for s in parsed if clean_text(s)]
                if cleaned:
                    return cleaned[:max_snippets]
        except Exception:
            pass
    support_snips = item.get("support_snippets")
    if isinstance(support_snips, list):
        cleaned = [clean_text(s) for s in support_snips if clean_text(s)]
        if cleaned:
            return cleaned[:max_snippets]

    text = clean_text(item.get("abstract_or_text"))
    if not text:
        return []
    sents = _split_sentences(text)
    if not sents:
        return []

    terms = _snippet_terms(explain)
    scored: list[tuple[int, int, str]] = []
    for idx, sent in enumerate(sents):
        ls = sent.lower()
        score = 0
        if any(t in ls for t in terms):
            score += 3
        if re.search(r"\b\d+(?:\.\d+)?%?\b", sent):
            score += 2
        if 60 <= len(sent) <= 320:
            score += 1
        scored.append((score, idx, sent))

    scored.sort(key=lambda x: (-x[0], x[1]))
    picked = [s for sc, _, s in scored if sc > 0][:max_snippets]
    if picked:
        return picked
    return sents[:max_snippets]


def _summary_field(summary_text: str, label: str) -> str:
    lines = [clean_text(ln) for ln in str(summary_text or "").splitlines() if clean_text(ln)]
    prefix = f"{label.lower()}:"
    for line in lines:
        if line.lower().startswith(prefix):
            return line.split(":", 1)[1].strip()
    return "Not stated"


def _full_text_badge_html(source: str | None) -> str:
    if not source:
        return ""
    label = f"Full text: {source}"
    return (
        "<span style='display:inline-block;padding:4px 10px;margin:2px 0;border-radius:999px;"
        "background:#164e63;color:#ecfeff;font-size:0.78rem;font-weight:600;'>"
        f"{label}</span>"
    )


def _preview_sentences(text: str, max_sentences: int = 3) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", cleaned) if s.strip()]
    return " ".join(sents[:max_sentences])


def _summary_basis_text(item: dict) -> str:
    if item.get("full_text_source"):
        return f"Summary basis: Full text ({item.get('full_text_source')})"
    source = (item.get("source") or "").lower()
    has_abstract = bool(clean_text(item.get("abstract_or_text")))
    if source in {"clinicaltrials", "fda"} and not has_abstract:
        return "Summary basis: Registry record only"
    if has_abstract:
        return "Summary basis: Abstract only"
    return "Summary basis: Not enough source text"


def render_top_card(item: dict, search_match_ctx: dict | None = None):
    try:
        explain = json.loads(item.get("score_explain_json") or "[]")
    except Exception:
        explain = []
    badges = _score_badges(explain)
    summary = str(item.get("summary_text") or "")
    study_type = _summary_field(summary, "Study type / phase")
    endpoints = _summary_field(summary, "Endpoints mentioned")
    key_finding = _summary_field(summary, "Key finding")
    why_matters = _summary_field(summary, "Why it matters")
    abstract_preview = _preview_sentences(item.get("abstract_or_text"), max_sentences=2)

    with st.container(border=True):
        evidence = _evidence_label(item)
        confidence = _confidence_label(item)
        c_val = item.get("citations")
        c_rate = citations_per_year(item)
        c_text = f"{c_val}" if c_val is not None else "N/A"
        if c_rate is not None:
            c_text += f" ({c_rate}/yr)"
        st.markdown(f"### {item.get('title')}")
        st.caption(
            f"Date: {item.get('published_at') or item.get('updated_at') or 'N/A'} | "
            f"Source: {item.get('source')} | "
            f"Score: {item.get('score', 0)} | "
            f"Citations: {c_text}"
        )
        st.markdown(_evidence_badge_html(evidence), unsafe_allow_html=True)
        st.markdown(_confidence_badge_html(confidence), unsafe_allow_html=True)
        if item.get("full_text_source"):
            st.markdown(_full_text_badge_html(item.get("full_text_source")), unsafe_allow_html=True)
        if item.get("url"):
            st.markdown(f"[Open source link]({item['url']})")

        search_match = _search_match_for_item(item, search_match_ctx)
        if search_match:
            st.markdown(
                f"<span style='display:inline-block;padding:4px 10px;margin:2px 0;border-radius:999px;background:{search_match['color']};color:{search_match['text']};font-size:0.78rem;font-weight:600;'>Search match: {search_match['level']} ({search_match['score']}%)</span>",
                unsafe_allow_html=True,
            )
            st.progress(int(search_match["score"]))
            st.caption(search_match["reason"])

        if badges:
            badge_html = " ".join(
                f"<span style='display:inline-block;padding:4px 10px;margin:2px;border-radius:999px;background:#1e293b;color:#e2e8f0;font-size:0.80rem;'>{b}</span>"
                for b in badges
            )
            st.markdown(badge_html, unsafe_allow_html=True)
        st.caption("Why ranked: " + (", ".join(explain) if explain else "No rule matches"))
        st.caption(_summary_basis_text(item))
        if abstract_preview:
            st.markdown(f"**Abstract preview:** {abstract_preview}")
        st.markdown(f"**Study type / phase:** {study_type}")
        st.markdown(f"**Endpoints:** {endpoints}")
        st.markdown(f"**Summary:** {key_finding}")
        st.markdown(f"**Why it matters:** {why_matters}")


def _identifier_blob(item: dict) -> str:
    doi = item.get("doi") or "-"
    pmid = item.get("pmid") or "-"
    nct = item.get("nct_id") or "-"
    return f"DOI:{doi} | PMID:{pmid} | NCT:{nct}"


def _table_row(item: dict) -> dict:
    text = f"{item.get('title', '')} {item.get('abstract_or_text', '')}"
    phase = clean_text(item.get("phase")) or detect_phase(text)
    study_type = clean_text(item.get("study_type")) or detect_study_type(text)
    endpoints = detect_endpoints(text)
    sample_size = detect_sample_size(text)
    return {
        "Date": item.get("published_at") or item.get("updated_at") or "Unknown",
        "Title": item.get("title") or "Untitled",
        "Phase": phase if phase else "Unknown",
        "Study type": study_type if study_type else "Unknown",
        "Endpoints": endpoints,
        "N heuristic": sample_size,
        "Citations": item.get("citations") if item.get("citations") is not None else "N/A",
        "Source": item.get("source") or "Unknown",
        "Identifiers": _identifier_blob(item),
        "Link": item.get("url") or "",
    }


def _rows_to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def render_card(item: dict, panel: str, search_match_ctx: dict | None = None):
    def _summary_to_markdown(summary_text: str) -> str:
        lines = [ln.strip() for ln in summary_text.splitlines() if ln.strip()]
        bullets: list[str] = []
        for line in lines:
            if ":" in line:
                k, v = line.split(":", 1)
                bullets.append(f"- **{k.strip()}**: {v.strip()}")
            else:
                bullets.append(f"- {line}")
        return "\n".join(bullets)

    with st.container(border=True):
        st.markdown(f"### {item.get('title')}")
        evidence = _evidence_label(item)
        confidence = _confidence_label(item)
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.caption(f"Source: {item.get('source')}")
        with m2:
            st.caption(f"Date: {item.get('published_at') or item.get('updated_at') or 'N/A'}")
        with m3:
            st.caption(f"Score: {item.get('score', 0)}")
        with m4:
            c_val = item.get("citations")
            c_rate = citations_per_year(item)
            c_text = f"{c_val}" if c_val is not None else "N/A"
            if c_rate is not None:
                c_text += f" ({c_rate}/yr)"
            st.caption(f"Citations: {c_text}")
        st.markdown(_evidence_badge_html(evidence), unsafe_allow_html=True)
        st.markdown(_confidence_badge_html(confidence), unsafe_allow_html=True)
        if item.get("full_text_source"):
            st.markdown(_full_text_badge_html(item.get("full_text_source")), unsafe_allow_html=True)

        search_match = _search_match_for_item(item, search_match_ctx)
        if search_match:
            st.markdown(
                f"<span style='display:inline-block;padding:4px 10px;margin:2px 0;border-radius:999px;background:{search_match['color']};color:{search_match['text']};font-size:0.78rem;font-weight:600;'>Search match: {search_match['level']} ({search_match['score']}%)</span>",
                unsafe_allow_html=True,
            )
            st.progress(int(search_match["score"]))
            st.caption(search_match["reason"])

        st.caption(f"Venue: {item.get('venue') or 'N/A'}")
        st.caption(f"PMID: {item.get('pmid') or '-'} | DOI: {item.get('doi') or '-'} | NCT: {item.get('nct_id') or '-'}")

        try:
            explain = json.loads(item.get("score_explain_json") or "[]")
        except Exception:
            explain = []
        st.markdown("**Why ranked**")
        badges = _score_badges(explain)
        if badges:
            badge_html = " ".join(
                f"<span style='display:inline-block;padding:4px 10px;margin:2px;border-radius:999px;background:#1e293b;color:#e2e8f0;font-size:0.80rem;'>{b}</span>"
                for b in badges
            )
            st.markdown(badge_html, unsafe_allow_html=True)
        st.caption(", ".join(explain) if explain else "No rule matches")

        st.markdown("**What I used (source snippets)**")
        snippets = _pick_source_snippets(item, explain, max_snippets=3)
        if snippets:
            for snip in snippets:
                st.markdown(f"- {clean_text(snip)}")
        else:
            st.caption("No source snippets available.")

        st.markdown("**Abstract**")
        abstract_text = clean_text(item.get("abstract_or_text"))
        if abstract_text:
            preview = _preview_sentences(abstract_text, max_sentences=3)
            if preview:
                st.markdown(preview)
            with st.expander("View abstract", expanded=False):
                st.write(abstract_text)
        else:
            st.write("No abstract available.")

        st.markdown("**Structured summary**")
        st.caption(_summary_basis_text(item))
        st.markdown(_summary_to_markdown(clean_text(item.get("summary_text")) or "No summary"))

        if item.get("url"):
            st.markdown(f"[Open source link]({item['url']})")


search_mode = st.toggle("Search mode", value=True, help="Search by free-text oncology query.")
search_query = st.text_input(
    "Search oncology topic",
    placeholder="e.g., metastatic NSCLC pembrolizumab phase 3",
    disabled=not search_mode,
)

search_match_ctx = _build_search_match_context(search_query) if search_mode and search_query.strip() else None

specialty_sorted = sorted(specialties, key=_pretty_specialty)
top1, top2, top3, top4 = st.columns([2, 2, 1, 2])
with top1:
    specialty_choice = st.selectbox(
        "Specialty",
        ["<Select specialty>"] + specialty_sorted,
        index=0,
        disabled=search_mode,
        format_func=lambda x: x if x.startswith("<") else _pretty_specialty(x),
    )
specialty = None if specialty_choice.startswith("<") else specialty_choice

subcategories = packs.list_subcategories(specialty) if specialty else []
with top2:
    subcategory_choice = st.selectbox(
        "Subcategory",
        ["<Select subcategory>"] + subcategories,
        index=0,
        disabled=search_mode or not specialty,
    )
subcategory = None if subcategory_choice.startswith("<") else subcategory_choice

with top3:
    window_options = {"One week": 7, "One month": 30, "Quarter": 90, "Year": 365}
    selected_window = st.selectbox("Time window", list(window_options.keys()), index=1)
    days_back = window_options[selected_window]

with top4:
    custom_profiles = db.list_custom_mode_profiles(conn)
    custom_name_map = {f"Custom: {p['name']}": p for p in custom_profiles}
    mode_options = MODE_OPTIONS + list(custom_name_map.keys())
    if st.session_state["selected_mode"] not in mode_options:
        st.session_state["selected_mode"] = MODE_ALL
    selected_mode = st.selectbox("Mode", mode_options, index=mode_options.index(st.session_state["selected_mode"]))
    st.session_state["selected_mode"] = selected_mode

if st.session_state["selected_mode"] in custom_name_map:
    mode_config = dict(custom_name_map[st.session_state["selected_mode"]].get("config", {}))
else:
    mode_config = dict(get_mode_preset(st.session_state["selected_mode"]))

include_papers = bool(mode_config.get("include_papers", True))
include_trials = bool(mode_config.get("include_trials", True))
include_preprints = bool(mode_config.get("include_preprints", False))
include_journal_rss = bool(mode_config.get("include_journal_rss", False))
include_fda_approvals = bool(mode_config.get("include_fda_approvals", False))
phase_2_3_only = bool(mode_config.get("phase_2_3_only", False))
rct_meta_only = bool(mode_config.get("rct_meta_only", False))
use_full_text_oa = bool(mode_config.get("use_full_text_oa", False))
if "run_use_full_text_oa" in st.session_state:
    use_full_text_oa = bool(st.session_state["run_use_full_text_oa"])
scoring_weights = dict(mode_config.get("scoring_weights", {}))

st.caption(
    f"Mode profile: papers={'on' if include_papers else 'off'}, trials={'on' if include_trials else 'off'}, "
    f"phase II/III only={'on' if phase_2_3_only else 'off'}, rct/meta only={'on' if rct_meta_only else 'off'}, "
    f"full text OA={'on' if use_full_text_oa else 'off'}, llm polish={'on' if llm_polish_summary else 'off'}"
)

any_source_selected = any(
    [include_papers, include_trials, include_preprints, include_journal_rss, include_fda_approvals]
)

preview_options_kwargs = dict(
    mode_name=st.session_state["selected_mode"],
    days_back=days_back,
    include_trials=include_trials,
    include_papers=include_papers,
    include_preprints=include_preprints,
    include_journal_rss=include_journal_rss,
    include_fda_approvals=include_fda_approvals,
    use_full_text_oa=use_full_text_oa,
    force_full_refresh=force_full_refresh,
    incremental_cap_days=days_back,
)
if "llm_polish_summary" in inspect.signature(RunOptions).parameters:
    preview_options_kwargs["llm_polish_summary"] = llm_polish_summary
preview_options = RunOptions(**preview_options_kwargs)

last_run_scope: tuple[str, str] | None = None
if search_mode and search_query.strip():
    last_run_scope = ("search", _query_key(search_query.strip()))
elif specialty and subcategory:
    last_run_scope = (specialty, subcategory)

if last_run_scope:
    last_success = db.get_last_successful_run(
        conn,
        last_run_scope[0],
        last_run_scope[1],
        mode_name=preview_options.mode_name,
        sources_key=build_sources_key(preview_options),
    )
    if last_success:
        effective_days_preview, _ = resolve_incremental_days_back(conn, last_run_scope[0], last_run_scope[1], preview_options)
        st.caption(
            f"Last run: {last_success.get('finished_at') or last_success.get('started_at')} | "
            f"Next window: last {effective_days_preview} day(s)"
        )
    else:
        st.caption("Last run: none for current scope/sources.")

clear_scope: tuple[str, str] | None = None
if search_mode:
    existing_search_scope = st.session_state.get("last_search_key", "").strip()
    if existing_search_scope:
        clear_scope = ("search", existing_search_scope)
    elif search_query.strip():
        clear_scope = ("search", _query_key(search_query.strip()))
elif specialty and subcategory:
    clear_scope = (specialty, subcategory)

if st.session_state.get("clear_notice"):
    st.success(st.session_state["clear_notice"])
    st.session_state["clear_notice"] = ""

if st.session_state.get("run_feedback"):
    kind, msg = st.session_state["run_feedback"]
    if kind == "warning":
        st.warning(msg)
    else:
        st.success(msg)
    st.session_state["run_feedback"] = None

actions_slot = st.empty()
with actions_slot.container():
    run_row_l, run_row_m, _ = st.columns([1.0, 1.0, 8.0], gap="medium")
    with run_row_l:
        run_clicked = st.button(
            "Run",
            key="run_button_main",
            type="primary",
            use_container_width=True,
            disabled=(not search_query.strip()) if search_mode else (not (specialty and subcategory)),
        )
    with run_row_m:
        clear_clicked = st.button(
            "Clear results",
            key="clear_results_main",
            use_container_width=True,
            disabled=clear_scope is None,
            help="Clears only the current search/specialty scope.",
        )

if clear_clicked and clear_scope is not None:
    db.clear_scope_items(conn, clear_scope[0], clear_scope[1])
    st.session_state["has_run_once"] = False
    if clear_scope[0] == "search":
        st.session_state["last_search_key"] = ""
        st.session_state["search_diagnostics"] = {}
    st.session_state["clear_notice"] = "Cleared results for current scope."
    st.rerun()

if run_clicked:
    actions_slot.empty()
    with st.spinner("Running ingestion and ranking..."):
        selected_mode_name = st.session_state.get("selected_mode", MODE_ALL)
        is_all_mode = selected_mode_name == MODE_ALL
        if fast_mode:
            limits = {
                "retmax_pubmed": 15,
                "trials_limit": 12,
                "europepmc_limit": 12,
                "preprint_limit": 8,
                "rss_limit": 8,
                "fda_limit": 8,
                "max_run_seconds": 20 if is_all_mode else 8,
                "max_total_run_seconds": 25,
            }
        else:
            limits = {
                "retmax_pubmed": 40,
                "trials_limit": 30,
                "europepmc_limit": 30,
                "preprint_limit": 20,
                "rss_limit": 20,
                "fda_limit": 20,
                "max_run_seconds": 28 if is_all_mode else 12,
                "max_total_run_seconds": 45,
            }

        if search_mode:
            qtxt = search_query.strip().lower()
            token_count = len([t for t in qtxt.split() if t])
            short_or_broad = (len(qtxt) <= 4) or (token_count <= 2)
            if short_or_broad:
                limits["max_run_seconds"] += 20
                limits["max_total_run_seconds"] += 20

        run_options_kwargs = dict(
            mode_name=st.session_state["selected_mode"],
            days_back=days_back,
            retmax_pubmed=limits["retmax_pubmed"],
            trials_limit=limits["trials_limit"],
            europepmc_limit=limits["europepmc_limit"],
            preprint_limit=limits["preprint_limit"],
            rss_limit=limits["rss_limit"],
            fda_limit=limits["fda_limit"],
            include_trials=include_trials,
            include_papers=include_papers,
            include_preprints=include_preprints,
            include_journal_rss=include_journal_rss,
            include_fda_approvals=include_fda_approvals,
            enrich_citations=enrich_citations and not fast_mode,
            enable_semantic_scholar=enable_semantic_scholar and enrich_citations and not fast_mode,
            phase_2_3_only=phase_2_3_only,
            rct_meta_only=rct_meta_only,
            scoring_weights=scoring_weights,
            use_full_text_oa=use_full_text_oa,
            incremental_cap_days=days_back,
            force_full_refresh=force_full_refresh,
        )
        if "llm_polish_summary" in inspect.signature(RunOptions).parameters:
            run_options_kwargs["llm_polish_summary"] = llm_polish_summary
        if "max_run_seconds" in inspect.signature(RunOptions).parameters:
            run_options_kwargs["max_run_seconds"] = limits["max_run_seconds"]

        total_ingested = 0
        total_deduped = 0
        run_count = 0
        timed_out_runs = 0
        overall_timed_out = False
        overall_start = time.monotonic()

        timeout_notes: list[str] = []
        if search_mode:
            result = run_pipeline_query(conn, query=search_query.strip(), options=RunOptions(**run_options_kwargs))
            total_ingested = result["ingested_count"]
            total_deduped = result["deduped_count"]
            run_count = 1
            timed_out_runs = 1 if result.get("timed_out") else 0
            st.session_state["last_search_key"] = _query_key(search_query.strip())
            st.session_state["search_diagnostics"] = result.get("diagnostics") or {}
            if result.get("timeout_reason"):
                timeout_notes.append(str(result.get("timeout_reason")))
        else:
            st.session_state["search_diagnostics"] = {}
            targets = [(specialty, subcategory)] if (specialty and subcategory) else []
            max_total_run_seconds = limits["max_total_run_seconds"]
            for sp, sub in targets:
                if (time.monotonic() - overall_start) > max_total_run_seconds:
                    overall_timed_out = True
                    break
                result = run_pipeline(conn, specialty=sp, subcategory=sub, options=RunOptions(**run_options_kwargs))
                total_ingested += result["ingested_count"]
                total_deduped += result["deduped_count"]
                if result.get("timed_out"):
                    timed_out_runs += 1
                    if result.get("timeout_reason"):
                        timeout_notes.append(str(result.get("timeout_reason")))
                run_count += 1

    st.session_state["has_run_once"] = True
    if overall_timed_out or timed_out_runs:
        hint = ""
        if timeout_notes:
            hint = f" | Reason: {timeout_notes[0]}"
        if search_mode and len(search_query.strip()) <= 4:
            hint += " | Tip: Short broad queries may need more specific terms (e.g., ocular melanoma, retinal toxicity)."
        st.session_state["run_feedback"] = (
            "warning",
            f"Run completed with timeout safeguards. Packs run: {run_count} | "
            f"Ingested: {total_ingested} | Deduped: {total_deduped} | "
            f"Timed-out packs: {timed_out_runs}{hint}",
        )
    else:
        st.session_state["run_feedback"] = (
            "success",
            f"Run complete. Packs run: {run_count} | Ingested: {total_ingested} | Deduped: {total_deduped}",
        )
    st.rerun()

if not any_source_selected:
    st.info("Current mode disabled all data sources. Choose a different mode to run.")
    st.stop()

if search_mode and not search_query.strip():
    st.info("Enter a search query to enable Run.")
    st.stop()

if (not search_mode) and (not specialty or not subcategory):
    st.info("Select specialty and subcategory to enable Run.")
    st.stop()


def _load_items() -> list[dict]:
    loaded: list[dict] = []
    if search_mode:
        key = st.session_state.get("last_search_key", "")
        if key:
            loaded.extend(db.get_ranked_items(conn, "search", key, mode="new", include_trials=True))
    elif specialty and subcategory:
        loaded.extend(db.get_ranked_items(conn, specialty, subcategory, mode="new", include_trials=True))

    seen_ids: set[int] = set()
    deduped: list[dict] = []
    for item in loaded:
        item_id = int(item["id"])
        if item_id in seen_ids:
            continue
        seen_ids.add(item_id)
        deduped.append(item)
    return deduped


all_items = _load_items()

trial_sources = {"clinicaltrials", "fda"}
paper_items = [i for i in all_items if i.get("source") not in trial_sources]
trial_items = [i for i in all_items if i.get("source") in trial_sources]
recent_paper_items = [i for i in paper_items if _is_within_window(i, days_back)]
recent_trial_items = [i for i in trial_items if _is_within_window(i, days_back)]


if "active_view" not in st.session_state:
    st.session_state["active_view"] = "Digest"

active_view = st.segmented_control(
    "View",
    ["Digest", "New", "Trials", "Saved", "Research Tools"],
    selection_mode="single",
    key="active_view",
)
active_view = active_view or st.session_state.get("active_view", "Digest")

if active_view == "Digest":
    if st.session_state.get("has_run_once", False):
        st.markdown("#### Top 7 in 5 minutes")
        top_priority = st.selectbox(
            "Top 7 priority",
            ["Balanced", "Papers first", "Trials first"],
            index=0,
            key="top_priority",
        )
        top_sort = st.selectbox(
            "Sort (Top N)",
            ["Highest score", "Newest", "Most cited", "Hot"],
            index=0,
            key="sort_topn",
        )
        top_n = st.slider("Top N (quick review)", min_value=3, max_value=15, value=7, step=1, key="top_n")
        if top_priority == "Papers first":
            source_rank = lambda x: 1 if x.get("source") in trial_sources else 0
        elif top_priority == "Trials first":
            source_rank = lambda x: 0 if x.get("source") in trial_sources else 1
        else:
            source_rank = lambda x: 0

        def metric_rank(x: dict) -> tuple:
            dt = _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)
            if top_sort == "Newest":
                return (-dt.timestamp(), -(x.get("score") or 0), -(x.get("citations") or 0))
            if top_sort == "Most cited":
                return (-(x.get("citations") or 0), -(x.get("score") or 0), -dt.timestamp())
            if top_sort == "Hot":
                return (-hot_score(x), -(x.get("score") or 0), -dt.timestamp())
            return (-(x.get("score") or 0), -(x.get("citations") or 0), -dt.timestamp())

        def top_sort_key(x: dict) -> tuple:
            return (source_rank(x), *metric_rank(x))

        top_items = sorted(recent_paper_items + recent_trial_items, key=top_sort_key)[:top_n]
        if not top_items:
            st.info("No items found for Top 7 in the selected window and filters.")
        for item in top_items:
            render_top_card(item, search_match_ctx=search_match_ctx)

elif active_view == "New":
    if st.session_state.get("has_run_once", False):
        new_sort = st.selectbox(
            "Sort (New)",
            ["Newest", "Highest score", "Most cited", "Hot"],
            key="sort_new",
        )
        if new_sort == "Most cited":
            new_items = sorted(
                recent_paper_items,
                key=lambda x: (x.get("citations") or 0, x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif new_sort == "Hot":
            new_items = sorted(
                recent_paper_items,
                key=lambda x: (hot_score(x), x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif new_sort == "Highest score":
            new_items = sorted(
                recent_paper_items,
                key=lambda x: (x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        else:
            new_items = sorted(
                recent_paper_items,
                key=lambda x: (_item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc), x.get("score") or 0),
                reverse=True,
            )
        if not new_items:
            st.info("No paper items found in the selected time window. Try a longer window or broader sources.")
        for item in new_items[:100]:
            render_card(item, panel="new", search_match_ctx=search_match_ctx)

elif active_view == "Trials":
    if st.session_state.get("has_run_once", False):
        trial_sort = st.selectbox(
            "Sort (Trials)",
            ["Most recently updated", "Highest score", "Most cited", "Hot"],
            key="sort_trials",
        )
        if trial_sort == "Highest score":
            trial_items_sorted = sorted(
                recent_trial_items,
                key=lambda x: (x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif trial_sort == "Hot":
            trial_items_sorted = sorted(
                recent_trial_items,
                key=lambda x: (hot_score(x), x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif trial_sort == "Most cited":
            trial_items_sorted = sorted(
                recent_trial_items,
                key=lambda x: (x.get("citations") or 0, x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        else:
            trial_items_sorted = sorted(
                recent_trial_items,
                key=lambda x: (_item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc), x.get("score") or 0),
                reverse=True,
            )
        if not trial_items_sorted:
            st.info("No trial updates found in the selected time window.")
        for item in trial_items_sorted[:100]:
            render_card(item, panel="trials", search_match_ctx=search_match_ctx)

elif active_view == "Saved":
    st.caption("Saved view will list starred/bookmarked papers in a future update.")

elif active_view == "Research Tools":
    if st.session_state.get("has_run_once", False):
        if search_mode and st.session_state.get("search_diagnostics"):
            diag = st.session_state.get("search_diagnostics") or {}
            with st.expander("Search diagnostics", expanded=False):
                st.caption("Effective queries used")
                st.code(
                    f"Paper query: {diag.get('paper_query') or '-'}\n"
                    f"Trial query: {diag.get('trial_query') or '-'}",
                    language="text",
                )
                st.caption(
                    f"Hits before relevance filter: {diag.get('raw_hits_total', 0)} | "
                    f"after filter: {diag.get('relevant_hits_total', 0)}"
                )
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Raw hits by source**")
                    st.json(diag.get("raw_hits_by_source") or {})
                with c2:
                    st.markdown("**Relevant hits by source**")
                    st.json(diag.get("relevant_hits_by_source") or {})

        table_scope = st.selectbox(
            "Table scope",
            ["All results", "Papers only", "Trials only"],
            index=0,
            key="table_scope",
        )
        table_sort = st.selectbox(
            "Sort (Table)",
            ["Newest", "Highest score", "Most cited", "Hot"],
            index=0,
            key="table_sort",
        )
        if table_scope == "Papers only":
            table_items = recent_paper_items
        elif table_scope == "Trials only":
            table_items = recent_trial_items
        else:
            table_items = recent_paper_items + recent_trial_items

        if table_sort == "Most cited":
            table_items = sorted(
                table_items,
                key=lambda x: (x.get("citations") or 0, x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif table_sort == "Hot":
            table_items = sorted(
                table_items,
                key=lambda x: (hot_score(x), x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        elif table_sort == "Highest score":
            table_items = sorted(
                table_items,
                key=lambda x: (x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
                reverse=True,
            )
        else:
            table_items = sorted(
                table_items,
                key=lambda x: (_item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc), x.get("score") or 0),
                reverse=True,
            )

        rows = [_table_row(i) for i in table_items]
        if not rows:
            st.info("No rows available for the current filters/time window.")
        else:
            st.caption("Sortable researcher view. Values are best-effort extraction; unknowns are expected.")
            csv_text = _rows_to_csv(rows)
            st.download_button(
                "Download CSV",
                data=csv_text,
                file_name=f"oncopulse_table_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key="download_table_csv",
            )
            st.dataframe(rows, use_container_width=True, hide_index=True)
