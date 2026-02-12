import inspect
import json
from datetime import datetime, timedelta, timezone
import time

import streamlit as st

from oncopulse import db, packs
from oncopulse.ingest import europepmc, pubmed
from oncopulse.ingest.source_extract import extract_abstract_from_url
from oncopulse.services.run_pipeline import RunOptions, run_pipeline, run_pipeline_query
from oncopulse.summarize import summarize_item
from oncopulse.text_utils import clean_multiline_text, clean_text


st.set_page_config(page_title="OncoPulse", layout="wide")

conn = db.get_conn()
db.init_db(conn)

header_l, header_r = st.columns([5, 1])
with header_l:
    st.title("OncoPulse - Oncology Research Inbox")
with header_r:
    with st.popover("Run settings"):
        fast_mode = st.toggle("Fast mode", value=True)
        enrich_citations = st.toggle("Enrich citations", value=False)
        st.caption("Maintenance")
        confirm_clear_cache = st.checkbox("Confirm clear local cache", value=False)
        if st.button("Clear local cache", disabled=not confirm_clear_cache):
            db.clear_all_local_cache(conn)
            st.session_state["has_run_once"] = False
            st.session_state["last_search_key"] = ""
            st.success("Local cache cleared.")
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


def render_card(item: dict, panel: str):
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
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.caption(f"Source: {item.get('source')}")
        with m2:
            st.caption(f"Date: {item.get('published_at') or item.get('updated_at') or 'N/A'}")
        with m3:
            st.caption(f"Score: {item.get('score', 0)}")
        with m4:
            st.caption(f"Citations: {item.get('citations') if item.get('citations') is not None else 'N/A'}")

        st.caption(f"Venue: {item.get('venue') or 'N/A'}")
        st.caption(f"PMID: {item.get('pmid') or '-'} | DOI: {item.get('doi') or '-'} | NCT: {item.get('nct_id') or '-'}")

        try:
            explain = json.loads(item.get("score_explain_json") or "[]")
        except Exception:
            explain = []
        st.markdown("**Why ranked**")
        st.markdown(", ".join(explain) if explain else "No rule matches")

        st.markdown("**Abstract**")
        abstract_text = clean_text(item.get("abstract_or_text"))
        pmid = (item.get("pmid") or "").strip()
        doi = (item.get("doi") or "").strip()
        url = (item.get("url") or "").strip()
        backfill_key = f"abs_backfill_{item.get('id')}"
        if not abstract_text and not st.session_state.get(backfill_key, False):
            st.session_state[backfill_key] = True
            fetched_abs = ""

            # 1) PubMed by PMID
            if pmid:
                try:
                    fetched = pubmed.fetch([pmid])
                    if fetched:
                        fetched_abs = clean_text(fetched[0].get("abstract_or_text"))
                except Exception:
                    pass

            # 2) Europe PMC by PMID/DOI
            if not fetched_abs:
                try:
                    fetched_abs = clean_text(europepmc.fetch_abstract_by_ids(pmid=pmid or None, doi=doi or None))
                except Exception:
                    pass

            # 3) Source page metadata/JSON-LD (for publisher pages with visible outline)
            if not fetched_abs and url:
                fetched_abs = clean_text(extract_abstract_from_url(url))

            if fetched_abs:
                item["abstract_or_text"] = fetched_abs
                item["summary_text"] = summarize_item(item)
                db.update_item_text_fields(conn, int(item["id"]), fetched_abs, item["summary_text"])
                abstract_text = fetched_abs
        if abstract_text:
            with st.expander("View abstract", expanded=False):
                st.write(abstract_text)
        else:
            st.write("No abstract available.")

        st.markdown("**Structured summary**")
        st.markdown(_summary_to_markdown(clean_multiline_text(item.get("summary_text")) or "No summary"))

        if item.get("url"):
            st.markdown(f"[Open source link]({item['url']})")


search_mode = st.toggle("Search mode", value=False, help="Search by free-text oncology topic instead of specialty packs.")
search_query = st.text_input(
    "Search oncology topic",
    placeholder="e.g., metastatic NSCLC pembrolizumab phase 3",
    disabled=not search_mode,
)

# Build grouped specialties.
grouped_specialties: dict[str, list[str]] = {}
for s in specialties:
    grouped_specialties.setdefault(_group_for_specialty(s), []).append(s)
for group_name in grouped_specialties:
    grouped_specialties[group_name] = sorted(grouped_specialties[group_name], key=_pretty_specialty)

group_order = [k for k in SPECIALTY_GROUPS if k in grouped_specialties]
if "Other" in grouped_specialties:
    group_order.append("Other")
group_options = ["<Select specialty group>"] + group_order

col1, col2, col3, col4 = st.columns(4)
with col1:
    selected_group_choice = st.selectbox("Specialty group", group_options, index=0, disabled=search_mode)
selected_group = None if selected_group_choice.startswith("<") else selected_group_choice

visible_specialties = grouped_specialties.get(selected_group or "", [])
visible_specialties = sorted(visible_specialties, key=_pretty_specialty)
specialty_options = ["<Any specialty in group>"] + visible_specialties

with col2:
    specialty_choice = st.selectbox(
        "Specialty",
        specialty_options,
        index=0,
        disabled=search_mode or not selected_group,
        format_func=lambda x: x if x.startswith("<") else _pretty_specialty(x),
    )
specialty = None if specialty_choice.startswith("<") else specialty_choice

subcategories = packs.list_subcategories(specialty) if specialty else []
subcategory_options = ["<Any subcategory>"] + subcategories
with col3:
    subcategory_choice = st.selectbox(
        "Subcategory",
        subcategory_options,
        index=0,
        disabled=search_mode or not specialty,
    )
subcategory = None if subcategory_choice.startswith("<") else subcategory_choice

with col4:
    window_options = {
        "One week": 7,
        "One month": 30,
        "Quarter": 90,
        "Year": 365,
    }
    selected_window = st.selectbox("Time window", list(window_options.keys()), index=1)
    days_back = window_options[selected_window]

st.markdown("**Data sources**")
s1, s2, s3, s4, s5 = st.columns(5)
with s1:
    include_papers = st.checkbox("PubMed/Europe PMC papers", value=True)
with s2:
    include_trials = st.checkbox("ClinicalTrials.gov", value=True)
with s3:
    include_preprints = st.checkbox("bioRxiv/medRxiv", value=False)
with s4:
    include_journal_rss = st.checkbox("Journal RSS", value=False)
with s5:
    include_fda_approvals = st.checkbox("FDA approvals", value=False)

any_source_selected = any(
    [include_papers, include_trials, include_preprints, include_journal_rss, include_fda_approvals]
)

can_run = bool(search_query.strip()) if search_mode else bool(selected_group)
if st.button("Run", type="primary", disabled=(not can_run or not any_source_selected)):
    with st.spinner("Running ingestion and ranking..."):
        if fast_mode:
            limits = {
                "retmax_pubmed": 15,
                "trials_limit": 12,
                "europepmc_limit": 12,
                "preprint_limit": 8,
                "rss_limit": 8,
                "fda_limit": 8,
                "max_run_seconds": 8,
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
                "max_run_seconds": 12,
                "max_total_run_seconds": 45,
            }

        run_options_kwargs = dict(
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
            phase_2_3_only=False,
            rct_meta_only=False,
        )
        if "max_run_seconds" in inspect.signature(RunOptions).parameters:
            run_options_kwargs["max_run_seconds"] = limits["max_run_seconds"]

        total_ingested = 0
        total_deduped = 0
        run_count = 0
        timed_out_runs = 0
        overall_timed_out = False
        overall_start = time.monotonic()

        if search_mode:
            result = run_pipeline_query(conn, query=search_query.strip(), options=RunOptions(**run_options_kwargs))
            total_ingested = result["ingested_count"]
            total_deduped = result["deduped_count"]
            run_count = 1
            timed_out_runs = 1 if result.get("timed_out") else 0
            st.session_state["last_search_key"] = _query_key(search_query.strip())
        else:
            if specialty and subcategory:
                targets = [(specialty, subcategory)]
            elif specialty:
                targets = [(specialty, sub) for sub in packs.list_subcategories(specialty)]
            else:
                targets = []
                for sp in visible_specialties:
                    for sub in packs.list_subcategories(sp):
                        targets.append((sp, sub))

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
                run_count += 1

    st.session_state["has_run_once"] = True
    if overall_timed_out or timed_out_runs:
        st.warning(
            f"Run completed with timeout safeguards. Packs run: {run_count} | "
            f"Ingested: {total_ingested} | Deduped: {total_deduped} | "
            f"Timed-out packs: {timed_out_runs}"
        )
    else:
        st.success(f"Run complete. Packs run: {run_count} | Ingested: {total_ingested} | Deduped: {total_deduped}")

if not any_source_selected:
    st.info("Select at least one data source to enable Run.")
    st.stop()

if search_mode and not search_query.strip():
    st.info("Enter a search query to enable Run. In search mode, dropdown selections are disabled.")
    st.stop()
if not search_mode and not selected_group:
    st.info("Select a specialty group to enable Run. In browse mode, search is disabled.")
    st.stop()


def _load_items() -> list[dict]:
    loaded: list[dict] = []
    if search_mode:
        key = st.session_state.get("last_search_key", "")
        if not key:
            return []
        loaded.extend(db.get_ranked_items(conn, "search", key, mode="new", include_trials=True))
    else:
        if specialty and subcategory:
            loaded.extend(db.get_ranked_items(conn, specialty, subcategory, mode="new", include_trials=True))
        elif specialty:
            for sub in packs.list_subcategories(specialty):
                loaded.extend(db.get_ranked_items(conn, specialty, sub, mode="new", include_trials=True))
        else:
            for sp in visible_specialties:
                for sub in packs.list_subcategories(sp):
                    loaded.extend(db.get_ranked_items(conn, sp, sub, mode="new", include_trials=True))

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

# Results section filters/sort (requested to be here, not in top controls).
st.markdown("**Results filters**")
rf1, rf2 = st.columns(2)
with rf1:
    result_phase_only = st.checkbox("Phase II/III only", value=False, key="result_phase")
with rf2:
    result_rct_meta_only = st.checkbox("RCT/meta only", value=False, key="result_rct")

trial_sources = {"clinicaltrials", "fda"}
paper_items = [i for i in all_items if i.get("source") not in trial_sources]
trial_items = [i for i in all_items if i.get("source") in trial_sources]
recent_paper_items = [i for i in paper_items if _is_within_window(i, days_back)]
recent_trial_items = [i for i in trial_items if _is_within_window(i, days_back)]

recent_paper_items = _result_filter(recent_paper_items, result_phase_only, result_rct_meta_only)
recent_trial_items = _result_filter(recent_trial_items, result_phase_only, result_rct_meta_only)


t1, t2 = st.tabs(["New & Most Cited", "Trials"])
with t1:
    if st.session_state.get("has_run_once", False):
        new_sort = st.selectbox("Sort (New & Most Cited)", ["Newest", "Highest score", "Most cited"], key="sort_new")
        if new_sort == "Most cited":
            new_items = sorted(
                recent_paper_items,
                key=lambda x: (x.get("citations") or 0, x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
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
            render_card(item, panel="new")

with t2:
    if st.session_state.get("has_run_once", False):
        trial_sort = st.selectbox("Sort (Trials)", ["Most recently updated", "Highest score", "Most cited"], key="sort_trials")
        if trial_sort == "Highest score":
            trial_items_sorted = sorted(
                recent_trial_items,
                key=lambda x: (x.get("score") or 0, _item_datetime(x) or datetime.min.replace(tzinfo=timezone.utc)),
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
            render_card(item, panel="trials")
