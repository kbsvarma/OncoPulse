from dataclasses import dataclass
from datetime import datetime, timezone
import math
import time
from typing import Any

from .. import db, nlp, packs, scoring, summarize
from ..ingest import clinicaltrials, dedup, europepmc, fda, fulltext_oa, openalex, preprints, pubmed, rss_feeds, semanticscholar

MODE_ALL = "All"
MODE_CLINICIAN = "Clinician (Practice-changing)"
MODE_SAFETY_WATCH = "Safety Watch"
MODE_TRIAL_RADAR = "Trial Radar"
MODE_RESEARCHER = "Researcher"
MODE_FELLOW = "Fellow"

MODE_OPTIONS = [
    MODE_ALL,
    MODE_CLINICIAN,
    MODE_SAFETY_WATCH,
    MODE_TRIAL_RADAR,
    MODE_RESEARCHER,
    MODE_FELLOW,
]

MODE_PRESETS: dict[str, dict[str, Any]] = {
    MODE_ALL: {
        "include_papers": True,
        "include_trials": True,
        "include_preprints": True,
        "include_journal_rss": True,
        "include_fda_approvals": True,
        "phase_2_3_only": False,
        "rct_meta_only": False,
        "use_full_text_oa": False,
        "scoring_weights": {},
    },
    MODE_CLINICIAN: {
        "include_papers": True,
        "include_trials": True,
        "include_preprints": False,
        "include_journal_rss": False,
        "include_fda_approvals": True,
        "phase_2_3_only": True,
        "rct_meta_only": True,
        "use_full_text_oa": False,
        "scoring_weights": {
            "phase_iii": 10,
            "randomized": 8,
            "overall_survival": 5,
            "progression_free_survival": 4,
            "meta_analysis": 5,
        },
    },
    MODE_SAFETY_WATCH: {
        "include_papers": True,
        "include_trials": True,
        "include_preprints": False,
        "include_journal_rss": True,
        "include_fda_approvals": True,
        "phase_2_3_only": False,
        "rct_meta_only": False,
        "use_full_text_oa": False,
        "scoring_weights": {
            "meta_analysis": 6,
            "phase_iii": 6,
            "randomized": 5,
            "overall_survival": 2,
            "progression_free_survival": 2,
        },
    },
    MODE_TRIAL_RADAR: {
        "include_papers": False,
        "include_trials": True,
        "include_preprints": False,
        "include_journal_rss": False,
        "include_fda_approvals": True,
        "phase_2_3_only": False,
        "rct_meta_only": False,
        "use_full_text_oa": False,
        "scoring_weights": {
            "phase_iii": 8,
            "phase_ii": 5,
            "randomized": 6,
            "overall_survival": 3,
            "progression_free_survival": 3,
        },
    },
    MODE_RESEARCHER: {
        "include_papers": True,
        "include_trials": True,
        "include_preprints": True,
        "include_journal_rss": True,
        "include_fda_approvals": False,
        "phase_2_3_only": False,
        "rct_meta_only": False,
        "use_full_text_oa": True,
        "scoring_weights": {
            "meta_analysis": 5,
            "phase_iii": 6,
            "phase_ii": 4,
            "citations_multiplier": 1.5,
        },
    },
    MODE_FELLOW: {
        "include_papers": True,
        "include_trials": True,
        "include_preprints": True,
        "include_journal_rss": False,
        "include_fda_approvals": False,
        "phase_2_3_only": False,
        "rct_meta_only": False,
        "use_full_text_oa": False,
        "scoring_weights": {
            "phase_iii": 7,
            "randomized": 6,
            "meta_analysis": 5,
            "sample_size": 2,
            "citations_multiplier": 1.2,
        },
    },
}


@dataclass
class RunOptions:
    mode_name: str = MODE_ALL
    days_back: int = 14
    retmax_pubmed: int = 200
    trials_limit: int = 100
    europepmc_limit: int = 100
    preprint_limit: int = 100
    rss_limit: int = 100
    fda_limit: int = 100
    include_trials: bool = True
    include_papers: bool = True
    include_preprints: bool = False
    include_journal_rss: bool = True
    include_fda_approvals: bool = True
    enrich_citations: bool = False
    phase_2_3_only: bool = False
    rct_meta_only: bool = False
    max_run_seconds: int = 45
    scoring_weights: dict[str, float] | None = None
    enable_semantic_scholar: bool = False
    incremental_cap_days: int | None = None
    force_full_refresh: bool = False
    use_full_text_oa: bool = False
    llm_polish_summary: bool = False


def get_mode_preset(mode_name: str) -> dict[str, Any]:
    return dict(MODE_PRESETS.get(mode_name, MODE_PRESETS[MODE_ALL]))


def build_sources_key(options: RunOptions) -> str:
    selected: list[str] = []
    if options.include_papers:
        selected.append("papers")
    if options.include_trials:
        selected.append("trials")
    if options.include_preprints:
        selected.append("preprints")
    if options.include_journal_rss:
        selected.append("journal_rss")
    if options.include_fda_approvals:
        selected.append("fda")
    return ",".join(selected) if selected else "none"


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001
        return None


def resolve_incremental_days_back(
    conn,
    specialty: str,
    subcategory: str,
    options: RunOptions,
) -> tuple[int, dict[str, Any] | None]:
    if options.force_full_refresh:
        return options.days_back, None

    cap_days = options.incremental_cap_days or options.days_back
    last_success = db.get_last_successful_run(
        conn,
        specialty,
        subcategory,
        mode_name=options.mode_name,
        sources_key=build_sources_key(options),
    )
    if not last_success:
        return min(options.days_back, cap_days), None

    reference_text = last_success.get("finished_at") or last_success.get("started_at")
    reference_dt = _parse_iso_datetime(reference_text)
    if reference_dt is None:
        return min(options.days_back, cap_days), last_success

    elapsed_days = (datetime.now(timezone.utc) - reference_dt).total_seconds() / 86400.0
    resolved = max(1, math.ceil(elapsed_days))
    resolved = min(resolved, cap_days)
    return resolved, last_success


def _apply_filters(items: list[dict[str, Any]], options: RunOptions) -> list[dict[str, Any]]:
    def include(item: dict[str, Any]) -> bool:
        text = f"{item.get('title', '')} {item.get('abstract_or_text', '')}".lower()

        if options.phase_2_3_only and not any(t in text for t in ["phase ii", "phase 2", "phase iii", "phase 3"]):
            return False

        if options.rct_meta_only and not any(t in text for t in ["randomized", "rct", "meta-analysis", "systematic review"]):
            return False

        return True

    return [i for i in items if include(i)]


def _default_rules() -> dict[str, Any]:
    return {
        "include_terms": [],
        "exclude_terms": [],
        "global_penalty_terms": ["case report", "in vitro", "murine", "mouse"],
        "major_journals": [
            "NEJM",
            "J Clin Oncol",
            "Lancet",
            "Lancet Oncology",
            "Annals of Oncology",
            "Nature Medicine",
            "Blood",
        ],
    }


def _query_key(query: str) -> str:
    return f"query:{query.strip().lower()[:180]}"


def _item_search_blob(item: dict[str, Any]) -> str:
    fields = [
        item.get("title"),
        item.get("abstract_or_text"),
        item.get("conditions"),
        item.get("interventions"),
        item.get("primary_endpoints"),
        item.get("study_type"),
        item.get("phase"),
    ]
    return " ".join(str(v or "") for v in fields).lower()


def _contains_query_term(blob: str, term: str) -> bool:
    token = (term or "").strip().lower()
    if not token:
        return False
    if token.isalnum():
        import re

        return re.search(rf"\b{re.escape(token)}\b", blob) is not None
    return token in blob


def _is_search_relevant(item: dict[str, Any], query_context: dict[str, Any]) -> bool:
    blob = _item_search_blob(item)
    if not blob.strip():
        return False

    concepts = query_context.get("concepts") or []
    for group in concepts:
        if isinstance(group, list) and any(_contains_query_term(blob, str(t)) for t in group):
            return True

    keywords = query_context.get("keywords") or []
    if any(_contains_query_term(blob, str(k)) for k in keywords):
        return True

    raw_query = str(query_context.get("raw_query") or "").strip().lower()
    if raw_query and len(raw_query) >= 4 and raw_query in blob:
        return True

    return False


def _ingest_for_query(
    specialty: str,
    subcategory: str,
    paper_query: str,
    trial_query: str,
    options: RunOptions,
    check_timeout,
) -> list[dict[str, Any]]:
    ingested: list[dict[str, Any]] = []

    if options.include_papers:
        check_timeout()
        try:
            pmids = pubmed.search(paper_query, days_back=options.days_back, retmax=options.retmax_pubmed)
            papers = pubmed.fetch(pmids)
        except Exception:  # noqa: BLE001
            papers = []
        for p in papers:
            p["specialty"] = specialty
            p["subcategory"] = subcategory
        ingested.extend(papers)

        check_timeout()
        try:
            epmc_papers = europepmc.search(
                paper_query,
                days_back=options.days_back,
                limit=options.europepmc_limit,
                preprint_only=False,
            )
        except Exception:  # noqa: BLE001
            epmc_papers = []
        for p in epmc_papers:
            p["specialty"] = specialty
            p["subcategory"] = subcategory
        ingested.extend(epmc_papers)

        if options.include_journal_rss:
            check_timeout()
            try:
                rss_items = rss_feeds.search(paper_query, limit=options.rss_limit)
            except Exception:  # noqa: BLE001
                rss_items = []
            for r in rss_items:
                r["specialty"] = specialty
                r["subcategory"] = subcategory
            ingested.extend(rss_items)

    if options.include_preprints:
        check_timeout()
        try:
            preprint_items = preprints.search(paper_query, days_back=options.days_back, limit=options.preprint_limit)
        except Exception:  # noqa: BLE001
            preprint_items = []
        for p in preprint_items:
            p["specialty"] = specialty
            p["subcategory"] = subcategory
        ingested.extend(preprint_items)

        check_timeout()
        try:
            epmc_preprints = europepmc.search(
                paper_query,
                days_back=options.days_back,
                limit=options.preprint_limit,
                preprint_only=True,
            )
        except Exception:  # noqa: BLE001
            epmc_preprints = []
        for p in epmc_preprints:
            p["specialty"] = specialty
            p["subcategory"] = subcategory
        ingested.extend(epmc_preprints)

    if options.include_trials:
        check_timeout()
        try:
            trials = clinicaltrials.search(trial_query, limit=options.trials_limit)
        except Exception:  # noqa: BLE001
            trials = []
        for t in trials:
            t["specialty"] = specialty
            t["subcategory"] = subcategory
        ingested.extend(trials)

    if options.include_fda_approvals:
        check_timeout()
        try:
            fda_items = fda.search(trial_query, days_back=options.days_back, limit=options.fda_limit)
        except Exception:  # noqa: BLE001
            fda_items = []
        for f in fda_items:
            f["specialty"] = specialty
            f["subcategory"] = subcategory
        ingested.extend(fda_items)

    return ingested


def _finalize_items(
    conn,
    run_id: int,
    ingested: list[dict[str, Any]],
    rules: dict[str, Any],
    options: RunOptions,
    check_timeout,
) -> dict[str, Any]:
    filtered = _apply_filters(ingested, options)
    unique = dedup.deduplicate(filtered)

    persisted = 0
    timed_out = False
    for item in unique:
        try:
            check_timeout()
        except TimeoutError:
            timed_out = True
            break

        item["fingerprint"] = dedup.fingerprint_item(item)
        item["mode_name"] = options.mode_name
        item["full_text_source"] = None
        item["support_snippets"] = []
        if options.use_full_text_oa and item.get("source") in {"pubmed", "europepmc"}:
            fulltext_oa.enrich_item_from_oa_full_text(conn, item)
        if options.enrich_citations:
            citations = openalex.get_citations(conn, item.get("doi"))
            citation_source = "openalex" if citations is not None else None
            if citations is None and options.enable_semantic_scholar:
                citations = semanticscholar.get_citations_by_pmid(item.get("pmid"))
                if citations is not None:
                    citation_source = "semantic_scholar"
            item["citations"] = citations
            item["citations_source"] = citation_source
        else:
            item["citations"] = None
            item["citations_source"] = None

        scoring.score_and_attach(item, rules, weight_overrides=options.scoring_weights)
        item["summary_text"] = summarize.summarize_item(item, llm_polish=options.llm_polish_summary)
        db.upsert_item(conn, item)
        persisted += 1

    status = "timeout" if timed_out else "success"
    db.finish_run(conn, run_id, status, len(ingested), persisted)
    return {
        "run_id": run_id,
        "status": status,
        "ingested_count": len(ingested),
        "deduped_count": persisted,
        "timed_out": timed_out,
    }


def run_pipeline(conn, specialty: str, subcategory: str, options: RunOptions) -> dict[str, Any]:
    resolved_days, _ = resolve_incremental_days_back(conn, specialty, subcategory, options)
    effective_options = RunOptions(**{**vars(options), "days_back": resolved_days})
    run_id = db.create_run(
        conn,
        specialty,
        subcategory,
        mode_name=effective_options.mode_name,
        sources_key=build_sources_key(effective_options),
        resolved_days_back=resolved_days,
        force_full_refresh=effective_options.force_full_refresh,
    )
    started = time.monotonic()

    def _check_timeout() -> None:
        if effective_options.max_run_seconds and (time.monotonic() - started) > effective_options.max_run_seconds:
            raise TimeoutError(f"Timed out after {effective_options.max_run_seconds}s")

    ingested: list[dict[str, Any]] = []
    try:
        rules = packs.get_pack(specialty, subcategory)
        ingested = _ingest_for_query(
            specialty,
            subcategory,
            rules["pubmed_query"],
            rules["trials_query"],
            effective_options,
            _check_timeout,
        )
        return _finalize_items(conn, run_id, ingested, rules, effective_options, _check_timeout)
    except TimeoutError as exc:
        db.finish_run(conn, run_id, "timeout", len(ingested), 0, str(exc))
        return {
            "run_id": run_id,
            "status": "timeout",
            "ingested_count": len(ingested),
            "deduped_count": 0,
            "timed_out": True,
        }
    except Exception as exc:  # noqa: BLE001
        db.finish_run(conn, run_id, "failed", len(ingested), 0, str(exc))
        raise


def run_pipeline_query(conn, query: str, options: RunOptions) -> dict[str, Any]:
    query_text = query.strip()
    if not query_text:
        return {
            "run_id": -1,
            "status": "failed",
            "ingested_count": 0,
            "deduped_count": 0,
            "timed_out": False,
        }

    specialty = "search"
    subcategory = _query_key(query_text)
    resolved_days, _ = resolve_incremental_days_back(conn, specialty, subcategory, options)
    effective_options = RunOptions(**{**vars(options), "days_back": resolved_days})
    # Search mode is always source-driven: clear previous cached records for this query scope.
    db.clear_scope_items(conn, specialty, subcategory)
    run_id = db.create_run(
        conn,
        specialty,
        subcategory,
        mode_name=effective_options.mode_name,
        sources_key=build_sources_key(effective_options),
        resolved_days_back=resolved_days,
        force_full_refresh=effective_options.force_full_refresh,
    )
    started = time.monotonic()

    def _check_timeout() -> None:
        if effective_options.max_run_seconds and (time.monotonic() - started) > effective_options.max_run_seconds:
            raise TimeoutError(f"Timed out after {effective_options.max_run_seconds}s")

    ingested: list[dict[str, Any]] = []
    try:
        query_bundle = nlp.build_search_queries(query_text)
        paper_query = str(query_bundle.get("paper_query") or query_text)
        trial_query = str(query_bundle.get("trial_query") or query_text)
        rules = _default_rules()
        rules["include_terms"] = list(query_bundle.get("keywords") or [])
        rules["search_query_context"] = {
            "raw_query": query_text,
            "keywords": list(query_bundle.get("keywords") or []),
            "concepts": list(query_bundle.get("concepts") or []),
        }

        ingested_raw = _ingest_for_query(
            specialty,
            subcategory,
            paper_query,
            trial_query,
            effective_options,
            _check_timeout,
        )
        query_context = rules.get("search_query_context") or {}
        ingested = [i for i in ingested_raw if _is_search_relevant(i, query_context)]
        return _finalize_items(conn, run_id, ingested, rules, effective_options, _check_timeout)
    except TimeoutError as exc:
        db.finish_run(conn, run_id, "timeout", len(ingested), 0, str(exc))
        return {
            "run_id": run_id,
            "status": "timeout",
            "ingested_count": len(ingested),
            "deduped_count": 0,
            "timed_out": True,
        }
    except Exception as exc:  # noqa: BLE001
        db.finish_run(conn, run_id, "failed", len(ingested), 0, str(exc))
        raise
