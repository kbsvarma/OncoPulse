from dataclasses import dataclass
import time
from typing import Any

from .. import db, packs, scoring, summarize
from ..ingest import clinicaltrials, dedup, europepmc, fda, openalex, preprints, pubmed, rss_feeds


@dataclass
class RunOptions:
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

    # Backfill missing abstracts when PMID exists (common for some Europe PMC entries).
    missing_pmids = sorted(
        {
            str(i.get("pmid")).strip()
            for i in unique
            if i.get("pmid") and not (i.get("abstract_or_text") or "").strip()
        }
    )
    if missing_pmids:
        try:
            check_timeout()
            pmid_records = pubmed.fetch(missing_pmids[:200])
            abstract_by_pmid = {
                (r.get("pmid") or "").strip(): (r.get("abstract_or_text") or "").strip()
                for r in pmid_records
                if (r.get("pmid") or "").strip()
            }
            for item in unique:
                pmid = (item.get("pmid") or "").strip()
                if pmid and not (item.get("abstract_or_text") or "").strip():
                    candidate = abstract_by_pmid.get(pmid, "")
                    if candidate:
                        item["abstract_or_text"] = candidate
        except Exception:  # noqa: BLE001
            pass

    persisted = 0
    timed_out = False
    for item in unique:
        try:
            check_timeout()
        except TimeoutError:
            timed_out = True
            break

        item["fingerprint"] = dedup.fingerprint_item(item)
        if options.enrich_citations:
            item["citations"] = openalex.get_citations(conn, item.get("doi"))
            item["citations_source"] = "openalex" if item.get("citations") is not None else None
        else:
            item["citations"] = None
            item["citations_source"] = None

        scoring.score_and_attach(item, rules)
        item["summary_text"] = summarize.summarize_item(item)
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
    run_id = db.create_run(conn, specialty, subcategory)
    started = time.monotonic()

    def _check_timeout() -> None:
        if options.max_run_seconds and (time.monotonic() - started) > options.max_run_seconds:
            raise TimeoutError(f"Timed out after {options.max_run_seconds}s")

    ingested: list[dict[str, Any]] = []
    try:
        rules = packs.get_pack(specialty, subcategory)
        ingested = _ingest_for_query(
            specialty,
            subcategory,
            rules["pubmed_query"],
            rules["trials_query"],
            options,
            _check_timeout,
        )
        return _finalize_items(conn, run_id, ingested, rules, options, _check_timeout)
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
    # Search mode is always source-driven: clear previous cached records for this query scope.
    db.clear_scope_items(conn, specialty, subcategory)
    run_id = db.create_run(conn, specialty, subcategory)
    started = time.monotonic()

    def _check_timeout() -> None:
        if options.max_run_seconds and (time.monotonic() - started) > options.max_run_seconds:
            raise TimeoutError(f"Timed out after {options.max_run_seconds}s")

    ingested: list[dict[str, Any]] = []
    try:
        ingested = _ingest_for_query(
            specialty,
            subcategory,
            query_text,
            query_text,
            options,
            _check_timeout,
        )
        return _finalize_items(conn, run_id, ingested, _default_rules(), options, _check_timeout)
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
