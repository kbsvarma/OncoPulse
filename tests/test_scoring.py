from datetime import datetime, timezone

from oncopulse.scoring import citations_per_year, hot_score, score_item


PACK_RULES = {
    "major_journals": ["NEJM"],
    "global_penalty_terms": ["case report", "mouse"],
    "include_terms": [],
    "exclude_terms": [],
}


def test_phase_iii_beats_phase_ii():
    item_iii = {
        "title": "A phase III randomized trial",
        "abstract_or_text": "overall survival improved",
        "venue": "NEJM",
        "citations": 0,
    }
    item_ii = {
        "title": "A phase II trial",
        "abstract_or_text": "overall survival improved",
        "venue": "NEJM",
        "citations": 0,
    }
    s3, _ = score_item(item_iii, PACK_RULES)
    s2, _ = score_item(item_ii, PACK_RULES)
    assert s3 > s2


def test_preclinical_penalty_applies():
    item = {
        "title": "Murine cell line model",
        "abstract_or_text": "in vitro mouse data",
        "venue": "Unknown",
        "citations": 0,
    }
    score, explain = score_item(item, PACK_RULES)
    assert score < 0
    assert any("preclinical" in e for e in explain)


def test_weight_override_changes_phase_iii_priority():
    item = {
        "title": "A phase III randomized trial",
        "abstract_or_text": "overall survival improved",
        "venue": "NEJM",
        "citations": 0,
    }
    base_score, _ = score_item(item, PACK_RULES)
    boosted_score, explain = score_item(item, PACK_RULES, weight_overrides={"phase_iii": 12})
    assert boosted_score > base_score
    assert any("+12 phase iii" in e for e in explain)


def test_citations_per_year_is_computed():
    item = {"citations": 24, "published_at": "2024-01-01"}
    rate = citations_per_year(item, now=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert rate is not None
    assert rate > 0


def test_hot_score_prefers_recent_with_similar_citations():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    older = {"citations": 20, "published_at": "2020-01-01"}
    newer = {"citations": 20, "published_at": "2025-12-01"}
    assert hot_score(newer, now=now) > hot_score(older, now=now)
