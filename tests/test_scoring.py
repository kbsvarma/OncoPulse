from oncopulse.scoring import score_item


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
