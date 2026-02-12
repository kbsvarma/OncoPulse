from oncopulse.summarize import BANNED_PRESCRIPTIVE_PHRASES, summarize_item


def test_summary_omits_numeric_claims_in_key_fields():
    item = {
        "title": "Randomized trial",
        "abstract_or_text": (
            "A total of 324 patients with NSCLC were enrolled. "
            "Patients received pembrolizumab versus chemotherapy. "
            "Median overall survival was 23.1 months with pembrolizumab and 14.5 months with chemotherapy."
        ),
    }
    summary = summarize_item(item)
    assert "Population:" in summary
    assert "Intervention vs comparator:" in summary
    assert "23.1" not in summary
    assert "14.5" not in summary
    assert "324" not in summary


def test_summary_has_no_abstract_fallback():
    item = {"title": "No abstract", "abstract_or_text": ""}
    summary = summarize_item(item)
    assert "Key finding: No abstract available" in summary
    assert "Why it matters: Not enough info in abstract." in summary


def test_summary_includes_why_it_matters_line():
    item = {
        "title": "Phase III RCT",
        "abstract_or_text": (
            "This randomized phase III trial in NSCLC reported overall survival and progression-free survival. "
            "Adverse events were monitored."
        ),
        "status": "Recruiting",
    }
    summary = summarize_item(item)
    assert "Why it matters:" in summary


def test_summary_avoids_prescribing_language():
    item = {
        "title": "Evidence update",
        "abstract_or_text": "Randomized phase III evidence in adults with reported overall survival.",
    }
    summary = summarize_item(item).lower()
    for phrase in BANNED_PRESCRIPTIVE_PHRASES:
        assert phrase not in summary


def test_summary_llm_polish_uses_polished_when_valid(monkeypatch):
    item = {
        "title": "Phase III RCT",
        "abstract_or_text": "Randomized phase III trial in NSCLC with overall survival endpoint.",
        "support_snippets": ["Randomized phase III trial in NSCLC with overall survival endpoint."],
    }

    def fake_polish(summary, source_text, support_snippets):
        return (
            "Study type / phase: Randomized trial\n"
            "Population: Not stated\n"
            "Intervention vs comparator: Not stated\n"
            "Endpoints mentioned: overall survival\n"
            "Key finding: Not explicitly stated in provided text\n"
            "Supporting snippets: Randomized phase III trial in NSCLC with overall survival endpoint.\n"
            "Why it matters: Study signal exists."
        )

    monkeypatch.setattr("oncopulse.summarize.llm.polish_summary_strict", fake_polish)
    summary = summarize_item(item, llm_polish=True)
    assert "Why it matters: Study signal exists." in summary


def test_summary_llm_polish_falls_back_when_unavailable(monkeypatch):
    item = {
        "title": "Phase III RCT",
        "abstract_or_text": "Randomized phase III trial in NSCLC with overall survival endpoint.",
    }
    monkeypatch.setattr("oncopulse.summarize.llm.polish_summary_strict", lambda *args, **kwargs: None)
    summary = summarize_item(item, llm_polish=True)
    assert "Study type / phase:" in summary
    assert "Why it matters:" in summary
