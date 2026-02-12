from oncopulse import nlp


def test_build_search_queries_expands_common_oncology_terms():
    out = nlp.build_search_queries("metastatic NSCLC pembrolizumab phase 3 OS")
    paper_query = str(out["paper_query"]).lower()
    trial_query = str(out["trial_query"]).lower()

    assert "nsclc" in paper_query
    assert "non-small cell lung cancer" in paper_query
    assert "overall survival" in paper_query
    assert "nsclc" in trial_query
    assert "pembrolizumab" in trial_query


def test_extract_keywords_removes_stopwords_and_keeps_signal():
    terms = nlp.extract_keywords("in oncology and cancer with pembrolizumab randomized survival")
    assert "pembrolizumab" in terms
    assert "randomized" in terms
    assert "oncology" not in terms
    assert "cancer" not in terms
