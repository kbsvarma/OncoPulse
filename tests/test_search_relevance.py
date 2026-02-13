from oncopulse.services.run_pipeline import _is_search_relevant


def test_search_relevance_requires_query_overlap():
    ctx = {
        "raw_query": "eye",
        "keywords": ["eye"],
        "concepts": [["eye", "ocular", "vision", "retina"]],
    }

    eye_item = {
        "title": "Ocular toxicity with checkpoint inhibitor therapy",
        "abstract_or_text": "Vision changes and retinal findings were reported.",
        "conditions": "",
        "interventions": "",
        "primary_endpoints": "",
    }
    lung_item = {
        "title": "Early non-small cell lung cancer trial",
        "abstract_or_text": "Phase III randomized trial with OS endpoint.",
        "conditions": "Lung Cancer",
        "interventions": "Atezolizumab",
        "primary_endpoints": "overall survival",
    }

    assert _is_search_relevant(eye_item, ctx) is True
    assert _is_search_relevant(lung_item, ctx) is False
