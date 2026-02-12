from oncopulse import db


def test_item_mode_name_persisted(tmp_path):
    conn = db.get_conn(str(tmp_path / "oncopulse_test.db"))
    db.init_db(conn)
    item = {
        "specialty": "lung",
        "subcategory": "Immunotherapy",
        "mode_name": "Clinician (Practice-changing)",
        "source": "pubmed",
        "title": "Example",
        "url": "https://example.org",
        "published_at": "2026-02-01",
        "updated_at": "2026-02-01",
        "pmid": "1",
        "doi": "10.1/example",
        "nct_id": None,
        "venue": "NEJM",
        "authors": "A. Author",
        "abstract_or_text": "phase iii randomized overall survival",
        "score": 1,
        "score_explain": ["+1 test"],
        "summary_text": "summary",
        "citations": 1,
        "citations_source": "openalex",
        "fingerprint": "doi:10.1/example",
    }
    db.upsert_item(conn, item)
    rows = db.get_ranked_items(conn, "lung", "Immunotherapy")
    assert rows
    assert rows[0].get("mode_name") == "Clinician (Practice-changing)"
