from oncopulse import db
from oncopulse.ingest import fulltext_oa


SAMPLE_JATS = """<?xml version="1.0" encoding="UTF-8"?>
<article>
  <front>
    <article-meta>
      <abstract>
        <p>This randomized phase III study in NSCLC reported overall survival.</p>
      </abstract>
    </article-meta>
  </front>
  <body>
    <sec>
      <title>Methods</title>
      <p>Patients were assigned to pembrolizumab versus chemotherapy.</p>
    </sec>
    <sec>
      <title>Results</title>
      <p>Overall survival and progression-free survival were assessed.</p>
    </sec>
    <sec>
      <title>Discussion</title>
      <p>The findings suggest a clinically meaningful efficacy signal.</p>
    </sec>
    <sec>
      <title>Conclusion</title>
      <p>The trial met key efficacy endpoints in the study population.</p>
    </sec>
    <fig><caption><title>Figure 1</title><p>Kaplan-Meier survival curves.</p></caption></fig>
  </body>
</article>
"""


def test_fulltext_oa_enrich_item_and_cache(monkeypatch, tmp_path):
    conn = db.get_conn(str(tmp_path / "ft.db"))
    db.init_db(conn)

    monkeypatch.setattr(fulltext_oa, "_resolve_pmcid", lambda item: "PMC12345")
    monkeypatch.setattr(fulltext_oa, "_is_pmc_oa", lambda pmcid: True)
    monkeypatch.setattr(fulltext_oa, "_fetch_pmc_xml", lambda pmcid: SAMPLE_JATS)

    item = {
        "source": "pubmed",
        "pmid": "123",
        "doi": "10.1000/test",
        "abstract_or_text": "Short abstract",
    }
    fulltext_oa.enrich_item_from_oa_full_text(conn, item)

    assert item.get("full_text_source") == "PMC"
    assert "overall survival" in (item.get("full_text_text") or "").lower()
    assert item.get("support_snippets")

    # second pass should use cache and not require refetch
    monkeypatch.setattr(fulltext_oa, "_fetch_pmc_xml", lambda pmcid: None)
    item2 = {"source": "pubmed", "pmid": "123", "doi": "10.1000/test", "abstract_or_text": ""}
    fulltext_oa.enrich_item_from_oa_full_text(conn, item2)
    assert item2.get("full_text_source") == "PMC"
    assert item2.get("support_snippets")
