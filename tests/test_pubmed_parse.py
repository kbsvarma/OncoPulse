from oncopulse.ingest.pubmed import parse_pubmed_xml


SAMPLE = """<?xml version=\"1.0\"?>
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID>12345678</PMID>
      <Article>
        <ArticleTitle>Phase III randomized trial in NSCLC</ArticleTitle>
        <Abstract>
          <AbstractText Label=\"Background\">Patients with NSCLC were enrolled.</AbstractText>
          <AbstractText Label=\"Results\">Overall survival improved.</AbstractText>
        </Abstract>
        <Journal>
          <ISOAbbreviation>J Clin Oncol</ISOAbbreviation>
          <JournalIssue>
            <PubDate><Year>2025</Year><Month>01</Month><Day>12</Day></PubDate>
          </JournalIssue>
        </Journal>
      </Article>
    </MedlineCitation>
    <PubmedData>
      <ArticleIdList>
        <ArticleId IdType=\"doi\">10.1000/test</ArticleId>
      </ArticleIdList>
    </PubmedData>
  </PubmedArticle>
</PubmedArticleSet>
"""


def test_parse_pubmed_xml_extracts_core_fields():
    items = parse_pubmed_xml(SAMPLE)
    assert len(items) == 1
    item = items[0]
    assert item["pmid"] == "12345678"
    assert item["doi"] == "10.1000/test"
    assert "Overall survival improved" in item["abstract_or_text"]
    assert item["venue"] == "J Clin Oncol"
