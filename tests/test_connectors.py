from types import SimpleNamespace

from oncopulse import db
from oncopulse.ingest import clinicaltrials, openalex, pubmed


class FakeResp:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http {self.status_code}")


def test_pubmed_search_paginates(monkeypatch):
    calls = []

    def fake_request(url, params, session=None):
        calls.append(params["retstart"])
        if params["retstart"] == 0:
            return FakeResp({"esearchresult": {"count": "5", "idlist": ["1", "2", "3"]}})
        return FakeResp({"esearchresult": {"count": "5", "idlist": ["4", "5"]}})

    monkeypatch.setattr(pubmed, "_request_with_retry", fake_request)
    ids = pubmed.search("nsclc", days_back=7, retmax=5)

    assert ids == ["1", "2", "3", "4", "5"]
    assert calls == [0, 3]


def test_clinicaltrials_search_paginates(monkeypatch):
    class FakeCTResp:
        def __init__(self, payload):
            self._payload = payload
            self.status_code = 200

        def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    page1 = {
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000001", "briefTitle": "Study 1"},
                    "statusModule": {"lastUpdatePostDateStruct": {"date": "2025-01-01"}, "overallStatus": "RECRUITING"},
                    "conditionsModule": {"conditions": ["NSCLC"]},
                    "armsInterventionsModule": {"interventions": [{"name": "Drug A"}]},
                    "descriptionModule": {"briefSummary": "Summary 1"},
                    "designModule": {"studyType": "INTERVENTIONAL", "phases": ["PHASE3"]},
                    "outcomesModule": {"primaryOutcomes": [{"measure": "Overall Survival"}]},
                }
            }
        ],
        "nextPageToken": "token2",
    }
    page2 = {
        "studies": [
            {
                "protocolSection": {
                    "identificationModule": {"nctId": "NCT00000002", "briefTitle": "Study 2"},
                    "statusModule": {"lastUpdatePostDateStruct": {"date": "2025-01-02"}},
                }
            }
        ]
    }

    state = {"count": 0}

    def fake_get(url, params, timeout):
        state["count"] += 1
        if state["count"] == 1:
            assert "pageToken" not in params
            return FakeCTResp(page1)
        assert params.get("pageToken") == "token2"
        return FakeCTResp(page2)

    monkeypatch.setattr(clinicaltrials.requests, "get", fake_get)
    items = clinicaltrials.search("nsclc", limit=2)

    assert len(items) == 2
    assert items[0]["nct_id"] == "NCT00000001"
    assert items[0]["phase"] == "PHASE3"
    assert items[0]["primary_endpoints"] == "Overall Survival"
    assert items[1]["nct_id"] == "NCT00000002"


def test_openalex_normalizes_doi_and_caches(monkeypatch):
    conn = db.get_conn(":memory:")
    db.init_db(conn)

    captured = {"url": ""}

    class FakeOAResp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {"cited_by_count": 42}

    def fake_get(url, timeout, params=None):
        captured["url"] = url
        return FakeOAResp()

    monkeypatch.setattr(openalex.requests, "get", fake_get)
    count = openalex.get_citations(conn, "https://doi.org/10.1000/XYZ")

    assert count == 42
    assert "10.1000%2Fxyz" in captured["url"]

    # cached path (no network call)
    monkeypatch.setattr(openalex.requests, "get", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not call")))
    count2 = openalex.get_citations(conn, "doi:10.1000/xyz")
    assert count2 == 42
