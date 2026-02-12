from oncopulse.ingest.dedup import deduplicate, fingerprint_item


def test_fingerprint_prefers_doi():
    item = {"doi": "10.1000/abc", "title": "X", "published_at": "2024"}
    assert fingerprint_item(item).startswith("doi:")


def test_dedup_collapses_duplicates():
    items = [
        {"title": "Trial A", "doi": "10.1/x", "published_at": "2024"},
        {"title": "Trial A duplicate", "doi": "10.1/x", "published_at": "2024"},
    ]
    out = deduplicate(items)
    assert len(out) == 1
