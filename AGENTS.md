# OncoPulse Agent Guide

## Project Goal
Build an ad hoc oncology research inbox that lets clinicians choose specialty/subcategory/time window and fetch ranked, transparent, practice-relevant items from public sources.

## Non-Goals (V1)
- No outbound email workflows
- No scraping paywalled PDFs or restricted conference portals
- No EHR integration and no PHI handling
- No black-box ranking model; ranking must remain rule-based and auditable

## Data Sources and Legal Scope
- Allowed sources: PubMed abstracts/metadata, ClinicalTrials.gov API v2 fields, OpenAlex citation counts
- If full text is paywalled or restricted, only link out; do not scrape
- Respect each source's usage limits and terms

## Coding Conventions
- Python 3.11+
- Keep modules small and single-purpose
- Prefer explicit typing and dataclasses/typed dicts for normalized payloads
- Keep side effects in service layers, not pure utility modules
- Write tests for scoring, dedup, and parser behavior
- Keep logs clear and non-sensitive

## Strict Summary Rules
- Summaries must use only retrieved source text and metadata
- Never fabricate efficacy/safety numbers, endpoints, or conclusions
- If a field is absent, state "Not stated" (or "No abstract available" fallback)
- Keep wording conservative and non-prescriptive

## Data Schema Notes
Main V1 SQLite tables:
- `items`: unified records from PubMed and ClinicalTrials.gov
- `notes`: local stars/notes (no PHI)
- `citation_cache`: DOI -> OpenAlex cited_by_count with fetched timestamp
- `run_history`: tracks manual pipeline runs and counts
- `topics`: optional specialty/subcategory registry

Dedup identity order:
1. DOI
2. PMID
3. NCT ID
4. normalized title + publication year bucket fingerprint
