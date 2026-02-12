from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Item:
    specialty: str
    subcategory: str
    source: str
    title: str
    url: str
    published_at: Optional[str] = None
    updated_at: Optional[str] = None
    pmid: Optional[str] = None
    doi: Optional[str] = None
    nct_id: Optional[str] = None
    venue: Optional[str] = None
    authors: Optional[str] = None
    abstract_or_text: Optional[str] = None
    score: int = 0
    score_explain_json: str = "[]"
    summary_text: Optional[str] = None
    citations: Optional[int] = None
    citations_source: Optional[str] = None
    fingerprint: Optional[str] = None


@dataclass
class Note:
    item_id: int
    starred: bool = False
    note_text: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
