import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from .config import DEFAULT_DB_PATH


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  specialty TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  UNIQUE(specialty, subcategory)
);

CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  specialty TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  mode_name TEXT,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  url TEXT,
  published_at TEXT,
  updated_at TEXT,
  pmid TEXT,
  doi TEXT,
  nct_id TEXT,
  venue TEXT,
  authors TEXT,
  abstract_or_text TEXT,
  score INTEGER DEFAULT 0,
  score_explain_json TEXT DEFAULT '[]',
  summary_text TEXT,
  citations INTEGER,
  citations_source TEXT,
  fingerprint TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  starred INTEGER NOT NULL DEFAULT 0,
  note_text TEXT DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(item_id) REFERENCES items(id)
);

CREATE TABLE IF NOT EXISTS citation_cache (
  doi TEXT PRIMARY KEY,
  cited_by_count INTEGER,
  fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  specialty TEXT NOT NULL,
  subcategory TEXT NOT NULL,
  mode_name TEXT,
  sources_key TEXT,
  resolved_days_back INTEGER,
  force_full_refresh INTEGER DEFAULT 0,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,
  ingested_count INTEGER DEFAULT 0,
  deduped_count INTEGER DEFAULT 0,
  error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_items_specialty_subcategory ON items(specialty, subcategory);
CREATE INDEX IF NOT EXISTS idx_items_source ON items(source);
CREATE INDEX IF NOT EXISTS idx_items_published_at ON items(published_at);
"""


def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    path = db_path or DEFAULT_DB_PATH
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(items)").fetchall()}
    if "mode_name" not in columns:
        conn.execute("ALTER TABLE items ADD COLUMN mode_name TEXT")
    run_cols = {row["name"] for row in conn.execute("PRAGMA table_info(run_history)").fetchall()}
    if "mode_name" not in run_cols:
        conn.execute("ALTER TABLE run_history ADD COLUMN mode_name TEXT")
    if "sources_key" not in run_cols:
        conn.execute("ALTER TABLE run_history ADD COLUMN sources_key TEXT")
    if "resolved_days_back" not in run_cols:
        conn.execute("ALTER TABLE run_history ADD COLUMN resolved_days_back INTEGER")
    if "force_full_refresh" not in run_cols:
        conn.execute("ALTER TABLE run_history ADD COLUMN force_full_refresh INTEGER DEFAULT 0")
    conn.commit()


def upsert_item(conn: sqlite3.Connection, item: dict[str, Any]) -> int:
    now = _now_iso_utc()
    payload = {
        "specialty": item.get("specialty", ""),
        "subcategory": item.get("subcategory", ""),
        "mode_name": item.get("mode_name"),
        "source": item.get("source", ""),
        "title": item.get("title", ""),
        "url": item.get("url"),
        "published_at": item.get("published_at"),
        "updated_at": item.get("updated_at"),
        "pmid": item.get("pmid"),
        "doi": item.get("doi"),
        "nct_id": item.get("nct_id"),
        "venue": item.get("venue"),
        "authors": item.get("authors"),
        "abstract_or_text": item.get("abstract_or_text"),
        "score": int(item.get("score", 0)),
        "score_explain_json": json.dumps(item.get("score_explain", [])),
        "summary_text": item.get("summary_text"),
        "citations": item.get("citations"),
        "citations_source": item.get("citations_source"),
        "fingerprint": item.get("fingerprint"),
    }

    conn.execute(
        """
        INSERT INTO items (
          specialty, subcategory, mode_name, source, title, url, published_at, updated_at,
          pmid, doi, nct_id, venue, authors, abstract_or_text,
          score, score_explain_json, summary_text,
          citations, citations_source, fingerprint,
          created_at, last_seen_at
        ) VALUES (
          :specialty, :subcategory, :mode_name, :source, :title, :url, :published_at, :updated_at,
          :pmid, :doi, :nct_id, :venue, :authors, :abstract_or_text,
          :score, :score_explain_json, :summary_text,
          :citations, :citations_source, :fingerprint,
          :created_at, :last_seen_at
        )
        ON CONFLICT(fingerprint) DO UPDATE SET
          specialty=excluded.specialty,
          subcategory=excluded.subcategory,
          mode_name=excluded.mode_name,
          source=excluded.source,
          title=excluded.title,
          url=excluded.url,
          published_at=excluded.published_at,
          updated_at=excluded.updated_at,
          pmid=excluded.pmid,
          doi=excluded.doi,
          nct_id=excluded.nct_id,
          venue=excluded.venue,
          authors=excluded.authors,
          abstract_or_text=excluded.abstract_or_text,
          score=excluded.score,
          score_explain_json=excluded.score_explain_json,
          summary_text=excluded.summary_text,
          citations=excluded.citations,
          citations_source=excluded.citations_source,
          last_seen_at=excluded.last_seen_at
        """,
        {**payload, "created_at": now, "last_seen_at": now},
    )

    row = conn.execute("SELECT id FROM items WHERE fingerprint = ?", (payload["fingerprint"],)).fetchone()
    conn.commit()
    return int(row["id"])


def get_items(conn: sqlite3.Connection, specialty: str, subcategory: str, source: Optional[str] = None) -> list[dict[str, Any]]:
    sql = "SELECT * FROM items WHERE specialty = ? AND subcategory = ?"
    params: list[Any] = [specialty, subcategory]
    if source:
        sql += " AND source = ?"
        params.append(source)
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def clear_scope_items(conn: sqlite3.Connection, specialty: str, subcategory: str) -> None:
    rows = conn.execute("SELECT id FROM items WHERE specialty = ? AND subcategory = ?", (specialty, subcategory)).fetchall()
    item_ids = [int(r["id"]) for r in rows]
    if item_ids:
        placeholders = ",".join("?" for _ in item_ids)
        conn.execute(f"DELETE FROM notes WHERE item_id IN ({placeholders})", item_ids)
    conn.execute("DELETE FROM items WHERE specialty = ? AND subcategory = ?", (specialty, subcategory))
    conn.commit()


def clear_all_local_cache(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM notes")
    conn.execute("DELETE FROM items")
    conn.execute("DELETE FROM citation_cache")
    conn.execute("DELETE FROM run_history")
    conn.execute("DELETE FROM topics")
    conn.commit()


def get_ranked_items(
    conn: sqlite3.Connection,
    specialty: str,
    subcategory: str,
    mode: str = "new",
    include_trials: bool = True,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM items WHERE specialty = ? AND subcategory = ?"
    params: list[Any] = [specialty, subcategory]
    if not include_trials:
        sql += " AND source != 'clinicaltrials'"
    if mode == "cited":
        sql += " ORDER BY COALESCE(citations, 0) DESC, score DESC, COALESCE(published_at, '') DESC"
    else:
        sql += " ORDER BY score DESC, COALESCE(published_at, '') DESC"
    sql += " LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def upsert_note(conn: sqlite3.Connection, item_id: int, starred: bool, note_text: str) -> None:
    now = _now_iso_utc()
    existing = conn.execute("SELECT id FROM notes WHERE item_id = ?", (item_id,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE notes SET starred = ?, note_text = ?, updated_at = ? WHERE item_id = ?",
            (1 if starred else 0, note_text, now, item_id),
        )
    else:
        conn.execute(
            "INSERT INTO notes (item_id, starred, note_text, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (item_id, 1 if starred else 0, note_text, now, now),
        )
    conn.commit()


def get_note(conn: sqlite3.Connection, item_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM notes WHERE item_id = ?", (item_id,)).fetchone()
    return dict(row) if row else None


def get_cached_citation(conn: sqlite3.Connection, doi: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM citation_cache WHERE doi = ?", (doi.lower(),)).fetchone()
    return dict(row) if row else None


def set_cached_citation(conn: sqlite3.Connection, doi: str, cited_by_count: Optional[int]) -> None:
    now = _now_iso_utc()
    conn.execute(
        "INSERT INTO citation_cache (doi, cited_by_count, fetched_at) VALUES (?, ?, ?) ON CONFLICT(doi) DO UPDATE SET cited_by_count = excluded.cited_by_count, fetched_at = excluded.fetched_at",
        (doi.lower(), cited_by_count, now),
    )
    conn.commit()


def create_run(
    conn: sqlite3.Connection,
    specialty: str,
    subcategory: str,
    mode_name: Optional[str] = None,
    sources_key: Optional[str] = None,
    resolved_days_back: Optional[int] = None,
    force_full_refresh: bool = False,
) -> int:
    now = _now_iso_utc()
    cur = conn.execute(
        """
        INSERT INTO run_history
        (specialty, subcategory, mode_name, sources_key, resolved_days_back, force_full_refresh, started_at, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            specialty,
            subcategory,
            mode_name,
            sources_key,
            resolved_days_back,
            1 if force_full_refresh else 0,
            now,
            "running",
        ),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_last_successful_run(
    conn: sqlite3.Connection,
    specialty: str,
    subcategory: str,
    mode_name: Optional[str] = None,
    sources_key: Optional[str] = None,
) -> dict[str, Any] | None:
    sql = """
      SELECT *
      FROM run_history
      WHERE specialty = ? AND subcategory = ? AND status = 'success'
    """
    params: list[Any] = [specialty, subcategory]
    if mode_name is not None:
        sql += " AND mode_name = ?"
        params.append(mode_name)
    if sources_key is not None:
        sql += " AND sources_key = ?"
        params.append(sources_key)
    sql += " ORDER BY COALESCE(finished_at, started_at) DESC LIMIT 1"
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    status: str,
    ingested_count: int,
    deduped_count: int,
    error_text: Optional[str] = None,
) -> None:
    now = _now_iso_utc()
    conn.execute(
        "UPDATE run_history SET finished_at = ?, status = ?, ingested_count = ?, deduped_count = ?, error_text = ? WHERE id = ?",
        (now, status, ingested_count, deduped_count, error_text, run_id),
    )
    conn.commit()
