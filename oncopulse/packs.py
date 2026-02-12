from pathlib import Path
from typing import Any

import yaml

from .config import PACKS_DIR


def _pack_path(specialty: str) -> Path:
    return PACKS_DIR / f"{specialty.strip().lower()}.yaml"


def load_pack_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def list_specialties() -> list[str]:
    if not PACKS_DIR.exists():
        return []
    return sorted(p.stem for p in PACKS_DIR.glob("*.yaml"))


def list_subcategories(specialty: str) -> list[str]:
    pack = load_pack_file(_pack_path(specialty))
    subcats = pack.get("subcategories", [])
    return [s["name"] for s in subcats if "name" in s]


def get_pack(specialty: str, subcategory: str) -> dict[str, Any]:
    pack = load_pack_file(_pack_path(specialty))
    if not pack:
        raise ValueError(f"No pack found for specialty '{specialty}'")

    subcategories = pack.get("subcategories", [])
    subcat = next((s for s in subcategories if s.get("name", "").lower() == subcategory.lower()), None)
    if not subcat:
        raise ValueError(f"No subcategory '{subcategory}' found in specialty '{specialty}'")

    return {
        "specialty": pack.get("specialty", specialty),
        "subcategory": subcat.get("name", subcategory),
        "pubmed_query": subcat.get("pubmed_query", ""),
        "trials_query": subcat.get("trials_query", ""),
        "include_terms": subcat.get("include_terms", []),
        "exclude_terms": subcat.get("exclude_terms", []),
        "global_boost_terms": pack.get("global_boost_terms", []),
        "global_penalty_terms": pack.get("global_penalty_terms", []),
        "major_journals": pack.get("major_journals", []),
    }
