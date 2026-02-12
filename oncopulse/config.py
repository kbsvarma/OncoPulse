import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
PACKS_DIR = BASE_DIR / "packs"
DEFAULT_DB_PATH = os.getenv("ONCOPULSE_DB_PATH", str(BASE_DIR / "oncopulse.db"))

NCBI_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_API_KEY = os.getenv("NCBI_API_KEY")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "oncopulse@example.com")
NCBI_TOOL = os.getenv("NCBI_TOOL", "OncoPulse")

CTGOV_V2_BASE = "https://clinicaltrials.gov/api/v2/studies"
OPENALEX_BASE = "https://api.openalex.org/works"
EUROPE_PMC_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
BIORXIV_BASE = "https://api.biorxiv.org"
MEDRXIV_BASE = "https://api.medrxiv.org"
FDA_DRUGS_BASE = "https://api.fda.gov/drug/drugsfda.json"

JOURNAL_RSS_FEEDS = [
    "https://www.nejm.org/rss/current.xml",
    "https://ascopubs.org/action/showFeed?jc=jco&type=etoc&feed=rss",
    "https://www.thelancet.com/rssfeed/lancet_online.xml",
    "https://ashpublications.org/rss/site_1000003/1000003.xml",
]

CITATION_CACHE_TTL_DAYS = 14
REQUEST_TIMEOUT = 8
MAX_RETRIES = 2
BACKOFF_SECONDS = 0.8
