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
SEMANTIC_SCHOLAR_BASE = "https://api.semanticscholar.org/graph/v1/paper"
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY")

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

PMC_IDCONV_BASE = "https://pmc.ncbi.nlm.nih.gov/tools/idconv/api/v1/articles/"
PMC_OA_BASE = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"
EUROPE_PMC_REST_BASE = "https://www.ebi.ac.uk/europepmc/webservices/rest"
FULLTEXT_CACHE_TTL_DAYS = int(os.getenv("FULLTEXT_CACHE_TTL_DAYS", "7"))

OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ONCOPULSE_LLM_MODEL = os.getenv("ONCOPULSE_LLM_MODEL", "gpt-4o-mini")
LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "12"))
