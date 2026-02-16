"""Central configuration for the job search pipeline."""

import json
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from ~/.config/job-search-pipeline/.env
_config_dir = Path.home() / ".config" / "job-search-pipeline"
_config_dir.mkdir(parents=True, exist_ok=True)
load_dotenv(_config_dir / ".env")

# ── Paths ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "jobs.db"
RANKINGS_PATH = DATA_DIR / "institution_rankings.json"
CV_KEYWORDS_PATH = DATA_DIR / "cv_keywords.json"

EXCEL_OUTPUT_DIR = (
    Path.home()
    / "Dropbox"
    / "0.Personal folder"
    / "1. CV"
    / "0. Postdoc"
    / "Putative list"
)

# ── Email ──────────────────────────────────────────────────────────────────
GMAIL_ADDRESS = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
REPORT_RECIPIENTS = [
    r.strip()
    for r in os.getenv("REPORT_RECIPIENTS", GMAIL_ADDRESS).split(",")
    if r.strip()
]

# ── Semantic Scholar ───────────────────────────────────────────────────────
SEMANTIC_SCHOLAR_API_KEY = os.getenv("SEMANTIC_SCHOLAR_API_KEY", "")
SEMANTIC_SCHOLAR_RATE_LIMIT = 100  # requests per 5 minutes

# ── Search Keywords ────────────────────────────────────────────────────────
SEARCH_KEYWORDS = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc extremophiles",
    "postdoc metabolic engineering",
    "postdoc genome engineering",
    "postdoc directed evolution",
    "postdoc microbiology",
    "postdoc systems biology",
    "postdoctoral researcher synthetic biology",
    "postdoctoral fellow CRISPR",
    "postdoctoral protein engineering",
]

# Keywords for matching CV to job postings
CV_KEYWORDS = [
    "synthetic biology",
    "CRISPR",
    "Cas9",
    "Cas12",
    "protein engineering",
    "directed evolution",
    "extremophile",
    "thermophile",
    "archaea",
    "metabolic engineering",
    "genome engineering",
    "gene editing",
    "cell-free",
    "biofoundry",
    "high-throughput screening",
    "microbiology",
    "molecular biology",
    "systems biology",
    "bioinformatics",
    "enzyme engineering",
]

# Keywords to detect job openings on lab websites
LAB_HIRING_KEYWORDS = [
    "positions",
    "openings",
    "join us",
    "postdoc",
    "postdoctoral",
    "hiring",
    "opportunities",
    "vacancies",
    "apply",
    "we are looking",
    "job opening",
    "research associate",
]

# ── Region Priority ────────────────────────────────────────────────────────
REGION_PRIORITY = {"US": 1, "EU": 2, "Asia": 3, "Other": 4}

COUNTRY_TO_REGION = {
    # North America
    "United States": "US",
    "USA": "US",
    "US": "US",
    "Canada": "US",  # grouped with US for priority
    # UK
    "United Kingdom": "EU",
    "UK": "EU",
    "England": "EU",
    "Scotland": "EU",
    # Western Europe
    "Germany": "EU",
    "France": "EU",
    "Netherlands": "EU",
    "Switzerland": "EU",
    "Sweden": "EU",
    "Denmark": "EU",
    "Norway": "EU",
    "Finland": "EU",
    "Belgium": "EU",
    "Austria": "EU",
    "Spain": "EU",
    "Italy": "EU",
    "Ireland": "EU",
    "Israel": "EU",
    "Portugal": "EU",
    "Luxembourg": "EU",
    "Iceland": "EU",
    # Central/Eastern Europe (common on EURAXESS)
    "Czech Republic": "EU",
    "Czechia": "EU",
    "Poland": "EU",
    "Hungary": "EU",
    "Romania": "EU",
    "Croatia": "EU",
    "Greece": "EU",
    "Slovenia": "EU",
    "Slovakia": "EU",
    "Estonia": "EU",
    "Latvia": "EU",
    "Lithuania": "EU",
    "Bulgaria": "EU",
    "Cyprus": "EU",
    "Malta": "EU",
    "Serbia": "EU",
    # Direct region values (for guess_country fallback)
    "EU": "EU",
    # Asia
    "South Korea": "Asia",
    "Korea": "Asia",
    "Japan": "Asia",
    "China": "Asia",
    "Singapore": "Asia",
    "Taiwan": "Asia",
    "Hong Kong": "Asia",
    "India": "Asia",
    "Thailand": "Asia",
    "Vietnam": "Asia",
    "Malaysia": "Asia",
    "Indonesia": "Asia",
    "Philippines": "Asia",
    # Other
    "Australia": "Other",
    "New Zealand": "Other",
    "Brazil": "Other",
    "Mexico": "Other",
    "South Africa": "Other",
    "Saudi Arabia": "Other",
}

# ── PI Recommender Weights ─────────────────────────────────────────────────
RECOMMENDER_WEIGHTS = {
    "field_similarity": 0.30,
    "connection_strength": 0.25,
    "institution_ranking": 0.20,
    "h_index": 0.15,
    "recent_activity": 0.10,
}

# ── Institution Rankings (load) ────────────────────────────────────────────
def load_rankings() -> dict:
    """Load institution rankings from JSON file."""
    if RANKINGS_PATH.exists():
        with open(RANKINGS_PATH) as f:
            return json.load(f)
    return {}
