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

EXCEL_OUTPUT_DIR = Path(os.getenv(
    "EXCEL_OUTPUT_DIR",
    str(Path.home() / "Dropbox/0.Personal folder/1. CV/0. Postdoc"),
))

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
    # Field-specific
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
    # Institution-specific (premier UK/EU research institutes)
    "postdoc Sanger Institute",
    "postdoc Oxford biology",
    "postdoc Cambridge biology",
    "postdoc EMBL",
    "postdoc EBI Hinxton",
    "postdoc Francis Crick Institute",
    "postdoc Max Planck biology",
    "postdoc ETH Zurich biology",
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
    "microbiome",
]

# Keywords to EXCLUDE — fields irrelevant to the user's background
EXCLUDE_KEYWORDS = [
    "neuroscience",
    "neurobiology",
    "neuroimaging",
    "neurodegeneration",
    "psychiatry",
    "psychology",
    "cognitive science",
    "optogenetics",
    "electrophysiology",
    "patch clamp",
    # Stem cell / regenerative
    "stem cell",
    "stem cells",
    # Cancer / Immunology
    "cancer biology",
    "oncology",
    "tumor",
    "tumour",
    "immunology",
    "immunotherapy",
    "antibody engineering",
    "CAR-T",
    # PhD positions (not postdoc)
    "PhD position",
    "PhD student",
    "doctoral position",
    "doctoral student",
    "PhD candidate",
    "doctoral candidate",
    "PhD fellowship",
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

# ── Institute → Country (for region inference when scraper misses country) ─
# Pattern-based rules + explicit overrides for known institutions.
# Keys are lowercased substrings matched against institute names.
INSTITUTE_COUNTRY_RULES: list[tuple[str, str]] = [
    # --- US universities & institutes ---
    ("harvard", "United States"),
    ("johns hopkins", "United States"),
    ("stanford", "United States"),
    ("mit", "United States"),
    ("massachusetts institute of technology", "United States"),
    ("caltech", "United States"),
    ("california institute of technology", "United States"),
    ("cornell", "United States"),
    ("columbia university", "United States"),
    ("yale", "United States"),
    ("princeton", "United States"),
    ("duke university", "United States"),
    ("northwestern university", "United States"),
    ("vanderbilt", "United States"),
    ("emory", "United States"),
    ("rice university", "United States"),
    ("tufts university", "United States"),
    ("rutgers", "United States"),
    ("northeastern university", "United States"),
    ("georgia tech", "United States"),
    ("georgia institute of technology", "United States"),
    ("mayo clinic", "United States"),
    ("baylor", "United States"),
    ("iowa state", "United States"),
    ("kennesaw state", "United States"),
    ("atrium health", "United States"),
    ("scripps research", "United States"),
    ("broad institute", "United States"),
    ("whitehead institute", "United States"),
    ("salk institute", "United States"),
    ("cold spring harbor", "United States"),
    ("j. craig venter", "United States"),
    ("hhmi", "United States"),
    ("howard hughes", "United States"),
    ("nih", "United States"),
    ("national institutes of health", "United States"),
    ("nist", "United States"),
    ("fred hutch", "United States"),
    ("fred hutchinson", "United States"),
    ("md anderson", "United States"),
    ("memorial sloan kettering", "United States"),
    ("dana-farber", "United States"),
    ("mass general", "United States"),
    ("st. jude", "United States"),
    ("city of hope", "United States"),
    ("cedars-sinai", "United States"),
    ("mount sinai", "United States"),
    ("boston children", "United States"),
    ("seattle children", "United States"),
    ("buck institute", "United States"),
    ("new york genome center", "United States"),
    ("nygc", "United States"),
    ("ucsf", "United States"),
    ("ucla", "United States"),
    ("ucsd", "United States"),
    ("uc berkeley", "United States"),
    ("uc irvine", "United States"),
    ("uc riverside", "United States"),
    ("uc san diego", "United States"),
    ("uc san francisco", "United States"),
    ("university of california", "United States"),
    ("university of washington", "United States"),
    ("university of pennsylvania", "United States"),
    ("university of michigan", "United States"),
    ("washington university in st. louis", "United States"),
    ("university of wisconsin", "United States"),
    ("university of minnesota", "United States"),
    ("university of pittsburgh", "United States"),
    ("university of north carolina", "United States"),
    ("unc school of medicine", "United States"),
    ("university of arizona", "United States"),
    ("university of utah", "United States"),
    ("university of miami", "United States"),
    ("university of georgia", "United States"),
    ("university of illinois", "United States"),
    ("uiuc", "United States"),
    ("texas a&m", "United States"),
    ("north carolina state", "United States"),
    ("oregon health", "United States"),
    ("ohsu", "United States"),
    ("nyu", "United States"),
    ("usc", "United States"),
    ("notre dame", "United States"),
    ("uthealth", "United States"),
    ("utexas", "United States"),
    ("university of houston", "United States"),
    ("university of kansas", "United States"),
    ("university of arkansas", "United States"),
    ("university of missouri", "United States"),
    ("university of maine", "United States"),
    ("indiana university", "United States"),
    ("george washington university", "United States"),
    ("tulane university", "United States"),
    ("augusta university", "United States"),
    ("beth israel", "United States"),
    ("genentech", "United States"),
    ("calico", "United States"),
    ("arc institute", "United States"),
    ("twist bioscience", "United States"),
    ("illumina", "United States"),
    ("ginkgo bioworks", "United States"),
    ("10x genomics", "United States"),
    ("amgen", "United States"),
    ("gilead", "United States"),
    ("regeneron", "United States"),
    ("terasaki institute", "United States"),
    ("ellison institute", "United States"),
    ("nrel", "United States"),
    ("national renewable energy", "United States"),
    ("lawrence livermore", "United States"),
    ("llnl", "United States"),
    ("pacific northwest national", "United States"),
    ("pnnl", "United States"),
    # --- Canada ---
    ("university of toronto", "Canada"),
    ("university of british columbia", "Canada"),
    ("mcgill", "Canada"),
    ("mcmaster", "Canada"),
    # --- UK ---
    ("university of oxford", "United Kingdom"),
    ("oxford university", "United Kingdom"),
    ("university of cambridge", "United Kingdom"),
    ("cambridge university", "United Kingdom"),
    ("imperial college", "United Kingdom"),
    ("university college london", "United Kingdom"),
    ("ucl", "United Kingdom"),
    ("king's college london", "United Kingdom"),
    ("university of edinburgh", "United Kingdom"),
    ("university of manchester", "United Kingdom"),
    ("university of exeter", "United Kingdom"),
    ("university of nottingham", "United Kingdom"),
    ("wellcome sanger", "United Kingdom"),
    ("sanger institute", "United Kingdom"),
    ("francis crick", "United Kingdom"),
    ("mrc laboratory of molecular biology", "United Kingdom"),
    ("babraham institute", "United Kingdom"),
    ("john innes centre", "United Kingdom"),
    ("institute of cancer research", "United Kingdom"),
    # --- Germany ---
    ("max planck", "Germany"),
    ("helmholtz", "Germany"),
    ("max delbruck", "Germany"),
    ("max delbrueck", "Germany"),
    ("heidelberg university", "Germany"),
    ("ludwig maximilian", "Germany"),
    ("lmu munich", "Germany"),
    ("technical university of munich", "Germany"),
    ("tu munich", "Germany"),
    ("tum ", "Germany"),
    ("university of freiburg", "Germany"),
    ("university of cologne", "Germany"),
    ("university of hamburg", "Germany"),
    ("free university of berlin", "Germany"),
    ("freie universit", "Germany"),
    ("leibniz", "Germany"),
    ("friedrich schiller", "Germany"),
    ("karlsruhe", "Germany"),
    # --- France ---
    ("sorbonne", "France"),
    ("cnrs", "France"),
    ("inserm", "France"),
    ("institut pasteur", "France"),
    ("universit\u00e9 de", "France"),
    ("universite de", "France"),
    ("aix-marseille", "France"),
    ("lyon", "France"),
    # --- Switzerland ---
    ("eth zurich", "Switzerland"),
    ("epfl", "Switzerland"),
    ("university of zurich", "Switzerland"),
    ("university of basel", "Switzerland"),
    ("university of geneva", "Switzerland"),
    # --- Netherlands ---
    ("university of amsterdam", "Netherlands"),
    ("university of groningen", "Netherlands"),
    ("leiden university", "Netherlands"),
    ("utrecht university", "Netherlands"),
    ("erasmus mc", "Netherlands"),
    ("amsterdam umc", "Netherlands"),
    ("wageningen", "Netherlands"),
    ("delft university", "Netherlands"),
    ("tu delft", "Netherlands"),
    ("eindhoven university", "Netherlands"),
    # --- Belgium ---
    ("ku leuven", "Belgium"),
    ("ghent university", "Belgium"),
    ("university of antwerp", "Belgium"),
    ("vib", "Belgium"),
    ("universit\u00e9 libre de bruxelles", "Belgium"),
    ("vrije universiteit brussel", "Belgium"),
    # --- Scandinavia ---
    ("karolinska", "Sweden"),
    ("lund university", "Sweden"),
    ("uppsala university", "Sweden"),
    ("university of gothenburg", "Sweden"),
    ("chalmers", "Sweden"),
    ("scilife", "Sweden"),
    ("slu", "Sweden"),
    ("linnaeus", "Sweden"),
    ("ume\u00e5 university", "Sweden"),
    ("umea university", "Sweden"),
    ("umeaa university", "Sweden"),
    ("aalto university", "Finland"),
    ("university of helsinki", "Finland"),
    ("tampere university", "Finland"),
    ("university of oulu", "Finland"),
    ("university of copenhagen", "Denmark"),
    ("aarhus university", "Denmark"),
    ("technical university of denmark", "Denmark"),
    ("dtu", "Denmark"),
    ("university of southern denmark", "Denmark"),
    ("university of oslo", "Norway"),
    ("simula", "Norway"),
    # --- Other EU ---
    ("university of vienna", "Austria"),
    ("medical university of vienna", "Austria"),
    ("university of graz", "Austria"),
    ("cemm", "Austria"),
    ("university college dublin", "Ireland"),
    ("university of luxembourg", "Luxembourg"),
    ("universitat pompeu fabra", "Spain"),
    ("universidad de sevilla", "Spain"),
    ("university of bologna", "Italy"),
    ("university of tartu", "Estonia"),
    ("tallinn university", "Estonia"),
    ("university of thessaly", "Greece"),
    ("adam mickiewicz", "Poland"),
    ("jagiellonian", "Poland"),
    # --- Asia ---
    ("seoul national university", "South Korea"),
    ("yonsei university", "South Korea"),
    ("kaist", "South Korea"),
    ("postech", "South Korea"),
    ("university of tokyo", "Japan"),
    ("kyoto university", "Japan"),
    ("osaka university", "Japan"),
    ("riken", "Japan"),
    ("peking university", "China"),
    ("tsinghua university", "China"),
    ("university of hong kong", "Hong Kong"),
    ("chinese university of hong kong", "Hong Kong"),
    ("national university of singapore", "Singapore"),
    ("nanyang technological", "Singapore"),
    ("national taiwan university", "Taiwan"),
    # --- Oceania ---
    ("university of melbourne", "Australia"),
    ("university of sydney", "Australia"),
    ("university of queensland", "Australia"),
    ("monash university", "Australia"),
    ("university of auckland", "New Zealand"),
    # --- EMBL (pan-European) ---
    ("embl", "Germany"),
    ("european molecular biology", "Germany"),
    ("european bioinformatics institute", "United Kingdom"),
    ("ebi", "United Kingdom"),
]


# ── PI Recommender Weights ─────────────────────────────────────────────────
# recent_activity raised from 0.10 → 0.20 (recency-weighted scoring);
# h_index lowered from 0.15 → 0.10 to compensate (established PIs already
# score well via connection_strength and institution_ranking).
RECOMMENDER_WEIGHTS = {
    "field_similarity": 0.30,
    "connection_strength": 0.25,
    "institution_ranking": 0.15,
    "h_index": 0.10,
    "recent_activity": 0.20,
}

# ── Institution Rankings (load) ────────────────────────────────────────────
def load_rankings() -> dict:
    """Load institution rankings from JSON file."""
    if RANKINGS_PATH.exists():
        with open(RANKINGS_PATH) as f:
            return json.load(f)
    return {}
