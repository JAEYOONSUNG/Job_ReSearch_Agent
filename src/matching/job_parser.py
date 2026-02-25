"""Job detail parser — extracts structured fields from job descriptions.

Parses free-form job posting text to extract:
- PI / supervisor name
- Requirements and qualifications
- Conditions (salary, duration, contract type)
- Research keywords
- Department / group info
"""

from __future__ import annotations

import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PI Name extraction patterns
# ---------------------------------------------------------------------------

# Reusable fragments for PI patterns
# Academic title prefix — handles "Prof.", "Dr.", "Prof. Dr.", "dr hab.", etc.
_TITLE = (
    r"(?:Prof(?:essor)?\.?\s+Dr\.?\s+"       # "Prof. Dr. "
    r"|dr\s+hab\.?\s+"                         # "dr hab. " (Polish habilitation)
    r"|(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+)"  # standard titles
)
_TITLE_OPT = f"(?:{_TITLE})?"

# Person name — Unicode-aware (handles ł, ń, ö, é, ř, etc.)
_UC = r"[A-ZÀ-ÖØ-Þ\u0100-\u017E]"   # uppercase first letter (incl. Latin Extended-A)
_LC = r"[a-zà-öø-ÿ\u0100-\u017E]"   # lowercase continuation (incl. ł, ń, ś, etc.)
_FIRST = rf"{_UC}{_LC}+"       # first name: "John", "José", "Joanna"
_MID = rf"(?:\s+{_UC}\.?)?"    # optional middle initial: " J." or " A"
_LAST = rf"{_UC}{_LC}+(?:-{_UC}{_LC}+)?"  # last name: "Smith", "Basta-Kaim"
_FULL_NAME = rf"({_FIRST}{_MID}\s+{_LAST})"  # captured full name

# Patterns that typically precede a PI name in job postings
_PI_PATTERNS = [
    # "Lab of Dr. John Smith", "Laboratory of Prof. Jane Doe"
    rf"(?:lab(?:orator(?:y|ies))?|group|team)\s+(?:of|led by|headed by|directed by)\s+{_TITLE_OPT}{_FULL_NAME}",
    # "PI: John Smith" or "Principal Investigator: Dr. Jane Doe"
    # Also: "Project leader:", "Program Coordinator", "Reporting to:", "Head of project"
    # \b prevents matching "PI" inside "EXPIRED", "OPIS" etc.
    rf"\b(?:PI|Principal\s+Investigator|Supervisor|Advisor|Adviser|Project\s+(?:leader|coordinator)|Program\s+Coordinator|Reporting\s+to|Head\s+of\s+(?:programme|project))\s*[:=\-]?\s*{_TITLE_OPT}{_FULL_NAME}",
    # "Prof. John Smith's lab" or "Dr. Jane Doe's group"
    rf"{_TITLE}{_FULL_NAME}'?s?\s+(?:lab(?:orator(?:y|ies))?|group|team|research)",
    # "under the supervision of Dr. John Smith"
    rf"(?:under the )?(?:supervision|direction|guidance|mentorship)\s+of\s+{_TITLE_OPT}{_FULL_NAME}",
    # "contact Dr. John Smith" or "Contact: Prof. Jane Doe"
    rf"(?:contact|inquiries?|questions?|(?:message|se(?:nd|nt))\s+to|directed\s+to(?:\s+both)?)\s*:?\s*{_TITLE_OPT}{_FULL_NAME}\s*(?:at|@|\(|\[at\])",
    # "The [Name] Lab" or "[Name] Laboratory"
    rf"(?:The\s+)?{_FULL_NAME}\s+(?:Lab(?:oratory)?|Group|Team)(?:\s|,|\.)",
    # "directed/supervised by Dr. Mitchell Ho" (lab/group prefix not required)
    rf"(?:directed|headed|led|managed|run|supervised)\s+by\s+{_TITLE_OPT}{_FULL_NAME}",
    # "join Dr. Mitchell Ho" or "work with Prof. Smith"
    rf"(?:join|work\s+with|collaborat\w+\s+with|assist)\s+{_TITLE}{_FULL_NAME}",
    # "lab/group/team led by Dr. Name" (passive)
    rf"(?:lab(?:orator(?:y|ies))?|group|team|unit)\s+(?:is\s+)?(?:led|headed|directed|supervised|run)\s+by\s+{_TITLE_OPT}{_FULL_NAME}",
    # "in the Name lab/group" (embedded)
    rf"in\s+(?:the\s+)?(?:(?:Prof|Dr)\.?\s+)?{_FULL_NAME}\s+(?:lab(?:orator(?:y|ies))?|group|team)",
    # "mentor: Dr. Name" or "mentored by Name"
    rf"(?:mentor(?:ed)?|mentoring)\s*(?:by|is|:)\s*{_TITLE_OPT}{_FULL_NAME}",
    # "led at [institution] by Prof. Name" (gap between verb and "by")
    rf"(?:directed|headed|led|supervised)\s+(?:at|in)\s+(?:the\s+)?\w[\w\s,]{{0,60}}?\s+by\s+{_TITLE_OPT}{_FULL_NAME}",
    # Parenthetical PI mention: "(Prof. Name, url)" or "(supervised by Dr. Name)"
    rf"\({_TITLE}{_FULL_NAME}(?:\s*,|\s*\))",
    # "About the PI: Prof. Name" / "About the supervisor: Dr. Name"
    rf"[Aa]bout the (?:PI|supervisor|mentor|advisor|adviser)\s*[:]\s*{_TITLE_OPT}{_FULL_NAME}",
    # "faculty mentor Name" (in titles or descriptions, e.g. "faculty mentor Lynette Cegelski")
    rf"faculty\s+(?:mentor|advisor|adviser|sponsor)\s+{_TITLE_OPT}{_FULL_NAME}",
    # "group leader Name" / "lab director Name" / "lab head Name"
    rf"(?:group\s+leader|lab(?:oratory)?\s+(?:director|head|manager))\s*[:,]?\s*{_TITLE_OPT}{_FULL_NAME}",
    # "(PI), Dr. Name" / "PI, Dr. Name" — comma between role and titled name
    rf"\b(?:PI|Principal\s+Investigator)\)?\s*,\s*{_TITLE}{_FULL_NAME}",
    # "Name, Group Leader" / "Name, Lab Head" (name before role, allows 2-4 part names)
    rf"({_FIRST}(?:\s+{_UC}{_LC}+){{1,3}})\s*,\s*(?:Group\s+Leader|Lab(?:oratory)?\s+(?:Head|Director)|Principal\s+Investigator|[Pp]roject\s+[Ll]eader)",
]

_PI_COMPILED = [re.compile(p, re.IGNORECASE) for p in _PI_PATTERNS]

# Common false-positive names to filter out
_FALSE_POSITIVE_NAMES = {
    "the university", "our group", "the lab", "the department",
    "this position", "the candidate", "the applicant",
    "the project", "the team", "the institute", "the faculty",
    "the research", "the program", "the centre", "the center",
    "new york", "san francisco", "los angeles", "united states",
    "for more", "to apply", "please send", "start date",
    "gene therapy", "cell biology", "molecular biology",
    "the it", "the pi", "the role", "the post",
    "more information", "full time", "part time",
    # Scientific fields often matching Pattern 6 ("X Lab/Group/Team")
    "magnetic field", "electric field", "gravitational field",
    "inflammation biology", "infection biology", "cancer biology",
    "developmental biology", "evolutionary biology", "computational biology",
    "structural biology", "chemical biology", "systems biology",
    "synthetic biology", "marine biology", "plant biology",
    "stem cell", "immune cell", "tumor biology",
    "network development", "method development", "drug development",
    "drug discovery", "target discovery", "gene discovery",
    "protein engineering", "genome engineering", "tissue engineering",
    "metabolic engineering", "genetic engineering",
    "machine learning", "deep learning", "transfer learning",
    "signal transduction", "electron microscopy", "mass spectrometry",
    "precision medicine", "regenerative medicine", "translational medicine",
    "climate change", "global health", "public health",
    "organic chemistry", "physical chemistry", "analytical chemistry",
    "prokaryotic gene", "eukaryotic gene",
    # Common false positives from actual data
    "cellular signalling", "central hr", "chan research",
    "ecology research", "environmental dna", "health equity",
    "join our", "marine modelling", "network development",
    "renewable energy", "ridge national",
    # False positives from page boilerplate / non-name phrases
    "red apply", "mechanics employment", "description the",
    "hill anton",  # "Chapel Hill" + "Anton lab" misparse
    "about the", "about our", "about this",
}


# ---------------------------------------------------------------------------
# Section header patterns for structured extraction
# ---------------------------------------------------------------------------

_REQUIREMENTS_HEADERS = [
    r"requirements?\s*[:.]",
    r"qualifications?\s*[:.]",
    r"(?:required|desired|preferred)\s+(?:qualifications?|skills?|experience)\s*[:.]",
    r"(?:we|the candidate|applicants?)\s+(?:should|must|are expected to)\s+(?:have|possess|hold)",
    r"what (?:we(?:'re| are) looking for|you(?:'ll)? (?:need|bring))\s*[:.]",
    r"eligibility\s*[:.]",
    r"(?:minimum|essential)\s+(?:requirements?|qualifications?|criteria)\s*[:.]",
    r"you (?:should|must|will) have\s*[:.]",
    r"candidates? should\s*[:.]",
    r"(?:your )?profile\s*[:.]",
]

_CONDITIONS_HEADERS = [
    r"(?:we )?offer\s*[:.]",
    r"(?:salary|compensation|remuneration|pay)\s*[:.]",
    r"(?:contract|appointment|position)\s+(?:type|details?|duration|terms?)\s*[:.]",
    r"(?:duration|length|term)\s+(?:of (?:the )?(?:position|appointment|contract))?\s*[:.]",
    r"(?:start(?:ing)? date|commencement)\s*[:.]",
    r"(?:benefits?|what we offer)\s*[:.]",
    r"(?:funding|stipend)\s*[:.]",
    r"(?:employment )?conditions?\s*[:.]",
    r"(?:working )?hours?\s*[:.]",
    r"(?:the position|this (?:is a|role))\s*[:.]",
]

_DESCRIPTION_HEADERS = [
    r"(?:job|position|role)\s+(?:description|summary|overview)\s*[:.]",
    r"(?:about|description of)\s+(?:the )?(?:position|role|project|lab(?:oratory)?|group|research)\s*[:.]",
    r"(?:project|research)\s+(?:description|summary|overview)\s*[:.]",
    r"(?:the )?(?:research|project|work)\s*[:.]",
    r"background\s*[:.]",
]


# ---------------------------------------------------------------------------
# Keyword extraction
# ---------------------------------------------------------------------------

# Domain-specific research keywords to detect in text
_RESEARCH_KEYWORDS = [
    "synthetic biology", "CRISPR", "Cas9", "Cas12", "Cas13",
    "protein engineering", "directed evolution", "metabolic engineering",
    "genome engineering", "gene editing", "gene therapy",
    "cell-free", "biofoundry", "high-throughput screening",
    "extremophile", "thermophile", "archaea", "hyperthermophile",
    "microbiology", "molecular biology", "systems biology",
    "bioinformatics", "computational biology", "structural biology",
    "enzyme engineering", "enzyme design", "de novo design",
    "machine learning", "deep learning", "AI-driven",
    "metagenomics", "genomics", "transcriptomics", "proteomics",
    "metabolomics", "multi-omics", "single-cell",
    "fermentation", "bioprocess", "biomanufacturing",
    "antibody engineering", "immunology", "cancer biology",
    "stem cell", "organoid", "tissue engineering",
    "neuroscience", "neurobiology", "optogenetics",
    "epigenetics", "chromatin", "RNA biology", "mRNA",
    "drug discovery", "pharmacology", "screening",
    "microfluidics", "biosensor", "bioelectronics",
    "plant biology", "agricultural biotechnology",
    "bioremediation", "environmental biology",
    "biochemistry", "biophysics", "chemical biology",
    "flow cytometry", "FACS", "mass spectrometry",
    "next-generation sequencing", "NGS", "nanopore",
    "cloning", "transformation", "transfection",
    "Western blot", "PCR", "qPCR", "RT-PCR",
    "microscopy", "confocal", "cryo-EM",
    "cell culture", "animal model", "mouse model",
]

# Duration/salary patterns
_DURATION_PATTERNS = [
    r"(\d+)\s*(?:-\s*\d+\s*)?(?:year|yr)s?",
    r"(\d+)\s*(?:-\s*\d+\s*)?months?",
    r"(?:up to|minimum|at least)\s+(\d+)\s+(?:year|yr|month)s?",
]

_SALARY_PATTERNS = [
    r"(?:salary|stipend|compensation|remuneration)\s*(?:of|is|:)?\s*[\$\u20ac\u00a3]?\s*([\d,]+(?:\.\d+)?)\s*(?:k|K|per\s+(?:year|annum|month))?",
    r"[\$\u20ac\u00a3]\s*([\d,]+(?:\.\d+)?)\s*(?:k|K)?\s*(?:per\s+(?:year|annum)|/\s*(?:yr|year|annum))",
    r"(?:TV-?L\s*E?\s*1[3-5]|W\s*[123]|E\s*1[3-5])",  # German pay scale
    r"(?:Grade|Band|Scale)\s*\d+",
]


# ---------------------------------------------------------------------------
# Field inference — keyword-to-field mapping
# ---------------------------------------------------------------------------

# Maps keyword patterns (lowercased) to canonical field names.
# Checked in order; first match wins. More specific patterns come first.
_FIELD_MAPPING: list[tuple[list[str], str]] = [
    (["crispr", "cas9", "cas12", "cas13", "gene editing", "genome editing"],
     "Genome Engineering"),
    (["synthetic biology", "synbio", "cell-free", "biofoundry", "genetic circuit"],
     "Synthetic Biology"),
    (["protein engineering", "directed evolution", "enzyme engineering",
      "enzyme design", "de novo protein", "protein design"],
     "Protein Engineering"),
    (["metabolic engineering", "metabolic flux", "pathway engineering",
      "fermentation", "bioprocess", "biomanufacturing"],
     "Metabolic Engineering"),
    (["extremophile", "thermophile", "hyperthermophile", "archaea",
      "extremozyme", "thermus", "sulfolobus"],
     "Extremophile Biology"),
    (["structural biology", "cryo-em", "x-ray crystallography",
      "protein structure", "structural determination"],
     "Structural Biology"),
    (["machine learning", "deep learning", "ai-driven", "artificial intelligence",
      "computational biology", "computational design"],
     "Computational Biology / AI"),
    (["bioinformatics", "genomics", "transcriptomics", "proteomics",
      "metabolomics", "multi-omics", "metagenomics"],
     "Bioinformatics / Omics"),
    (["single-cell", "single cell", "scrna-seq", "spatial transcriptomics"],
     "Single-Cell Biology"),
    (["immunology", "antibody engineering", "car-t", "immune",
      "immunotherapy", "antibody"],
     "Immunology"),
    (["cancer biology", "oncology", "tumor", "tumour"],
     "Cancer Biology"),
    (["neuroscience", "neurobiology", "optogenetics", "neural",
      "brain", "electrophysiology"],
     "Neuroscience"),
    (["stem cell", "organoid", "tissue engineering", "regenerative",
      "ips cell", "ipsc"],
     "Stem Cell / Regenerative Medicine"),
    (["drug discovery", "pharmacology", "drug design",
      "medicinal chemistry", "screening"],
     "Drug Discovery / Pharmacology"),
    (["plant biology", "agricultural biotechnology", "crop",
      "plant science", "plant genetics"],
     "Plant Biology"),
    (["microfluidics", "biosensor", "bioelectronics", "lab-on-a-chip"],
     "Bioengineering / Devices"),
    (["epigenetics", "chromatin", "histone", "dna methylation"],
     "Epigenetics"),
    (["rna biology", "mrna", "non-coding rna", "ribosome", "rna therapeutics"],
     "RNA Biology"),
    (["microbiology", "bacteriology", "virology", "mycology", "microbiome"],
     "Microbiology"),
    (["biochemistry", "biophysics", "chemical biology", "bioorganic"],
     "Biochemistry / Chemical Biology"),
    (["cell biology", "cell signaling", "cell cycle", "membrane biology"],
     "Cell Biology"),
    (["molecular biology", "gene expression", "gene regulation",
      "cloning", "transformation"],
     "Molecular Biology"),
]


# ---------------------------------------------------------------------------
# Department extraction
# ---------------------------------------------------------------------------

_DEPARTMENT_PATTERNS = [
    # "Department of Molecular Biology" / "Dept. of Chemistry"
    re.compile(r"(?:Department|Dept\.?)\s+of\s+([\w\s&,/()-]+?)(?:\s*[,.\n(]|\s+(?:at|in|is|and)\b)", re.IGNORECASE),
    # "School of Medicine" / "School of Public Health"
    re.compile(r"School\s+of\s+([\w\s&,/()-]+?)(?:\s*[,.\n(]|\s+(?:at|in|is|and)\b)", re.IGNORECASE),
    # "Faculty of Science and Technology"
    re.compile(r"Faculty\s+of\s+([\w\s&,/()-]+?)(?:\s*[,.\n(]|\s+(?:at|in|is|and)\b)", re.IGNORECASE),
    # "Division of Applied Chemistry"
    re.compile(r"Division\s+of\s+([\w\s&,/()-]+?)(?:\s*[,.\n(]|\s+(?:at|in|is|and)\b)", re.IGNORECASE),
]

# Map from pattern prefix keyword to human-readable prefix
_DEPT_PREFIX_MAP = {
    "department": "Department of",
    "dept": "Department of",
    "dept.": "Department of",
    "school": "School of",
    "faculty": "Faculty of",
    "division": "Division of",
}


def extract_department(text: str) -> str | None:
    """Extract department/school/faculty name from description text.

    Scans for patterns like "Department of X", "School of X", etc.
    Returns the full name including the prefix (e.g. "Department of Neuroscience").
    """
    if not text:
        return None

    for pattern in _DEPARTMENT_PATTERNS:
        m = pattern.search(text)
        if not m:
            continue
        name = m.group(1).strip().rstrip(",.")
        if len(name) < 3 or len(name) > 80:
            continue
        # Determine prefix from the matched text
        matched_prefix = m.group(0).split()[0].lower().rstrip(".")
        prefix = _DEPT_PREFIX_MAP.get(matched_prefix, "Department of")
        return f"{prefix} {name}"

    return None


# ---------------------------------------------------------------------------
# Contact email & info URL extraction
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
)

# Emails to skip (no-reply, generic HR systems, tracking pixels)
_EMAIL_SKIP = re.compile(
    r"(?:noreply|no-reply|donotreply|mailer-daemon|example\.com|"
    r"\.png$|\.jpg$|\.gif$|sentry\.io|email\.com$)",
    re.IGNORECASE,
)

# Context keywords that indicate a contact/question email
_EMAIL_CONTEXT = re.compile(
    r"(?:contact|e-?mail|inquir|question|information|apply|"
    r"send\s+(?:your|a|an)|submit|reach\s+(?:out|us)|"
    r"directed\s+to|correspondence|where\s+to\s+apply)",
    re.IGNORECASE,
)


def extract_contact_email(text: str) -> str | None:
    """Extract the most relevant contact email from description text.

    Prefers emails near context words like 'contact', 'email', 'inquiries'.
    Falls back to first valid email found.
    """
    if not text:
        return None

    candidates: list[tuple[str, bool]] = []  # (email, has_context)
    for m in _EMAIL_RE.finditer(text):
        email = m.group(0).rstrip(".,;:)")
        if _EMAIL_SKIP.search(email):
            continue
        # Check context: 80 chars before the match
        start = max(0, m.start() - 80)
        context = text[start:m.start()]
        has_ctx = bool(_EMAIL_CONTEXT.search(context))
        candidates.append((email, has_ctx))

    if not candidates:
        return None
    # Prefer contextual matches
    for email, has_ctx in candidates:
        if has_ctx:
            return email
    return candidates[0][0]


_URL_RE = re.compile(r"https?://[^\s<>\"')\]\},]+")

# URL patterns to skip (boilerplate, legal, EEO, benefit pages, etc.)
_URL_SKIP = re.compile(
    r"(?:eeoc\.gov|eeo|equal.?employ|affirmative.?action|"
    r"privacy.?policy|cookie.?policy|terms.?of.?(?:use|service)|"
    r"legal.?statement|policymanual|"
    r"/hr/benefits|/hr/learning|/hr/orientation|"
    r"annualclery\.pdf|police\..*\.edu|"
    r"linkedin\.com|facebook\.com|twitter\.com|instagram\.com|"
    r"youtube\.com|glassdoor\.com|indeed\.com|"
    r"fonts\.googleapis|cdn\.|\.css|\.js(?:\?|$)|"
    r"google\.com/maps|maps\.google|"
    r"mission\.php|about/mission)",
    re.IGNORECASE,
)

# Context keywords that indicate a useful info/detail/apply URL
_URL_CONTEXT = re.compile(
    r"(?:apply|application|detail|more\s+info|learn\s+more|"
    r"full\s+(?:description|posting|details|announcement)|"
    r"lab(?:oratory)?|group|team|research|"
    r"where\s+to\s+apply|visit|click\s+here|"
    r"see\s+(?:link|here|below))",
    re.IGNORECASE,
)


def extract_info_urls(text: str, job_url: str | None = None, max_urls: int = 3) -> list[str]:
    """Extract notable info/detail/apply URLs from description text.

    Filters out boilerplate (EEO, privacy, social media) and the job's own URL.
    Returns up to *max_urls* unique URLs, prioritising those near context words.
    """
    if not text:
        return []

    job_domain = ""
    if job_url:
        # Extract domain of the job URL to avoid returning the same link
        dm = re.search(r"https?://([^/]+)", job_url)
        if dm:
            job_domain = dm.group(1).lower()

    contextual: list[str] = []
    other: list[str] = []
    seen: set[str] = set()

    for m in _URL_RE.finditer(text):
        url = m.group(0).rstrip(".,;:)\"'")
        if url in seen:
            continue
        seen.add(url)
        if _URL_SKIP.search(url):
            continue
        # Skip the job's own URL
        if job_url and url == job_url:
            continue

        # Check context
        start = max(0, m.start() - 80)
        context = text[start:m.start()]
        if _URL_CONTEXT.search(context):
            contextual.append(url)
        else:
            other.append(url)

    result = contextual + other
    return result[:max_urls]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _is_valid_name(name: str) -> bool:
    """Check if extracted text looks like an actual person name."""
    lower = name.lower().strip()
    if lower in _FALSE_POSITIVE_NAMES:
        return False
    if len(name) < 4:
        return False
    # Must contain at least one space (first + last name)
    if " " not in name.strip():
        return False
    # Must start with a capital letter
    if not name[0].isupper():
        return False
    # Reject names starting with common non-name words
    first_word = name.split()[0].lower()
    non_name_starters = {
        "the", "a", "an", "this", "that", "our", "their", "his", "her", "about",
        "new", "old", "full", "part", "more", "all", "any", "each",
        "perform", "conduct", "manage", "develop", "lead", "apply",
        "research", "senior", "junior", "assistant", "associate", "postdoctoral", "fellow", "position",
        "earth", "life", "data", "cell", "gene", "protein",
        "biological", "biomedical", "clinical", "computational",
        "discovery", "priming", "romanian", "european", "american",
        "molecular", "synthetic", "structural", "functional",
        "advanced", "applied", "basic", "general", "specific",
        "medical", "pharmaceutical", "chemical", "physical",
        "national", "international", "regional", "global",
        # Scientific terms that look like first names (capitalized)
        "magnetic", "electric", "gravitational", "immune", "neural",
        "inflammation", "infection", "cancer", "tumor", "tumour",
        "developmental", "evolutionary", "marine", "plant", "animal",
        "network", "method", "target", "signal", "electron",
        "machine", "deep", "transfer", "precision", "regenerative",
        "translational", "organic", "analytical", "metabolic",
        "genetic", "genomic", "proteomic", "prokaryotic", "eukaryotic",
        "human", "tissue", "stem", "drug", "vaccine",
        "climate", "public", "global", "digital", "quantum",
        "chromatin", "driven", "nurse", "oxidative", "membrane",
        "peatland", "cement", "repair", "response", "sensing",
        "prague", "hematology", "electrochemistry", "electrophysiology",
        "energy", "nanotechnology", "cardiovascular", "postdoc",
        "world", "news",
        "aiming", "performing", "communicates", "collaborate", "collaborating",
        "further", "former", "info", "weekend", "smart", "spring",
        "professor", "doc", "phd", "ellis",  # titles/acronyms, not first names
        "principal",  # "Principal Investigator" false match
    }
    if first_word in non_name_starters:
        return False
    # Reject if any word is a common science/non-name term
    science_words = {
        "biology", "chemistry", "physics", "engineering", "medicine",
        "science", "sciences", "research", "studies", "technology",
        "bioinformatics", "genomics", "proteomics", "metabolomics",
        "immunology", "neuroscience", "oncology", "pathology",
        "pharmacology", "physiology", "biochemistry", "biophysics",
        "microbiology", "ecology", "genetics", "epigenetics",
        "field", "development", "discovery", "learning", "transduction",
        "microscopy", "spectrometry", "imaging",
    }
    words = [w.lower().rstrip(".,;:") for w in name.split()]
    if any(w in science_words for w in words):
        return False
    # Reject if all words are common English words (not names)
    common_words = {
        "the", "a", "an", "and", "or", "in", "on", "at", "to", "of",
        "for", "with", "this", "that", "our", "their", "his", "her",
        "new", "old", "full", "part", "more", "less", "all", "any",
        "advanced", "scientists", "researchers",
    }
    if all(w in common_words for w in words):
        return False
    # Reject two-word "names" where second word is a common non-surname
    non_surname_seconds = {
        "research", "biology", "dna", "hr", "national", "modelling",
        "energy", "equity", "signalling", "signaling",
        "employment", "apply", "expired", "status", "description",
        "systems", "skills", "schedule", "leadership", "networks",
        "people", "harbor", "harbour", "acoustics", "university",
    }
    if len(words) == 2 and words[1] in non_surname_seconds:
        return False
    # Reject phrases with common prepositions/conjunctions as second word
    if len(words) >= 2 and words[1] in {"and", "from", "with", "at", "the", "for", "or", "in", "on"}:
        return False
    return True


def infer_field(text: str) -> Optional[str]:
    """Infer the research field from job text using keyword mapping.

    Scans *text* against a prioritised list of keyword groups and returns
    the canonical field name for the first match.  Returns ``None`` if no
    field can be determined.
    """
    if not text:
        return None
    lower = text.lower()
    for keywords, field_name in _FIELD_MAPPING:
        if any(kw in lower for kw in keywords):
            return field_name
    return None


def extract_pi_name(text: str) -> Optional[str]:
    """Try to extract a PI / supervisor name from job text.

    First tries patterns requiring full names (First Last).  If none match,
    falls back to single-surname Lab/Group/Laboratory patterns and attempts
    to expand the last name to a full name from surrounding text.
    """
    if not text:
        return None

    # Normalize whitespace (newlines inside names break matching)
    norm = re.sub(r"\s+", " ", text)

    # Pass 1: full-name patterns
    for pattern in _PI_COMPILED:
        m = pattern.search(norm)
        if m:
            name = m.group(1).strip()
            if _is_valid_name(name):
                return name

    # Pass 2: single-surname Lab/Group/Laboratory patterns from description
    for pattern in _TITLE_LAB_COMPILED:
        m = pattern.search(norm)
        if m:
            last = m.group(1).strip()
            if last.lower() in _LAB_FALSE_POSITIVES:
                continue
            if len(last) < 3:
                continue
            # For multi-word names, validate
            if " " in last and not _is_valid_name(last):
                continue
            # Try to expand to a full name from the same text
            full = expand_pi_last_name(last, norm)
            if full:
                return full
            # Return just the last name if expansion fails
            return last

    return None


def expand_pi_last_name(last_name: str, text: str) -> str | None:
    """Expand a single last name (from title) to a full name using description text.

    Searches for patterns like "Dr./Prof. First Last" or "First Last" where
    *Last* matches the given surname.  Returns the full name or ``None``.
    """
    if not last_name or not text:
        return None

    text = re.sub(r"\s+", " ", text)  # normalise whitespace

    # 1. "Dr./Prof. First Last" where Last matches
    pattern1 = rf"{_TITLE}({_FIRST}{_MID})\s+{re.escape(last_name)}"
    m = re.search(pattern1, text, re.IGNORECASE)
    if m:
        full = f"{m.group(1)} {last_name}"
        if _is_valid_name(full):
            return full

    # 2. "First Last" where Last matches and First is capitalised
    pattern2 = rf"\b({_UC}{_LC}{{2,}})\s+{re.escape(last_name)}\b"
    for m in re.finditer(pattern2, text):
        candidate = f"{m.group(1)} {last_name}"
        if _is_valid_name(candidate):
            return candidate

    return None


# Title-specific Lab patterns (shorter names, different context)
_TITLE_LAB_PATTERNS = [
    # "Badran Lab", "Kampmann Lab", "Vardhana Lab" (single last name + Lab)
    r"(?:^|[\s\-\(])([A-Z][a-z]{2,})\s+Lab(?:oratory)?(?:[\s\)\-,.]|$)",
    # "(Coruzzi Lab)", "[Smith Lab]"
    r"[\(\[]([A-Z][a-z]{2,})\s+Lab(?:oratory)?[\)\]]",
    # "Laimins Laboratory", "Bhatt Lab"
    r"(?:^|[\s\-])([A-Z][a-z]{2,})\s+Lab(?:oratory)?(?:\s|$|[,\-])",
    # "Dr. Wan's Lab", "Dr. Nguyen's lab"
    r"(?:Dr|Prof)\.?\s+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)?)'?s?\s+[Ll]ab",
    # "in Jeffrey J. Gray's ... Lab" (possessive full name in title — apostrophe-s required)
    r"(?:^|[\s\-])(?:(?:Dr|Prof)\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)'s\s+(?:.*?\s)?(?:Lab|Laboratory)",
]
_TITLE_LAB_COMPILED = [re.compile(p) for p in _TITLE_LAB_PATTERNS]

# Last names that are false positives for Lab patterns
_LAB_FALSE_POSITIVES = {
    "research", "biology", "science", "clinical", "national",
    "animal", "federal", "central", "virtual", "digital",
    "mobile", "advanced", "applied", "general", "special",
    "teaching", "testing", "training", "fabrication", "innovation",
    "computer", "media", "data", "design",
    # Additional false positives from description context
    "peatland", "cement", "chromatin", "discovery", "magnetic",
    "neuropsychiatry", "membrane", "structural", "chemical",
    "biophysics", "ecology", "electron", "infection", "inflammation",
    "cancer", "tumor", "immune", "genomics", "proteomics",
    "metabolic", "synthetic", "computational", "translational",
    "alamos", "prague", "ridge", "harbor", "harbour",
    "driven", "nurse", "wallenberg", "the", "field",
    "sciences", "systems", "muscle", "time", "acoustics",
    "physics", "skills", "doc",
    "repair", "response", "signaling", "sensing", "imaging",
    "precision", "regenerative", "functional", "oxidative",
    "energy", "life", "nanotechnology", "electrophysiology",
    "electrochemistry", "cardiovascular", "hematology",
}


def extract_pi_from_title(title: str) -> Optional[str]:
    """Extract a PI last name (or full name) from a job title.

    Targets patterns like "Badran Lab", "Dr. Wan's Lab", "(Coruzzi Lab)",
    and "faculty mentor Lynette Cegelski".
    Returns the extracted name, or None.
    """
    if not title:
        return None

    # Check "faculty mentor/advisor Name" in title first (yields full name)
    m = re.search(
        rf"(?:faculty\s+)?(?:mentor|advisor|adviser|sponsor)\s+{_TITLE_OPT}{_FULL_NAME}",
        title, re.IGNORECASE,
    )
    if m:
        name = m.group(1).strip()
        if _is_valid_name(name):
            return name

    for pattern in _TITLE_LAB_COMPILED:
        m = pattern.search(title)
        if m:
            name = m.group(1).strip()
            if name.lower() in _LAB_FALSE_POSITIVES:
                continue
            if len(name) < 3:
                continue
            # For multi-word names, apply full validation
            if " " in name and not _is_valid_name(name):
                continue
            return name
    return None


def extract_section(text: str, header_patterns: list[str], max_chars: int = 1500) -> Optional[str]:
    """Extract the text following a section header.

    Looks for a header matching one of *header_patterns* and returns everything
    until the next section header or end of relevant content.
    """
    if not text:
        return None

    for pattern in header_patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if not m:
            continue

        start = m.end()
        remaining = text[start:start + max_chars * 2]

        # Find the end of this section (next header-like line or double newline gap)
        end_patterns = [
            r"\n\s*\n\s*(?:[A-Z][a-z]+(?:\s+[a-z]+){0,3}\s*[:.])",
            r"\n\s*(?:Requirements?|Qualifications?|We offer|Salary|How to apply|"
            r"Application|About|Contact|Deadline|Duration|Benefits?|"
            r"Responsibilities?|Key tasks?|Your (?:profile|tasks?)|"
            r"What we offer|The position|Starting date)\s*[:.]",
        ]
        end_pos = len(remaining)
        for ep in end_patterns:
            em = re.search(ep, remaining, re.IGNORECASE)
            if em:
                end_pos = min(end_pos, em.start())

        section = remaining[:end_pos].strip()
        if section and len(section) > 10:
            return section[:max_chars]

    return None


def extract_requirements(text: str) -> Optional[str]:
    """Extract requirements/qualifications section."""
    return extract_section(text, _REQUIREMENTS_HEADERS)


def extract_conditions(text: str) -> Optional[str]:
    """Extract conditions section (salary, duration, contract type)."""
    section = extract_section(text, _CONDITIONS_HEADERS)
    if section:
        return section

    # Fallback: try to find specific condition snippets
    snippets = []

    for p in _DURATION_PATTERNS:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            # Get surrounding context
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 30)
            snippets.append(text[start:end].strip())
            break

    for p in _SALARY_PATTERNS:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            start = max(0, m.start() - 20)
            end = min(len(text), m.end() + 20)
            snippets.append(text[start:end].strip())
            break

    return " | ".join(snippets) if snippets else None


# ---------------------------------------------------------------------------
# Application materials extraction
# ---------------------------------------------------------------------------

_APPLICATION_HEADERS = [
    r"how\s+to\s+apply\s*[:.]?",
    r"application\s+procedure\s*[:.]?",
    r"required\s+documents?\s*[:.]?",
    r"your\s+application\s+should\s+include\s*[:.]?",
    r"please\s+(?:submit|send|include)\s+the\s+following\s*[:.]?",
    r"application\s+(?:documents?|materials?|package)\s*[:.]?",
    r"to\s+apply[,\s]+(?:please\s+)?(?:submit|send|include)\s*[:.]?",
]

_APPLICATION_MATERIAL_KEYWORDS = [
    ("cover letter", "Cover letter"),
    ("motivation letter", "Cover letter"),
    ("letter of motivation", "Cover letter"),
    ("curriculum vitae", "CV"),
    ("resume", "CV"),
    (" cv ", "CV"),
    (" cv,", "CV"),
    (" cv.", "CV"),
    ("research statement", "Research statement"),
    ("research plan", "Research plan"),
    ("research proposal", "Research proposal"),
    ("statement of research", "Research statement"),
    ("publication list", "Publication list"),
    ("list of publications", "Publication list"),
    ("publications list", "Publication list"),
    ("reference letter", "Reference letters"),
    ("letters of reference", "Reference letters"),
    ("recommendation letter", "Reference letters"),
    ("letters of recommendation", "Reference letters"),
    ("names of referees", "Reference letters"),
    ("names of references", "Reference letters"),
    ("contact details of referees", "Reference letters"),
    ("diploma", "Diplomas/Certificates"),
    ("degree certificate", "Diplomas/Certificates"),
    ("transcript", "Transcripts"),
    ("academic record", "Transcripts"),
    ("teaching statement", "Teaching statement"),
    ("diversity statement", "Diversity statement"),
]

# Context words that suggest application-related sentences
_APPLICATION_CONTEXT = re.compile(
    r"(?:submit|send|include|attach|upload|provide|enclose|forward|prepare)",
    re.IGNORECASE,
)


def extract_application_materials(text: str) -> Optional[str]:
    """Extract required application materials from a job posting.

    Returns a semicolon-separated list of required materials, or None.
    """
    if not text:
        return None

    found: set[str] = set()
    lower = text.lower()

    # Strategy 1: Find "How to Apply" section and scan for material keywords
    section = extract_section(text, _APPLICATION_HEADERS, max_chars=1500)
    if section:
        sec_lower = section.lower()
        for keyword, label in _APPLICATION_MATERIAL_KEYWORDS:
            if keyword in sec_lower:
                found.add(label)

    # Strategy 2: Scan full text for material keywords near context words
    if len(found) < 2:
        for ctx_match in _APPLICATION_CONTEXT.finditer(text):
            window_start = max(0, ctx_match.start() - 20)
            window_end = min(len(text), ctx_match.end() + 200)
            window = text[window_start:window_end].lower()
            for keyword, label in _APPLICATION_MATERIAL_KEYWORDS:
                if keyword in window:
                    found.add(label)

    if not found:
        return None

    # Deterministic order
    order = [
        "CV", "Cover letter", "Research statement", "Research plan",
        "Research proposal", "Publication list", "Reference letters",
        "Diplomas/Certificates", "Transcripts", "Teaching statement",
        "Diversity statement",
    ]
    sorted_materials = [m for m in order if m in found]
    # Include any not in the predefined order
    sorted_materials.extend(sorted(found - set(order)))
    return "; ".join(sorted_materials)


def extract_keywords(text: str) -> list[str]:
    """Extract research keywords found in the text."""
    if not text:
        return []
    lower = text.lower()
    found = []
    for kw in _RESEARCH_KEYWORDS:
        if kw.lower() in lower:
            found.append(kw)
    return found


def extract_job_description(text: str) -> Optional[str]:
    """Extract the main job/research description section."""
    return extract_section(text, _DESCRIPTION_HEADERS, max_chars=2000)


# ---------------------------------------------------------------------------
# LinkedIn / Indeed description cleaner & structured parser
# ---------------------------------------------------------------------------

# Boilerplate lines to strip from LinkedIn scraped descriptions
_LINKEDIN_NOISE = re.compile(
    r"^(?:"
    r"Apply|Save|Report this job|"
    r"Sign in (?:to |with ).*|"
    r"Use AI to assess.*|"
    r"Get AI-powered.*|"
    r"Am I a good fit.*|"
    r"Tailor my resume|"
    r"New to LinkedIn\?|"
    r"Join now|"
    r"See who .* has hired.*|"
    r"By clicking .*|"
    r"User Agreement|Privacy Policy|Cookie Policy|"
    r"\d+ (?:applicants?|days? ago)|"
    r"Over \d+ applicants|"
    r"Be among the first.*"
    r")$",
    re.IGNORECASE | re.MULTILINE,
)

# Section headers recognized in structured LinkedIn/Indeed descriptions
_STRUCTURED_SECTIONS = {
    "summary": re.compile(
        r"^(?:Position |Job )?Summary\s*[:.]?\s*$|"
        r"^About (?:the |this )?(?:Position|Role|Opportunity)\s*[:.]?\s*$|"
        r"^Overview\s*[:.]?\s*$|"
        r"^Description\s*[:.]?\s*$|"
        # EURAXESS / AcademicPositions / ScholarshipDB patterns
        r"^Job [Dd]escription\s*[:.]?\s*$|"
        r"^About the (?:research |)project\s*[:.]?\s*$|"
        r"^About (?:us|the (?:lab|group|team|institute))\s*[:.]?\s*$|"
        r"^Offer [Dd]escription\s*[:.]?\s*$|"
        r"^Project [Dd]escription\s*[:.]?\s*$|"
        r"^Presentation of the (?:position|organisation)\s*[:.]?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "responsibilities": re.compile(
        r"^(?:Position |Key |Primary )?Responsibilities\s*[:.]?\s*$|"
        r"^(?:Key |Major )?(?:Duties|Tasks)\s*[:.]?\s*$|"
        r"^What (?:you'll|you will) do\s*[:.]?\s*$|"
        r"^The Role\s*[:.]?\s*$|"
        # EURAXESS / AcademicPositions patterns
        r"^(?:Key |Main )?(?:Tasks?|Activities)\s*[:.]?\s*$|"
        r"^Your (?:tasks?|role|mission)\s*[:.]?\s*$|"
        r"^The (?:Postdoc(?:toral)?|position|role|fellow)\s+will\s*[:.]?\s*$|"
        r"^Key [Dd]uties\s*[:.]?\s*$|"
        r"^(?:Main |Key )?[Rr]esponsibilities\s*[:.]?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "requirements": re.compile(
        r"^(?:Position |Minimum )?(?:Requirements?|Qualifications?)\s*[:.]?\s*$|"
        r"^(?:Required |Minimum )?(?:Education|Experience|Skills)(?: and Experience)?\s*[:.]?\s*$|"
        r"^What (?:you'll|you will) (?:need|bring)\s*[:.]?\s*$|"
        r"^Who you are\s*[:.]?\s*$|"
        # EURAXESS / AcademicPositions patterns
        r"^(?:Your |Candidate )?[Pp]rofile\s*[:.]?\s*$|"
        r"^(?:Essential |Required )?(?:Criteria|Competencies)\s*[:.]?\s*$|"
        r"^Eligibility\s*[:.]?\s*$|"
        r"^(?:We|The candidate|Applicants?)\s+(?:should|must|are expected)\s*[:.]?\s*$|"
        r"^(?:Minimum |Essential )?(?:Education|Experience)\s+(?:and |&\s+)?(?:Experience |Education )?[Rr]equirements?\s*[:.]?\s*$|"
        r"^Who we are looking for\s*[:.]?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "preferred": re.compile(
        r"^Preferred\s+(?:Experience|Education|Skills|Qualifications?)\s*[:.]?\s*$|"
        r"^(?:Nice to have|Desired|Bonus)\s*[:.]?\s*$|"
        r"^Additional (?:Skills|Qualifications?)\s*[:.]?\s*$|"
        # EURAXESS / AcademicPositions patterns
        r"^Preferred [Ss]kills?\s*[:.]?\s*$|"
        r"^(?:Assets?|Advantages?|Plus)\s*[:.]?\s*$|"
        r"^(?:Desired|Preferred)\s+(?:Criteria|Competencies)\s*[:.]?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
    "conditions": re.compile(
        r"^(?:Compensation|Salary|Pay|Benefits?|What we offer)\s*[:.]?\s*$|"
        r"^(?:Contract|Employment) (?:Type|Details?|Terms?)\s*[:.]?\s*$|"
        # EURAXESS / AcademicPositions patterns
        r"^(?:We )?[Oo]ffer\s*[:.]?\s*$|"
        r"^(?:Employment |Working )?[Cc]onditions?\s*[:.]?\s*$|"
        r"^(?:Benefits? (?:and |& )?)?[Ss]ervices\s*[:.]?\s*$|"
        r"^(?:Contract |Position )?[Dd]etails?\s*[:.]?\s*$",
        re.IGNORECASE | re.MULTILINE,
    ),
}


def clean_linkedin_description(text: str) -> str:
    """Strip LinkedIn UI boilerplate from scraped job descriptions.

    Removes 'Apply', 'Save', 'Sign in with Email', 'Cookie Policy' lines etc.
    Also strips the repeated title/company/location header that LinkedIn duplicates.
    """
    if not text:
        return ""

    # Remove boilerplate lines
    text = _LINKEDIN_NOISE.sub("", text)

    # Remove duplicated title/company block at top (LinkedIn shows it twice)
    lines = text.split("\n")
    # Find and remove the second occurrence of the first non-empty line
    first_line = ""
    for line in lines:
        if line.strip():
            first_line = line.strip()
            break
    if first_line and len(first_line) > 5:
        count = 0
        cleaned = []
        for line in lines:
            if line.strip() == first_line:
                count += 1
                if count <= 1:
                    cleaned.append(line)
                # Skip duplicates
            else:
                cleaned.append(line)
        text = "\n".join(cleaned)

    # Collapse excessive blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_structured_description(text: str) -> dict[str, str]:
    """Parse a job description into structured sections.

    Returns a dict with keys: summary, responsibilities, requirements,
    preferred, conditions. Empty sections are omitted.
    """
    if not text:
        return {}

    text = clean_linkedin_description(text)

    result: dict[str, str] = {}

    # Find all section header positions
    positions: list[tuple[int, int, str]] = []
    for section_name, pattern in _STRUCTURED_SECTIONS.items():
        for m in pattern.finditer(text):
            positions.append((m.start(), m.end(), section_name))

    if not positions:
        return {}

    # Sort by position in text
    positions.sort(key=lambda x: x[0])

    # Extract text between consecutive headers
    for i, (start, end, name) in enumerate(positions):
        if name in result:
            continue  # keep first occurrence
        next_start = positions[i + 1][0] if i + 1 < len(positions) else len(text)
        section_text = text[end:next_start].strip()
        if section_text and len(section_text) > 10:
            result[name] = section_text[:2000]

    return result


# ---------------------------------------------------------------------------
# Deadline extraction
# ---------------------------------------------------------------------------

# Date patterns — matches common date formats in job postings
_MONTH_NAMES = (
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December"
    r"|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?"
)

# "March 15, 2026" or "15 March 2026" or "Mar 15, 2026"
_DATE_MDY = rf"({_MONTH_NAMES})\s+(\d{{1,2}}),?\s+(\d{{4}})"
_DATE_DMY = rf"(\d{{1,2}})\s+({_MONTH_NAMES})\s+(\d{{4}})"
# "2026-03-15" or "2026/03/15"
_DATE_ISO = r"(\d{4})[-/](\d{2})[-/](\d{2})"
# "15/03/2026" or "15.03.2026"
_DATE_EU = r"(\d{1,2})[./](\d{1,2})[./](\d{4})"

_DEADLINE_CONTEXT_PATTERNS = [
    # "Deadline: <date>" or "Application deadline: <date>"
    r"(?:application\s+)?deadline\s*[:=]\s*",
    # "Closing date: <date>" or "Closing date for applications is <date>"
    r"closing\s+date\s*(?:for\s+\w+\s*)?(?:[:=]|is)\s*",
    # "Applications must be received by <date>"
    r"(?:applications?|submissions?)\s+(?:must be|should be|are)\s+(?:received|submitted)\s+(?:by|before|no later than)\s+",
    # "Submit your application by/before/no later than <date>"
    r"submit\s+(?:your\s+)?(?:application|documents?|materials?)\s+(?:by|before|no later than)\s+",
    # "Apply by <date>" or "Apply before <date>"
    r"apply\s+(?:by|before|no later than)\s+",
    # "Apply Before <date>" (ScholarshipDB format)
    r"apply\s+before\s+",
    # "Applications (are) due (by) <date>"
    r"(?:applications?\s+)?(?:are\s+)?due\s*(?:[:=]|by|before|on)?\s*",
    # "no later than <date>" (standalone — strong deadline indicator)
    r"no later than\s+",
    # "open until <date>"
    r"(?:position|posting|vacancy|job)\s+(?:is\s+)?open\s+(?:until|through|till)\s+",
    # "please submit/apply before <date>"
    r"please\s+(?:submit|apply)\s+(?:by|before|no later than)\s+",
    # "Review of applications will begin/begins <date>"
    r"review\s+of\s+(?:applications?|candidates?)\s+(?:will\s+)?(?:begins?|starts?|commences?)\s*(?:on)?\s*",
    # "for full consideration, apply/submit by <date>"
    r"for\s+full\s+consideration[,\s]+(?:please\s+)?(?:apply|submit)\s+(?:by|before)\s+",
    # "applications accepted until <date>"
    r"applications?\s+accepted\s+(?:until|till)\s+",
    # "position/vacancy closes on <date>"
    r"(?:position|vacancy|posting)\s+closes?\s+(?:on\s+)?",
    # "screening/review begins <date>"
    r"(?:screening|review)\s+(?:of\s+\w+\s+)?begins?\s*(?:on)?\s*",
    # "priority consideration by <date>"
    r"priority\s+consideration\s+(?:by|before)\s+",
    # German: "Bewerbungsfrist: <date>"
    r"Bewerbungsfrist\s*[:=]\s*",
    # French: "Date limite: <date>"
    r"Date\s+limite\s*[:=]?\s*",
]

_DEADLINE_COMPILED = [re.compile(p, re.IGNORECASE) for p in _DEADLINE_CONTEXT_PATTERNS]
_DATE_PATTERNS = [
    re.compile(_DATE_MDY, re.IGNORECASE),
    re.compile(_DATE_DMY, re.IGNORECASE),
    re.compile(_DATE_ISO),
    re.compile(_DATE_EU),
]

_MONTH_MAP = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6,
    "july": 7, "jul": 7, "august": 8, "aug": 8, "september": 9, "sep": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}


def _parse_date_string(text: str) -> Optional[str]:
    """Try to parse a date from *text* and return ISO format (YYYY-MM-DD).

    Handles: "March 15, 2026", "15 March 2026", "2026-03-15",
    "15/03/2026", "Mar 15, 2026".
    Strips trailing time components like ", 10:59 PM" or " at 23:59 Danish time".
    """
    # Strip trailing time components before parsing
    text = re.sub(
        r",?\s+(?:at\s+)?\d{1,2}:\d{2}(?::\d{2})?\s*(?:[AP]M)?(?:\s+\w+\s+time)?",
        "", text, flags=re.IGNORECASE,
    ).strip()

    # MDY: "March 15, 2026"
    m = re.match(_DATE_MDY, text, re.IGNORECASE)
    if m:
        month_str, day, year = m.group(1), m.group(2), m.group(3)
        month = _MONTH_MAP.get(month_str.rstrip(".").lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"

    # DMY: "15 March 2026"
    m = re.match(_DATE_DMY, text, re.IGNORECASE)
    if m:
        day, month_str, year = m.group(1), m.group(2), m.group(3)
        month = _MONTH_MAP.get(month_str.rstrip(".").lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"

    # ISO: "2026-03-15"
    m = re.match(_DATE_ISO, text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # EU: "15/03/2026" or "15.03.2026"
    m = re.match(_DATE_EU, text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= month <= 12 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"

    return None


def extract_deadline(text: str) -> Optional[str]:
    """Extract application deadline date from job posting text.

    Searches for deadline-related keywords followed by date patterns.
    Collects all candidate dates and returns the earliest one in ISO format
    (YYYY-MM-DD) or None.
    """
    if not text:
        return None

    candidates: list[str] = []

    # Strategy 1: Look for deadline keywords followed by a date
    for pattern in _DEADLINE_COMPILED:
        for m in pattern.finditer(text):
            # Extract the text after the keyword (up to 60 chars)
            after = text[m.end():m.end() + 60]
            date = _parse_date_string(after.strip())
            if date:
                candidates.append(date)

    # Strategy 2: Look for "Deadline" section header
    deadline_section = extract_section(text, [r"deadline\s*[:.]"])
    if deadline_section:
        # Try to find a date in the first 100 chars of the section
        for dp in _DATE_PATTERNS:
            dm = dp.search(deadline_section[:100])
            if dm:
                date = _parse_date_string(dm.group(0))
                if date:
                    candidates.append(date)

    # Strategy 3: Scan for any date within 120 chars of deadline keywords
    _DEADLINE_PROXIMITY_KW = re.compile(
        r"(?:deadline|closing\s+date|apply\s+before|bewerbungsfrist|date\s+limite"
        r"|applications?\s+accepted\s+until|vacancy\s+closes)",
        re.IGNORECASE,
    )
    if not candidates:
        for kw_match in _DEADLINE_PROXIMITY_KW.finditer(text):
            window = text[kw_match.start():kw_match.start() + 120]
            for dp in _DATE_PATTERNS:
                dm = dp.search(window)
                if dm:
                    date = _parse_date_string(dm.group(0))
                    if date:
                        candidates.append(date)

    if not candidates:
        return None

    # Return the earliest deadline (most urgent)
    return min(candidates)


def parse_job_posting(text: str) -> dict[str, Any]:
    """Parse a full job posting and return all extracted structured data.

    Returns a dict with keys:
    - pi_name: str or None
    - requirements: str or None
    - conditions: str or None
    - keywords: str (comma-separated) or None
    - job_description: str or None (structured description section)
    """
    if not text:
        return {
            "pi_name": None,
            "requirements": None,
            "conditions": None,
            "keywords": None,
            "application_materials": None,
        }

    pi_name = extract_pi_name(text)
    requirements = extract_requirements(text)
    conditions = extract_conditions(text)
    keywords = extract_keywords(text)
    app_materials = extract_application_materials(text)

    return {
        "pi_name": pi_name,
        "requirements": requirements,
        "conditions": conditions,
        "keywords": ", ".join(keywords) if keywords else None,
        "application_materials": app_materials,
    }
