"""Job detail parser — extracts structured fields from job descriptions.

Parses free-form job posting text to extract:
- PI / supervisor name
- Requirements and qualifications
- Conditions (salary, duration, contract type)
- Research keywords
- Department / group info
"""

from __future__ import annotations

import re
from typing import Any, Optional


# ---------------------------------------------------------------------------
# PI Name extraction patterns
# ---------------------------------------------------------------------------

# Patterns that typically precede a PI name in job postings
_PI_PATTERNS = [
    # "Lab of Dr. John Smith", "Laboratory of Prof. Jane Doe"
    r"(?:lab(?:oratory)?|group|team)\s+(?:of|led by|headed by|directed by)\s+(?:(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)",
    # "PI: John Smith" or "Principal Investigator: Dr. Jane Doe"
    r"(?:PI|Principal\s+Investigator|Supervisor|Advisor|Adviser)\s*[:=]\s*(?:(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)",
    # "Prof. John Smith's lab" or "Dr. Jane Doe's group"
    r"(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)'?s?\s+(?:lab(?:oratory)?|group|team|research)",
    # "under the supervision of Dr. John Smith"
    r"(?:under the )?(?:supervision|direction|guidance|mentorship)\s+of\s+(?:(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)",
    # "contact Dr. John Smith" or "Contact: Prof. Jane Doe"
    r"(?:contact|inquiries?|questions?)\s*:?\s*(?:(?:Prof(?:essor)?|Dr|Assoc\.?\s*Prof|Asst\.?\s*Prof)\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)\s*(?:at|@|\()",
    # "The [Name] Lab" or "[Name] Laboratory"
    r"(?:The\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)\s+(?:Lab(?:oratory)?|Group|Team)(?:\s|,|\.)",
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
        "the", "a", "an", "this", "that", "our", "their", "his", "her",
        "new", "old", "full", "part", "more", "all", "any", "each",
        "perform", "conduct", "manage", "develop", "lead", "apply",
        "research", "senior", "junior", "assistant", "associate",
        "earth", "life", "data", "cell", "gene", "protein",
        "biological", "biomedical", "clinical", "computational",
        "discovery", "priming", "romanian", "european", "american",
        "molecular", "synthetic", "structural", "functional",
        "advanced", "applied", "basic", "general", "specific",
        "medical", "pharmaceutical", "chemical", "physical",
        "national", "international", "regional", "global",
    }
    if first_word in non_name_starters:
        return False
    # Reject if all words are common English words (not names)
    common_words = {
        "the", "a", "an", "and", "or", "in", "on", "at", "to", "of",
        "for", "with", "this", "that", "our", "their", "his", "her",
        "new", "old", "full", "part", "more", "less", "all", "any",
        "science", "biology", "chemistry", "engineering", "medicine",
        "research", "advanced", "scientists", "researchers",
    }
    words = [w.lower().rstrip(".,;:") for w in name.split()]
    if all(w in common_words for w in words):
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
    """Try to extract a PI / supervisor name from job text."""
    if not text:
        return None

    for pattern in _PI_COMPILED:
        m = pattern.search(text)
        if m:
            name = m.group(1).strip()
            if _is_valid_name(name):
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
        }

    pi_name = extract_pi_name(text)
    requirements = extract_requirements(text)
    conditions = extract_conditions(text)
    keywords = extract_keywords(text)

    return {
        "pi_name": pi_name,
        "requirements": requirements,
        "conditions": conditions,
        "keywords": ", ".join(keywords) if keywords else None,
    }
