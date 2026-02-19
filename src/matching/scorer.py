"""Score job postings against CV keywords and preferences."""

import logging
import re
import unicodedata

from src.config import (
    COUNTRY_TO_REGION,
    CV_KEYWORDS,
    INSTITUTE_COUNTRY_RULES,
    REGION_PRIORITY,
    load_rankings,
)

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def _strip_accents(text: str) -> str:
    """Remove accents/diacritics: Zürich→Zurich, Université→Universite, Poznań→Poznan."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _norm_inst(text: str) -> str:
    """Normalize institute name for matching: lowercase, strip accents, remove punctuation."""
    text = _strip_accents(text.lower().strip())
    # Remove commas, periods, "the " prefix, ", inc." suffix
    text = re.sub(r"[,.]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^the ", "", text)
    text = re.sub(r"\s+inc$", "", text)
    return text


def keyword_match_score(job_text: str, keywords: list[str] = None) -> float:
    """Calculate keyword overlap score (0.0 - 1.0)."""
    if keywords is None:
        keywords = CV_KEYWORDS
    if not keywords or not job_text:
        return 0.0

    text = _normalize(job_text)
    matches = sum(1 for kw in keywords if kw.lower() in text)
    return matches / len(keywords)


def get_institution_tier(institute: str) -> int:
    """Look up institution tier from rankings. Returns 1-5 (5 = unranked).

    Checks tiers 1-4, then companies (top_companies -> 2, companies -> 3).
    Also resolves aliases from tier_lookup_aliases.
    Uses accent-stripped matching (ETH Zürich == ETH Zurich).
    """
    if not institute:
        return 5

    rankings = load_rankings()
    tiers = rankings.get("tiers", {})
    aliases = rankings.get("tier_lookup_aliases", {})
    inst_norm = _norm_inst(institute)

    # Check aliases first (normalized, short aliases require exact match)
    for alias, canonical in aliases.items():
        alias_n = _norm_inst(alias)
        if alias_n == inst_norm:
            inst_norm = _norm_inst(canonical)
            break
        # Only do substring match for aliases >= 5 chars to avoid false positives
        if len(alias_n) >= 5 and alias_n in inst_norm:
            inst_norm = _norm_inst(canonical)
            break

    # Check tiers 1-4
    for tier_num, tier_data in tiers.items():
        try:
            tier_int = int(tier_num)
        except (ValueError, TypeError):
            continue
        for inst in tier_data.get("institutions", []):
            ref = _norm_inst(inst)
            if ref in inst_norm or inst_norm in ref:
                return tier_int

    # Check companies section
    companies = rankings.get("companies", {})
    top_co = companies.get("top_companies", {})
    top_list = top_co.get("institutions", []) if isinstance(top_co, dict) else top_co
    for inst in top_list:
        ref = _norm_inst(inst)
        if ref in inst_norm or inst_norm in ref:
            return top_co.get("tier_equivalent", 2) if isinstance(top_co, dict) else 2

    std_co = companies.get("companies", {})
    std_list = std_co.get("institutions", []) if isinstance(std_co, dict) else std_co
    for inst in std_list:
        ref = _norm_inst(inst)
        if ref in inst_norm or inst_norm in ref:
            return std_co.get("tier_equivalent", 3) if isinstance(std_co, dict) else 3

    return 5


def is_company(institute: str) -> bool:
    """Return True if the institute is a company (not academic institution).

    Uses stricter matching than tier lookup to avoid false positives
    (e.g., "MIT" must not match "GlaxoSmithKline").
    """
    if not institute:
        return False
    rankings = load_rankings()
    companies = rankings.get("companies", {})
    inst_n = _norm_inst(institute)

    for group in companies.values():
        comp_list = group.get("institutions", []) if isinstance(group, dict) else group
        for comp in comp_list:
            comp_n = _norm_inst(comp)
            shorter = min(len(inst_n), len(comp_n))
            if shorter < 5:
                if inst_n == comp_n:
                    return True
            else:
                if comp_n in inst_n or inst_n in comp_n:
                    return True
    return False


def guess_country_from_institute(institute: str) -> str:
    """Infer country from institution name using known patterns.

    Returns a country string (e.g. "United States") or empty string if unknown.
    """
    if not institute:
        return ""
    inst_lower = institute.lower()
    for pattern, country in INSTITUTE_COUNTRY_RULES:
        if pattern in inst_lower:
            return country
    return ""


def get_region(country: str) -> str:
    """Map country to region."""
    if not country:
        return "Other"
    # Direct match
    if country in COUNTRY_TO_REGION:
        return COUNTRY_TO_REGION[country]
    # Partial match
    country_lower = country.lower()
    for key, region in COUNTRY_TO_REGION.items():
        if key.lower() in country_lower or country_lower in key.lower():
            return region
    return "Other"


def score_job(job: dict, keywords: list[str] = None) -> dict:
    """Score a job and enrich with region/tier info.

    Returns the job dict with added fields:
      - match_score: float (0.0 - 1.0)
      - region: str
      - tier: int (1-5, where 5 = unranked)
      - sort_key: tuple for sorting
    """
    text = " ".join(
        str(job.get(f, ""))
        for f in ("title", "description", "field", "pi_name", "institute")
    )

    match_score = keyword_match_score(text, keywords)
    country = job.get("country") or ""
    if not country:
        country = guess_country_from_institute(job.get("institute", ""))
    region = job.get("region") or get_region(country)
    tier = job.get("tier") or get_institution_tier(job.get("institute", ""))
    h_index = job.get("h_index") or 0

    job["match_score"] = round(match_score, 3)
    job["region"] = region
    job["tier"] = tier

    # Sort key: region priority ASC, tier ASC, h_index DESC, match_score DESC
    job["sort_key"] = (
        REGION_PRIORITY.get(region, 99),
        tier,
        -h_index,
        -match_score,
    )

    return job


def score_and_sort_jobs(jobs: list[dict], keywords: list[str] = None) -> list[dict]:
    """Score and sort a list of jobs by priority."""
    scored = [score_job(j, keywords) for j in jobs]
    scored.sort(key=lambda j: j["sort_key"])
    return scored
