"""Score job postings against CV keywords and preferences."""

import logging
import re

from src.config import (
    COUNTRY_TO_REGION,
    CV_KEYWORDS,
    REGION_PRIORITY,
    load_rankings,
)

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


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
    """Look up institution tier from rankings. Returns 1-4 (4 = unranked)."""
    if not institute:
        return 4

    rankings = load_rankings()
    tiers = rankings.get("tiers", {})
    inst_lower = institute.lower()

    for tier_num, tier_data in tiers.items():
        for inst in tier_data.get("institutions", []):
            if inst.lower() in inst_lower or inst_lower in inst.lower():
                return int(tier_num)

    return 4


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
      - tier: int (1-4)
      - sort_key: tuple for sorting
    """
    text = " ".join(
        str(job.get(f, ""))
        for f in ("title", "description", "field", "pi_name", "institute")
    )

    match_score = keyword_match_score(text, keywords)
    region = job.get("region") or get_region(job.get("country", ""))
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
