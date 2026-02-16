"""Deduplicate job listings across sources."""

import logging
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def normalize_title(title: str) -> str:
    """Normalize job title for comparison."""
    title = title.lower().strip()
    title = re.sub(r"\s+", " ", title)
    # Remove common prefixes
    for prefix in ("postdoctoral", "postdoc", "post-doctoral", "research"):
        title = re.sub(rf"^{prefix}\s+(position|fellow|researcher|associate)\s*[-:–]?\s*", "", title)
    return title


def normalize_institute(name: str) -> str:
    """Normalize institution name."""
    name = name.lower().strip()
    replacements = {
        "university of california, ": "uc ",
        "university of california ": "uc ",
        "massachusetts institute of technology": "mit",
        "california institute of technology": "caltech",
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return re.sub(r"\s+", " ", name)


def similarity(a: str, b: str) -> float:
    """Calculate string similarity ratio."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def is_duplicate(job_a: dict, job_b: dict, threshold: float = 0.85) -> bool:
    """Check if two jobs are duplicates."""
    # Same URL = definite duplicate
    if job_a.get("url") and job_a.get("url") == job_b.get("url"):
        return True

    title_sim = similarity(
        normalize_title(job_a.get("title", "")),
        normalize_title(job_b.get("title", "")),
    )
    inst_sim = similarity(
        normalize_institute(job_a.get("institute", "")),
        normalize_institute(job_b.get("institute", "")),
    )

    # Same PI at same institute = likely duplicate
    if (
        job_a.get("pi_name")
        and job_b.get("pi_name")
        and job_a["pi_name"].lower() == job_b["pi_name"].lower()
        and inst_sim > 0.7
    ):
        return True

    # High title + institute similarity
    combined = (title_sim * 0.6) + (inst_sim * 0.4)
    return combined >= threshold


def deduplicate_jobs(jobs: list[dict], threshold: float = 0.85) -> list[dict]:
    """Remove duplicate jobs, keeping the first (higher priority source) occurrence."""
    unique = []
    for job in jobs:
        is_dup = False
        for existing in unique:
            if is_duplicate(job, existing, threshold):
                is_dup = True
                logger.debug(
                    "Duplicate: '%s' ≈ '%s'",
                    job.get("title", "")[:50],
                    existing.get("title", "")[:50],
                )
                break
        if not is_dup:
            unique.append(job)

    removed = len(jobs) - len(unique)
    if removed:
        logger.info("Deduplicated: %d → %d jobs (%d removed)", len(jobs), len(unique), removed)
    return unique
