"""Deduplicate job listings across sources."""

import logging
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# Normalisation patterns for postdoc position variants
_POSITION_VARIANTS = re.compile(
    r"\b(post[\s-]?doc(?:toral)?|postdoc)\b", re.IGNORECASE
)


def normalize_title(title: str) -> str:
    """Normalize job title for comparison."""
    title = (title or "").lower().strip()
    title = re.sub(r"\s+", " ", title)
    # Unify postdoc variants: "Post-Doc" / "Postdoctoral" / "Post Doc" -> "postdoc"
    title = _POSITION_VARIANTS.sub("postdoc", title)
    # Remove common prefixes
    for prefix in ("postdoc", "research"):
        title = re.sub(rf"^{prefix}\s+(position|fellow|researcher|associate)\s*[-:–]?\s*", "", title)
    return title


def normalize_institute(name: str) -> str:
    """Normalize institution name."""
    name = (name or "").lower().strip()
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
    inst_a = normalize_institute(job_a.get("institute", ""))
    inst_b = normalize_institute(job_b.get("institute", ""))
    inst_sim = similarity(inst_a, inst_b)

    # Same PI at same institute with loosely similar title -> duplicate
    if (
        job_a.get("pi_name")
        and job_b.get("pi_name")
        and job_a["pi_name"].lower() == job_b["pi_name"].lower()
        and inst_sim > 0.7
        and title_sim > 0.4
    ):
        return True

    # Same institute + very similar normalised title (catches "Postdoc Research Associate" ≈ "Post-Doc Research Associate")
    if inst_a and inst_b and inst_a == inst_b and title_sim >= 0.6:
        return True

    # High title + institute similarity
    combined = (title_sim * 0.6) + (inst_sim * 0.4)
    return combined >= threshold


def _pick_best(job_a: dict, job_b: dict) -> dict:
    """Between two duplicates, keep the one with the longer description."""
    len_a = len(job_a.get("description") or "")
    len_b = len(job_b.get("description") or "")
    return job_a if len_a >= len_b else job_b


def deduplicate_jobs(jobs: list[dict], threshold: float = 0.85) -> list[dict]:
    """Remove duplicate jobs, keeping the entry with the longest description."""
    unique: list[dict] = []
    for job in jobs:
        dup_idx = None
        for i, existing in enumerate(unique):
            if is_duplicate(job, existing, threshold):
                dup_idx = i
                logger.debug(
                    "Duplicate: '%s' ≈ '%s'",
                    job.get("title", "")[:50],
                    existing.get("title", "")[:50],
                )
                break
        if dup_idx is not None:
            # Replace with the better entry (longer description)
            unique[dup_idx] = _pick_best(unique[dup_idx], job)
        else:
            unique.append(job)

    removed = len(jobs) - len(unique)
    if removed:
        logger.info("Deduplicated: %d → %d jobs (%d removed)", len(jobs), len(unique), removed)
    return unique
