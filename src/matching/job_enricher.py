"""Job enrichment subagent — resolves aggregator URLs and fills missing fields.

Handles:
1. Aggregator resolution (Inside Higher Ed, PhDFinder via LinkedIn → actual job page)
2. Single-name PI full-name resolution via Semantic Scholar
3. PI URL enrichment updates back to the jobs table
"""

import logging
import re
import time
from typing import Optional
from urllib.parse import urlparse

import requests

from src import db

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT = 20
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _USER_AGENT}

# Institutes that are actually aggregators, not real employers
_AGGREGATOR_INSTITUTES = {
    "inside higher ed",
    "phdfinder",
    "higher ed jobs",
    "higheredjobs",
    "academickeys",
    "chronicle of higher education",
    "nature careers",
    "nature",
    "the",
    "times higher education",
}


# ---------------------------------------------------------------------------
# 1. Aggregator detection & resolution
# ---------------------------------------------------------------------------

def _is_aggregator(job: dict) -> bool:
    """Return True if the job's institute is an aggregator, not a real employer."""
    inst = (job.get("institute") or "").strip().lower()
    return inst in _AGGREGATOR_INSTITUTES


def _is_linkedin_url(url: str) -> bool:
    return "linkedin.com" in (url or "")


def _resolve_linkedin_job(url: str) -> Optional[dict]:
    """Follow a LinkedIn job URL and extract the real employer and description.

    LinkedIn public pages expose:
    - ``<title>`` with pattern: ``<Company> hiring <Title> in <Location> | LinkedIn``
    - ``show-more-less-html__markup`` div with the full job description
    - JSON-LD ``JobPosting`` (sometimes)

    From the description text we can infer the real institute when the
    listed company is an aggregator like Inside Higher Ed.
    """
    from html import unescape as html_unescape

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            logger.debug("LinkedIn URL returned %d: %s", resp.status_code, url)
            return None

        html = resp.text
        result: dict = {}

        # --- Title parsing: "<Company> hiring <Title> in <Location> | LinkedIn"
        title_tag = re.search(r'<title>([^<]+)</title>', html)
        if title_tag:
            title_text = html_unescape(title_tag.group(1))
            hiring_match = re.match(
                r'(.+?)\s+hiring\s+(.+?)\s+in\s+(.+?)\s*\|', title_text,
            )
            if hiring_match:
                result["location"] = hiring_match.group(3).strip()

        # --- Description from show-more-less-html__markup
        desc_match = re.search(
            r'class="show-more-less-html__markup[^"]*"[^>]*>(.*?)</div>',
            html, re.DOTALL,
        )
        if desc_match:
            desc_html = desc_match.group(1)
            desc_text = re.sub(r'<[^>]+>', ' ', desc_html)
            desc_text = html_unescape(desc_text)
            desc_text = re.sub(r'\s+', ' ', desc_text).strip()
            if len(desc_text) > 50:
                result["description"] = desc_text[:3000]

        # --- JSON-LD (if available)
        import json
        ld_blocks = re.findall(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        for block in ld_blocks:
            try:
                ld = json.loads(block)
                if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                    org = ld.get("hiringOrganization", {})
                    if isinstance(org, dict) and org.get("name"):
                        org_name = org["name"]
                        if org_name.lower() not in _AGGREGATOR_INSTITUTES:
                            result["institute"] = org_name
                    break
            except (json.JSONDecodeError, TypeError):
                continue

        # --- Infer real institute from description + location
        if not result.get("institute") and result.get("description"):
            desc = result["description"]
            loc = result.get("location", "")

            # Strategy A: "Location: <University>" or "About The University Of <Place>"
            loc_inst = re.search(
                r'(?:Location|Employer|Organization|Institution)\s*:\s*'
                r'((?:[A-Z][\w.]+\s+){1,6}(?:University|College|Institute|Hospital|Center))',
                desc,
            )
            if not loc_inst:
                # UGA-style: "About The University Of Georgia"
                loc_inst = re.search(
                    r'About\s+(?:The\s+)?(University\s+[Oo]f\s+[A-Z][\w]+(?:\s+(?:at\s+)?[A-Za-z]+){0,5})',
                    desc,
                )
            if loc_inst:
                # Strip trailing non-name words (e.g. "consistently ranks")
                _inst_trail = {
                    "consistently", "ranks", "is", "has", "was", "the",
                    "a", "an", "and", "or", "for", "in", "with", "where",
                    "offers", "seeks", "invites", "located", "founded",
                }
                inst_text = loc_inst.group(1).strip()
                words = inst_text.split()
                while len(words) > 3 and words[-1].lower() in _inst_trail:
                    words.pop()
                result["institute"] = " ".join(words)
            else:
                # Strategy B: "<Place> University" or "University of <Place>"
                uni_patterns = re.findall(
                    r'((?:[A-Z][a-z]+\s+){0,3}'
                    r'(?:University|College|Institute|Hospital)'
                    r'(?:\s+of\s+(?:the\s+)?[A-Z][a-z]+(?:\s+(?:at\s+)?[A-Za-z]+){0,5})?)',
                    desc,
                )
                # Words that signal a department/section header, not an institute
                _noise_words = {
                    "about", "location", "biochemistry", "pathology",
                    "engineering", "medicine", "chemistry", "biology",
                    "physics", "psychology", "sociology", "economics",
                    "position", "description", "overview", "summary",
                    "department", "division", "section", "school",
                    "open", "date", "close", "closing", "posted",
                    "review", "application", "apply", "submit",
                }
                # Trailing words that should be stripped from institute names
                _trailing_noise = {
                    "open", "date", "close", "posted", "review",
                    "application", "apply", "submit", "is", "has", "was",
                    "the", "a", "an", "and", "or", "for", "in", "at",
                    "consistently", "ranks", "offers", "seeks", "invites",
                    "located", "founded", "where", "with",
                }
                for candidate in uni_patterns:
                    candidate = candidate.strip().rstrip(",. ")
                    # Strip trailing noise words
                    words = candidate.split()
                    while len(words) > 2 and words[-1].lower() in _trailing_noise:
                        words.pop()
                    candidate = " ".join(words)
                    if len(candidate) < 10:
                        continue
                    if candidate.lower().startswith(("the ", "a ", "center ")):
                        continue
                    # Reject if first word is a department/noise word
                    first_word = candidate.split()[0].lower()
                    if first_word in _noise_words:
                        continue
                    # Reject if contains noise words like "About The University"
                    lower_cand = candidate.lower()
                    if any(w in lower_cand for w in ("about the", "location", "overview")):
                        continue
                    result["institute"] = candidate
                    break

            # Fallback: use location city to infer well-known institutes
            if not result.get("institute") and loc:
                _LOCATION_TO_INSTITUTE = {
                    "chapel hill": "University of North Carolina at Chapel Hill",
                    "durham, nc": "Duke University",
                    "boston": "Boston University",
                    "cambridge, ma": "Harvard University",
                    "new haven": "Yale University",
                    "baltimore": "Johns Hopkins University",
                    "ann arbor": "University of Michigan",
                    "madison, wi": "University of Wisconsin-Madison",
                    "augusta": "Augusta University",
                    "palo alto": "Stanford University",
                    "seattle": "University of Washington",
                    "houston": "Baylor College of Medicine",
                    "st. louis": "Washington University in St. Louis",
                    "san francisco": "UC San Francisco",
                    "la jolla": "UC San Diego",
                    "philadelphia": "University of Pennsylvania",
                    "pittsburgh": "University of Pittsburgh",
                    "new york": "New York University",
                    "chicago": "University of Chicago",
                    "atlanta": "Emory University",
                    "nashville": "Vanderbilt University",
                    "rochester, mn": "Mayo Clinic",
                }
                loc_lower = loc.lower()
                for key, inst in _LOCATION_TO_INSTITUTE.items():
                    if key in loc_lower:
                        result["institute"] = inst
                        break

        # --- Extract deadline from description
        if result.get("description"):
            from src.matching.job_parser import extract_deadline
            deadline = extract_deadline(result["description"])
            if deadline:
                result["deadline"] = deadline

        return result if result else None

    except Exception:
        logger.debug("Failed to resolve LinkedIn URL: %s", url, exc_info=True)
        return None


def _resolve_from_description(job: dict) -> Optional[str]:
    """Extract the real institute name from a job's description text.

    Used for aggregator jobs (Nature Careers, Times Higher Education, etc.)
    whose descriptions already contain the actual employer name.

    Applies multiple strategies:
    - Strategy A: Explicit labels like "Employer: X University"
    - Strategy B: Regex for university/institute name patterns
    - Strategy C: Contextual phrases like "X invites applications"
    - Strategy D: ScholarshipDB leading institute text

    Returns the resolved institute name, or None if not found.
    Updates the job dict in-place with the resolved institute.
    """
    desc = (job.get("description") or "").strip()
    if not desc:
        return None

    institute = None

    # Words that signal a department/section header, not an institute
    _noise_words = {
        "about", "location", "biochemistry", "pathology",
        "engineering", "medicine", "chemistry", "biology",
        "physics", "psychology", "sociology", "economics",
        "position", "description", "overview", "summary",
        "department", "division", "section", "school", "college",
        "open", "date", "close", "closing", "posted",
        "review", "application", "apply", "submit",
    }
    # Trailing words that should be stripped from institute names
    _trailing_noise = {
        "open", "date", "close", "posted", "review",
        "application", "apply", "submit", "is", "has", "was",
        "the", "a", "an", "and", "or", "for", "in", "at",
        "consistently", "ranks", "offers", "seeks", "invites",
        "located", "founded", "where", "with", "since", "our",
    }

    def _clean_candidate(text: str) -> Optional[str]:
        """Strip trailing noise words and validate a candidate name."""
        text = text.strip().rstrip(",. ")
        words = text.split()
        while len(words) > 2 and words[-1].lower() in _trailing_noise:
            words.pop()
        text = " ".join(words)
        if len(text) < 10:
            return None
        if text.lower().startswith(("the ", "a ", "center ")):
            return None
        first_word = text.split()[0].lower()
        if first_word in _noise_words:
            return None
        lower = text.lower()
        if any(w in lower for w in ("about the", "location", "overview")):
            return None
        return text

    # Strategy 0: EURAXESS-style "Organisation/Company\nSome Institute Name"
    org_match = re.search(
        r'Organisation/Company\s*\n?\s*(.+?)(?:\n|Research Field|Department|$)',
        desc,
    )
    if org_match:
        org_name = org_match.group(1).strip()
        if len(org_name) >= 3 and org_name.lower() not in (
            "n/a", "unknown", "various", "multiple",
        ):
            institute = org_name

    # Strategy A: Explicit label patterns (Employer/Organization/Institution)
    if not institute:
        loc_inst = re.search(
            r'(?:Location|Employer|Organization|Institution)\s*:\s*'
            r'((?:[A-Z][\w.]+\s+){1,6}(?:University|College|Institute|Hospital|Center|Centre|Universit[àéè]))',
            desc,
        )
        if not loc_inst:
            # "About The University Of Georgia"
            loc_inst = re.search(
                r'About\s+(?:The\s+)?(University\s+[Oo]f\s+[A-Z][\w]+(?:\s+(?:at\s+)?[A-Za-z]+){0,5})',
                desc,
            )
        if loc_inst:
            candidate = loc_inst.group(1).strip()
            words = candidate.split()
            while len(words) > 3 and words[-1].lower() in _trailing_noise:
                words.pop()
            institute = " ".join(words)

    # Strategy C: Contextual invitation/seeking phrases
    # "The Mohammed VI Polytechnic University invites applications"
    # "King Abdullah University is seeking"
    if not institute:
        ctx_patterns = [
            # "The <Name> invites applications" / "is seeking" / "is looking for"
            r'(?:The\s+|At\s+)?'
            r'((?:[A-Z][\w\'-]+\s+){1,6}'
            r'(?:University|College|Institute|Hospital|Center|Centre|Universit[àéè]|Polytechnic)'
            r'(?:\s+of\s+(?:the\s+)?[A-Z][\w]+(?:\s+[A-Za-z]+){0,3})?)'
            r'\s+(?:invites?|is\s+(?:seeking|looking|recruiting|offering)|seeks|offers|has\s+an?\s+opening)',
            # "<Name> — <dept> is seeking" (sometimes institute precedes a dash)
            r'((?:[A-Z][\w\'-]+\s+){1,6}'
            r'(?:University|College|Institute|Hospital|Center|Centre|Universit[àéè]))'
            r'\s*[–—-]\s*.{0,80}?\s+(?:is\s+(?:seeking|looking|recruiting)|invites?)',
        ]
        for pat in ctx_patterns:
            m = re.search(pat, desc)
            if m:
                candidate = _clean_candidate(m.group(1))
                if candidate:
                    institute = candidate
                    break

    # Strategy D: ScholarshipDB format — description often starts with
    # institute-related text in the first ~300 chars
    if not institute:
        # Check the first 300 characters for an institute name
        head = desc[:300]
        head_inst = re.search(
            r'((?:[A-Z][\w\'-]+\s+){0,4}'
            r'(?:University|College|Institute|Hospital|Center|Centre|Universit[àéè]|Polytechnic)'
            r'(?:\s+(?:of|for|de|di)\s+(?:the\s+)?[A-Z][\w]+(?:\s+[A-Za-z]+){0,3})?)',
            head,
        )
        if head_inst:
            candidate = _clean_candidate(head_inst.group(1))
            if candidate:
                institute = candidate

    # Strategy B: General university/institute pattern scan (full description)
    if not institute:
        uni_patterns = re.findall(
            r'((?:[A-Z][a-z]+\s+){0,3}'
            r'(?:University|College|Institute|Hospital|Polytechnic)'
            r'(?:\s+of\s+(?:the\s+)?[A-Z][a-z]+(?:\s+(?:at\s+)?[A-Za-z]+){0,5})?)',
            desc,
        )
        for candidate in uni_patterns:
            cleaned = _clean_candidate(candidate)
            if cleaned:
                institute = cleaned
                break

    # Also try broader patterns for European institutes
    if not institute:
        eu_patterns = re.findall(
            r'((?:[A-Z][a-z]+\s+){1,3}'
            r'(?:Research\s+)?(?:Center|Centre|Institut|Laboratory|Laboratorium)'
            r'(?:\s+(?:of|for|de|di)\s+[A-Z][a-z]+(?:\s+[A-Za-z]+){0,2})?)',
            desc,
        )
        for candidate in eu_patterns:
            cleaned = _clean_candidate(candidate)
            if cleaned:
                institute = cleaned
                break

    if institute:
        old_inst = job.get("institute", "")
        job["institute"] = institute
        logger.info("Resolved institute from description: %s -> %s", old_inst, institute)

    return institute


def _persist_updates(job: dict, updates: dict) -> None:
    """Write update dict to the DB for the given job (by URL)."""
    if not updates or not job.get("url"):
        return
    with db.get_connection() as conn:
        set_parts = []
        vals = []
        for k, v in updates.items():
            set_parts.append(f"{k} = ?")
            vals.append(v)
        vals.append(job["url"])
        conn.execute(
            f"UPDATE jobs SET {', '.join(set_parts)} WHERE url = ?",
            vals,
        )


def resolve_aggregator_jobs(jobs: list[dict]) -> list[dict]:
    """Resolve aggregator jobs to find real institute and description.

    For LinkedIn-sourced aggregator jobs (Inside Higher Ed, PhDFinder etc.),
    follows the LinkedIn URL to extract the actual employer.

    For non-LinkedIn aggregator jobs (Nature Careers via ScholarshipDB,
    Times Higher Education via EURAXESS etc.), extracts the real institute
    from the job description already stored in the DB.

    Updates jobs in-place and persists changes to DB.
    """
    # --- Pass 1: LinkedIn-sourced aggregator jobs ---
    linkedin_candidates = [
        j for j in jobs
        if _is_aggregator(j) and _is_linkedin_url(j.get("url", ""))
    ]

    # --- Pass 2: Non-LinkedIn aggregator jobs (description-based resolution) ---
    description_candidates = [
        j for j in jobs
        if _is_aggregator(j) and not _is_linkedin_url(j.get("url", ""))
    ]

    total = len(linkedin_candidates) + len(description_candidates)
    if total == 0:
        return jobs

    logger.info(
        "Resolving %d aggregator jobs (%d LinkedIn, %d description-based)",
        total, len(linkedin_candidates), len(description_candidates),
    )
    resolved = 0

    # --- LinkedIn resolution (existing logic) ---
    for job in linkedin_candidates:
        url = job["url"]
        logger.debug("Resolving aggregator job (LinkedIn): %s", url)

        info = _resolve_linkedin_job(url)
        time.sleep(1.5)

        if not info:
            continue

        updates: dict = {}

        if info.get("institute"):
            old_inst = job.get("institute", "")
            job["institute"] = info["institute"]
            updates["institute"] = info["institute"]
            logger.info("Resolved institute: %s -> %s", old_inst, info["institute"])

        if info.get("description") and len(info["description"]) > len(job.get("description") or ""):
            job["description"] = info["description"]
            updates["description"] = info["description"]

            # Re-parse PI name, field, deadline from new description
            from src.matching.job_parser import extract_pi_name, infer_field, extract_deadline
            if not job.get("pi_name"):
                pi = extract_pi_name(info["description"])
                if pi:
                    job["pi_name"] = pi
                    updates["pi_name"] = pi
            if not job.get("field"):
                field = infer_field(info["description"])
                if field:
                    job["field"] = field
                    updates["field"] = field
            if not job.get("deadline"):
                dl = extract_deadline(info["description"])
                if dl:
                    job["deadline"] = dl
                    updates["deadline"] = dl

        if info.get("deadline") and not job.get("deadline"):
            job["deadline"] = info["deadline"]
            updates["deadline"] = info["deadline"]

        if info.get("location"):
            from src.scrapers.base import BaseScraper
            country = BaseScraper.guess_country(info["location"])
            if country and not job.get("country"):
                job["country"] = country
                updates["country"] = country

        # Persist to DB
        if updates:
            _persist_updates(job, updates)
            resolved += 1

    # --- Description-based resolution (Nature Careers, THE, etc.) ---
    for job in description_candidates:
        logger.debug("Resolving aggregator job (description): %s", job.get("url", "?"))

        inst = _resolve_from_description(job)
        if inst:
            _persist_updates(job, {"institute": inst})
            resolved += 1

    logger.info("Resolved %d/%d aggregator jobs", resolved, total)
    return jobs


# ---------------------------------------------------------------------------
# 2. Single-name PI full-name resolution
# ---------------------------------------------------------------------------

def resolve_single_name_pis(jobs: list[dict]) -> list[dict]:
    """For jobs with single-name PIs, try to find the full name via S2.

    Updates pi_name in the job dict and DB when a full name is found.
    """
    from src.matching.pi_lookup import lookup_pi_urls

    candidates = [
        j for j in jobs
        if j.get("pi_name")
        and " " not in j["pi_name"].strip()
        and j.get("institute")
    ]
    if not candidates:
        return jobs

    logger.info("Resolving %d single-name PIs", len(candidates))
    resolved = 0

    # Group by (pi_name, institute) to avoid duplicate lookups
    seen: dict[tuple, Optional[str]] = {}

    for job in candidates:
        key = (job["pi_name"].lower(), (job.get("institute") or "").lower())
        if key in seen:
            full_name = seen[key]
        else:
            urls = lookup_pi_urls(job["pi_name"], job.get("institute"), job.get("department"))
            full_name = urls.get("_full_name")
            seen[key] = full_name

        if full_name and " " in full_name:
            old_name = job["pi_name"]
            job["pi_name"] = full_name
            resolved += 1
            logger.info("Resolved PI name: %s → %s (%s)", old_name, full_name, job.get("institute"))

            # Persist to DB
            if job.get("url"):
                with db.get_connection() as conn:
                    conn.execute(
                        "UPDATE jobs SET pi_name = ? WHERE url = ? AND pi_name = ?",
                        (full_name, job["url"], old_name),
                    )

    logger.info("Resolved %d/%d single-name PIs to full names", resolved, len(candidates))
    return jobs


# ---------------------------------------------------------------------------
# 3. Main enrichment entry point
# ---------------------------------------------------------------------------

def enrich_jobs_deep(jobs: list[dict]) -> list[dict]:
    """Run all deep enrichment steps on a list of jobs.

    Steps:
    1. Resolve aggregator institutes (Inside Higher Ed → real institute)
    2. Resolve single-name PIs (Badran → Ahmed Badran via S2 + institute)
    3. Trigger PI URL lookup for newly resolved PIs

    Called from pipeline.py after scoring but before reporting.
    """
    logger.info("Deep enrichment: %d jobs", len(jobs))

    # Step 1: Resolve aggregators
    jobs = resolve_aggregator_jobs(jobs)

    # Step 2: Resolve single-name PIs
    jobs = resolve_single_name_pis(jobs)

    # Step 3: Re-run PI enrichment for newly-resolved full-name PIs
    from src.matching.pi_lookup import lookup_pi_urls
    newly_full = [
        j for j in jobs
        if j.get("pi_name") and " " in j["pi_name"]
        and not j.get("scholar_url") and not j.get("lab_url")
    ]
    if newly_full:
        logger.info("Re-enriching %d newly resolved PIs", len(newly_full))
        for job in newly_full:
            try:
                urls = lookup_pi_urls(job["pi_name"], job.get("institute"), job.get("department"))
                for key in ("scholar_url", "lab_url", "dept_url", "h_index", "citations",
                            "recent_papers", "top_cited_papers"):
                    if urls.get(key) and not job.get(key):
                        job[key] = urls[key]
            except Exception:
                logger.debug("Re-enrichment failed for %s", job.get("pi_name"))

    logger.info("Deep enrichment complete")
    return jobs


# ---------------------------------------------------------------------------
# 4. One-time migration: fix existing aggregator institute names in the DB
# ---------------------------------------------------------------------------

def fix_existing_aggregators() -> int:
    """Resolve institute names for existing aggregator jobs in the database.

    Queries all jobs whose institute matches the aggregator list, then
    attempts to extract the real institute from each job's description.
    Updates the DB in-place.

    Returns the number of jobs successfully resolved.
    """
    placeholders = ", ".join("?" for _ in _AGGREGATOR_INSTITUTES)
    query = (
        f"SELECT id, title, institute, description, url "
        f"FROM jobs WHERE LOWER(TRIM(institute)) IN ({placeholders})"
    )
    params = list(_AGGREGATOR_INSTITUTES)

    with db.get_connection() as conn:
        rows = conn.execute(query, params).fetchall()

    jobs = [dict(r) for r in rows]
    if not jobs:
        logger.info("fix_existing_aggregators: no aggregator jobs found")
        return 0

    logger.info("fix_existing_aggregators: found %d aggregator jobs to resolve", len(jobs))
    resolved = 0

    from src.matching.scorer import guess_country_from_institute, get_region, get_institution_tier

    for job in jobs:
        inst = _resolve_from_description(job)
        if inst:
            # Also infer country/region/tier from the resolved institute
            country = guess_country_from_institute(inst)
            region = get_region(country) if country else None
            tier = get_institution_tier(inst)
            with db.get_connection() as conn:
                updates = ["institute = ?"]
                params = [inst]
                if country:
                    updates.append("country = ?")
                    params.append(country)
                if region:
                    updates.append("region = ?")
                    params.append(region)
                if tier:
                    updates.append("tier = ?")
                    params.append(tier)
                params.append(job["id"])
                conn.execute(
                    f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?",
                    params,
                )
            resolved += 1
            logger.info(
                "Fixed job #%d: %s -> %s (country=%s, tier=%s) (%s)",
                job["id"], job.get("institute", ""), inst,
                country or "?", tier,
                (job.get("title") or "")[:60],
            )
        else:
            logger.debug(
                "Could not resolve institute for job #%d: %s",
                job["id"], (job.get("title") or "")[:60],
            )

    logger.info(
        "fix_existing_aggregators: resolved %d/%d aggregator jobs",
        resolved, len(jobs),
    )
    return resolved


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )
    db.init_db()
    n = fix_existing_aggregators()
    print(f"Resolved {n} aggregator jobs.")
    sys.exit(0)
