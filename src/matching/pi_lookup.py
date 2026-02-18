"""PI URL lookup: Scholar, lab homepage, department URL, and papers.

Reuses existing infrastructure from seed_profiler and lab_finder,
adding a caching layer via the pis table and department URL search.

Fallback chain:
1. Cache check (with negative-cache / 7-day TTL)
2. Google Scholar direct HTTP scraping -> scholar_url, citations
3. DDG multi-query -> lab_url  (if Scholar homepage is missing)
4. Semantic Scholar metadata -> h_index, citations, s2_author_id
5. S2 paper fetch -> recent_papers, top_cited_papers
6. Dept URL search
7. Cache results
"""

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from src import db
from src.discovery.lab_finder import (
    _institute_to_domain,
    _is_valid_lab_url,
    _search_university_directory,
    find_lab_url_multi_strategy,
)
from src.discovery.scholar_scraper import search_scholar_author
from src.discovery.seed_profiler import (
    fetch_pi_papers,
    fetch_semantic_scholar_metadata,
)

logger = logging.getLogger(__name__)

_RATE_LIMIT = 1.5  # seconds between external requests
_NEGATIVE_CACHE_DAYS = 7  # skip re-search within this window


def _get_cached_pi(name: str, institute: Optional[str] = None) -> Optional[dict]:
    """Check the pis table for cached URL data."""
    with db.get_connection() as conn:
        if institute:
            row = conn.execute(
                "SELECT scholar_url, lab_url, dept_url, h_index, citations,"
                " s2_author_id, recent_papers, top_cited_papers, last_scraped"
                " FROM pis WHERE name = ? AND institute = ?",
                (name, institute),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT scholar_url, lab_url, dept_url, h_index, citations,"
                " s2_author_id, recent_papers, top_cited_papers, last_scraped"
                " FROM pis WHERE name = ?",
                (name,),
            ).fetchone()
        return dict(row) if row else None


def _is_negative_cache_valid(cached: dict) -> bool:
    """Return True if the cached record was scraped recently (within 7 days).

    A record with no URLs but a recent ``last_scraped`` timestamp means
    we already tried and found nothing -- skip re-searching.
    """
    last_scraped = cached.get("last_scraped")
    if not last_scraped:
        return False
    try:
        ts = datetime.fromisoformat(last_scraped)
        return (datetime.now() - ts) < timedelta(days=_NEGATIVE_CACHE_DAYS)
    except (ValueError, TypeError):
        return False


def _lookup_dept_url(
    department: Optional[str], institute: Optional[str]
) -> Optional[str]:
    """Search for a department homepage via DuckDuckGo site-scoped search."""
    if not department or not institute:
        return None

    domain = _institute_to_domain(institute)
    if not domain:
        return None

    return _search_university_directory(department, domain, suffix="department")


def _cache_pi_urls(
    name: str,
    institute: Optional[str],
    scholar_url: Optional[str] = None,
    lab_url: Optional[str] = None,
    dept_url: Optional[str] = None,
    h_index: Optional[int] = None,
    citations: Optional[int] = None,
    scholar_id: Optional[str] = None,
    s2_author_id: Optional[str] = None,
    recent_papers: Optional[str] = None,
    top_cited_papers: Optional[str] = None,
) -> None:
    """Upsert PI URL data into the pis cache table."""
    record: dict = {"name": name, "last_scraped": datetime.now().isoformat()}
    if institute:
        record["institute"] = institute
    if scholar_url:
        record["scholar_url"] = scholar_url
    if lab_url:
        record["lab_url"] = lab_url
    if dept_url:
        record["dept_url"] = dept_url
    if h_index is not None:
        record["h_index"] = h_index
    if citations is not None:
        record["citations"] = citations
    if scholar_id:
        record["scholar_id"] = scholar_id
    if s2_author_id:
        record["s2_author_id"] = s2_author_id
    if recent_papers:
        record["recent_papers"] = recent_papers
    if top_cited_papers:
        record["top_cited_papers"] = top_cited_papers
    db.upsert_pi(record)


def lookup_pi_urls(
    pi_name: str,
    institute: Optional[str] = None,
    department: Optional[str] = None,
) -> dict:
    """Look up Scholar URL, lab URL, dept URL, and papers for a PI.

    Uses the pis table as a cache.  External requests are rate-limited.
    Google Scholar uses its own circuit breaker (in scholar_scraper).

    Fallback chain:
    1. Cache check (with 7-day negative cache TTL)
    2. Google Scholar direct scraping -> scholar_url, citations
    3. DDG multi-query -> lab_url (if Scholar homepage absent)
    4. Semantic Scholar -> h_index, citations, s2_author_id
    5. S2 paper fetch -> recent_papers, top_cited_papers
    6. Dept URL search
    7. Cache results

    Returns
    -------
    dict with keys: scholar_url, lab_url, dept_url, h_index, citations,
                    recent_papers, top_cited_papers
    """
    result: dict = {
        "scholar_url": None,
        "lab_url": None,
        "dept_url": None,
        "h_index": None,
        "citations": None,
        "recent_papers": None,
        "top_cited_papers": None,
    }

    # 1. Cache check
    cached = _get_cached_pi(pi_name, institute)
    if cached:
        result.update({k: v for k, v in cached.items() if v and k != "last_scraped"})
        has_scholar = bool(cached.get("scholar_url"))
        has_lab = bool(cached.get("lab_url"))
        has_dept = bool(cached.get("dept_url"))
        has_papers = bool(cached.get("recent_papers"))

        # Full cache hit
        if has_scholar and has_lab and has_dept and has_papers:
            logger.debug("Full cache hit for %s", pi_name)
            return result

        # Negative cache: already tried recently and found nothing -- skip
        if not has_scholar and not has_lab and _is_negative_cache_valid(cached):
            logger.debug("Negative cache hit for %s (within %d days)", pi_name, _NEGATIVE_CACHE_DAYS)
            return result

    # 2. Google Scholar direct scraping (with built-in circuit breaker)
    #    Skip single-name PIs (too ambiguous for Scholar search)
    is_single_name = " " not in pi_name.strip()
    s2_author_id = cached.get("s2_author_id") if cached else None

    if not result.get("scholar_url") and not is_single_name:
        logger.info("Fetching Scholar profile for %s (direct scrape)", pi_name)
        gs_data = search_scholar_author(pi_name, institute)

        if gs_data:
            result["scholar_url"] = gs_data.get("scholar_url")
            # Use GS cited_by as citations if we don't have it yet
            gs_cited = gs_data.get("cited_by")
            if gs_cited and result.get("citations") is None:
                result["citations"] = gs_cited

    # 3. DDG multi-query lab URL (if Scholar homepage didn't provide one)
    #    For single-name PIs, combine with institute for better results
    if not result.get("lab_url"):
        logger.debug("Trying DDG multi-query for %s", pi_name)
        lab_url = find_lab_url_multi_strategy(pi_name, institute)
        if lab_url:
            result["lab_url"] = lab_url

    # 4. Semantic Scholar metadata (h_index, citations, homepage, authorId)
    #    Now supports single-name PIs by cross-referencing with institute
    s2_meta = None
    if result.get("h_index") is None or result.get("citations") is None or not s2_author_id:
        logger.debug("Trying Semantic Scholar metadata for %s", pi_name)
        s2_meta = fetch_semantic_scholar_metadata(pi_name, institute)
        if s2_meta:
            if result.get("h_index") is None and s2_meta.get("h_index") is not None:
                result["h_index"] = s2_meta["h_index"]
            if result.get("citations") is None and s2_meta.get("citations") is not None:
                result["citations"] = s2_meta["citations"]
            # S2 may return full name for single-name PIs -- update the result
            if is_single_name and s2_meta.get("full_name"):
                result["_full_name"] = s2_meta["full_name"]
            # Bonus: S2 homepage as lab_url fallback
            if not result.get("lab_url") and s2_meta.get("homepage"):
                if _is_valid_lab_url(s2_meta["homepage"]):
                    result["lab_url"] = s2_meta["homepage"]
            # Capture authorId for paper fetching
            if s2_meta.get("authorId"):
                s2_author_id = s2_meta["authorId"]

    # 5. S2 paper fetch (recent + top cited)
    if not result.get("recent_papers") and s2_author_id:
        logger.debug("Fetching S2 papers for %s (author_id=%s)", pi_name, s2_author_id)
        paper_data = fetch_pi_papers(pi_name, institute, s2_author_id=s2_author_id)
        if paper_data:
            result["recent_papers"] = json.dumps(paper_data["recent_papers"])
            result["top_cited_papers"] = json.dumps(paper_data["top_cited_papers"])

    # 6. Dept URL search (if needed)
    if not result.get("dept_url") and department:
        dept_url = _lookup_dept_url(department, institute)
        time.sleep(_RATE_LIMIT)
        if dept_url:
            result["dept_url"] = dept_url

    # 7. Cache the results
    _cache_pi_urls(
        name=pi_name,
        institute=institute,
        scholar_url=result.get("scholar_url"),
        lab_url=result.get("lab_url"),
        dept_url=result.get("dept_url"),
        h_index=result.get("h_index"),
        citations=result.get("citations"),
        s2_author_id=s2_author_id,
        recent_papers=result.get("recent_papers"),
        top_cited_papers=result.get("top_cited_papers"),
    )

    return result
