"""PI URL lookup: Scholar, lab homepage, and department URL.

Reuses existing infrastructure from seed_profiler and lab_finder,
adding a caching layer via the pis table and department URL search.
"""

import logging
import signal
import time
from typing import Optional

from src import db
from src.discovery.lab_finder import (
    _institute_to_domain,
    _search_university_directory,
    find_lab_url_for_pi,
)
from src.discovery.seed_profiler import _fetch_scholar_profile

logger = logging.getLogger(__name__)

_RATE_LIMIT = 1.5  # seconds between external requests

# Circuit breaker: after N consecutive Scholar failures, skip all further attempts
_SCHOLAR_MAX_FAILURES = 2
_scholar_consecutive_failures = 0
_scholar_disabled = False


class _ScholarTimeout(Exception):
    pass


def _scholar_timeout_handler(signum, frame):
    raise _ScholarTimeout("Scholar lookup timed out")


def _get_cached_pi(name: str, institute: Optional[str] = None) -> Optional[dict]:
    """Check the pis table for cached URL data."""
    with db.get_connection() as conn:
        if institute:
            row = conn.execute(
                "SELECT scholar_url, lab_url, dept_url, h_index, citations "
                "FROM pis WHERE name = ? AND institute = ?",
                (name, institute),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT scholar_url, lab_url, dept_url, h_index, citations "
                "FROM pis WHERE name = ?",
                (name,),
            ).fetchone()
        return dict(row) if row else None


def _lookup_dept_url(
    department: Optional[str], institute: Optional[str]
) -> Optional[str]:
    """Search for a department homepage via DuckDuckGo site-scoped search."""
    if not department or not institute:
        return None

    domain = _institute_to_domain(institute)
    if not domain:
        return None

    return _search_university_directory(department, domain)


def _cache_pi_urls(
    name: str,
    institute: Optional[str],
    scholar_url: Optional[str] = None,
    lab_url: Optional[str] = None,
    dept_url: Optional[str] = None,
    h_index: Optional[int] = None,
    citations: Optional[int] = None,
    scholar_id: Optional[str] = None,
) -> None:
    """Upsert PI URL data into the pis cache table."""
    record: dict = {"name": name}
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
    db.upsert_pi(record)


def _safe_scholar_lookup(pi_name: str, institute: Optional[str]) -> Optional[dict]:
    """Call _fetch_scholar_profile with a SIGALRM-based hard timeout."""
    old_handler = signal.signal(signal.SIGALRM, _scholar_timeout_handler)
    signal.alarm(20)  # 20-second hard kill
    try:
        return _fetch_scholar_profile(pi_name, institute)
    except _ScholarTimeout:
        logger.warning("Scholar lookup hard-timed-out for %s", pi_name)
        return None
    except Exception:
        logger.debug("Scholar lookup failed for %s", pi_name, exc_info=True)
        return None
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


def lookup_pi_urls(
    pi_name: str,
    institute: Optional[str] = None,
    department: Optional[str] = None,
) -> dict:
    """Look up Scholar URL, lab URL, and dept URL for a PI.

    Uses the pis table as a cache.  External requests are rate-limited
    to ~1.5 s each; cached lookups return instantly.

    Returns
    -------
    dict with keys: scholar_url, lab_url, dept_url, h_index, citations
    """
    global _scholar_consecutive_failures, _scholar_disabled

    result: dict = {
        "scholar_url": None,
        "lab_url": None,
        "dept_url": None,
        "h_index": None,
        "citations": None,
    }

    # 1. Cache check
    cached = _get_cached_pi(pi_name, institute)
    if cached:
        has_scholar = bool(cached.get("scholar_url"))
        has_lab = bool(cached.get("lab_url"))
        has_dept = bool(cached.get("dept_url"))
        result.update({k: v for k, v in cached.items() if v})

        if has_scholar and has_lab and has_dept:
            logger.debug("Full cache hit for %s", pi_name)
            return result

    # 2. Scholar search (if needed and not circuit-broken)
    if not result.get("scholar_url") and not _scholar_disabled:
        logger.info("Fetching Scholar profile for %s", pi_name)
        scholar_data = _safe_scholar_lookup(pi_name, institute)
        time.sleep(_RATE_LIMIT)
        if scholar_data:
            _scholar_consecutive_failures = 0
            result["scholar_url"] = scholar_data.get("scholar_url")
            result["h_index"] = scholar_data.get("h_index")
            result["citations"] = scholar_data.get("citations")
        else:
            _scholar_consecutive_failures += 1
            if _scholar_consecutive_failures >= _SCHOLAR_MAX_FAILURES:
                _scholar_disabled = True
                logger.warning(
                    "Scholar circuit breaker tripped after %d failures — "
                    "skipping remaining Scholar lookups this run",
                    _scholar_consecutive_failures,
                )

    # 3. Lab URL search (if needed)
    if not result.get("lab_url"):
        logger.info("Searching lab URL for %s", pi_name)
        lab_url = find_lab_url_for_pi(pi_name, institute)
        time.sleep(_RATE_LIMIT)
        if lab_url:
            result["lab_url"] = lab_url

    # 4. Dept URL search (if needed)
    if not result.get("dept_url") and department:
        logger.info("Searching dept URL for %s at %s", department, institute)
        dept_url = _lookup_dept_url(department, institute)
        time.sleep(_RATE_LIMIT)
        if dept_url:
            result["dept_url"] = dept_url

    # 5. Cache the results
    _cache_pi_urls(
        name=pi_name,
        institute=institute,
        scholar_url=result.get("scholar_url"),
        lab_url=result.get("lab_url"),
        dept_url=result.get("dept_url"),
        h_index=result.get("h_index"),
        citations=result.get("citations"),
    )

    return result
