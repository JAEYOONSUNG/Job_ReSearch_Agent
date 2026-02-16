"""Lab homepage finder.

For recommended PIs that lack a ``lab_url``, attempts to locate their lab
website using Google Scholar profile links and university directory searches.
"""

import logging
import re
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests
from scholarly import scholarly

from src import db

logger = logging.getLogger(__name__)

_SCHOLARLY_DELAY = 1.0
_REQUEST_TIMEOUT = 15  # seconds
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _USER_AGENT}


# ---------------------------------------------------------------------------
# Strategy 1: Google Scholar profile -> homepage link
# ---------------------------------------------------------------------------

def _find_url_via_scholar(name: str, institute: Optional[str] = None) -> Optional[str]:
    """Search Google Scholar for a PI and extract the homepage URL.

    Scholarly fills the ``homepage`` field from the author profile page.
    """
    try:
        query = f"{name} {institute}" if institute else name
        results = scholarly.search_author(query)
        author = next(results, None)
        if author is None:
            return None

        time.sleep(_SCHOLARLY_DELAY)
        author = scholarly.fill(author)

        homepage = author.get("homepage", "")
        if homepage and _is_valid_lab_url(homepage):
            logger.debug("Scholar homepage for %s: %s", name, homepage)
            return homepage

        # Some profiles link to the university page in the affiliation
        # or email domain -- we can derive a search from that
        email_domain = author.get("email_domain", "")
        if email_domain:
            return _search_university_directory(name, email_domain)

        return None
    except StopIteration:
        return None
    except Exception:
        logger.exception("Error searching Scholar for %s", name)
        return None


# ---------------------------------------------------------------------------
# Strategy 2: University directory search
# ---------------------------------------------------------------------------

def _search_university_directory(
    name: str, domain: str
) -> Optional[str]:
    """Try to find a lab page by searching ``site:<domain> <name> lab``.

    Uses a simple HTTP request to DuckDuckGo HTML search (no API key needed).
    """
    query = f"site:{domain} {name} lab"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        resp.raise_for_status()
        time.sleep(1.0)  # be polite

        # Extract result URLs from DuckDuckGo HTML
        # Links are in <a class="result__a" href="...">
        urls = re.findall(r'class="result__a"[^>]*href="([^"]+)"', resp.text)
        for candidate in urls:
            # DuckDuckGo wraps URLs in a redirect; extract the real URL
            real = _extract_ddg_url(candidate)
            if real and _is_valid_lab_url(real):
                logger.debug("Directory search found %s for %s", real, name)
                return real
    except Exception:
        logger.exception("Directory search failed for %s at %s", name, domain)

    return None


def _extract_ddg_url(ddg_url: str) -> Optional[str]:
    """Extract the actual URL from a DuckDuckGo redirect link."""
    # DuckDuckGo HTML search uses //duckduckgo.com/l/?uddg=<encoded_url>&...
    if "uddg=" in ddg_url:
        match = re.search(r"uddg=([^&]+)", ddg_url)
        if match:
            from urllib.parse import unquote
            return unquote(match.group(1))
    # Sometimes URLs are direct
    if ddg_url.startswith("http"):
        return ddg_url
    return None


# ---------------------------------------------------------------------------
# Strategy 3: Guess common lab URL patterns
# ---------------------------------------------------------------------------

def _guess_lab_url(name: str, institute: Optional[str] = None) -> Optional[str]:
    """Try common lab URL patterns (lastname-lab at known domains).

    This is a heuristic fallback.
    """
    last_name = name.split()[-1].lower() if name.split() else ""
    if not last_name:
        return None

    # Common patterns
    patterns = [
        f"https://www.{last_name}lab.org",
        f"https://{last_name}lab.org",
        f"https://www.{last_name}lab.com",
    ]

    for url in patterns:
        if _url_is_reachable(url):
            logger.debug("Guessed URL works: %s for %s", url, name)
            return url

    return None


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _is_valid_lab_url(url: str) -> bool:
    """Check whether *url* looks like a plausible lab/faculty page."""
    if not url or not url.startswith("http"):
        return False

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # Reject social media, generic sites
    reject_domains = {
        "twitter.com",
        "x.com",
        "linkedin.com",
        "facebook.com",
        "youtube.com",
        "researchgate.net",
        "orcid.org",
        "github.com",
        "wikipedia.org",
    }
    for rd in reject_domains:
        if rd in domain:
            return False

    return True


def _url_is_reachable(url: str) -> bool:
    """Return True if *url* returns a 200 status (HEAD request)."""
    try:
        resp = requests.head(
            url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT, allow_redirects=True
        )
        return resp.status_code == 200
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_lab_url_for_pi(name: str, institute: Optional[str] = None) -> Optional[str]:
    """Try multiple strategies to find a lab URL for a single PI.

    Returns the URL string if found, else None.
    """
    # Strategy 1: Google Scholar homepage
    url = _find_url_via_scholar(name, institute)
    if url:
        return url

    time.sleep(_SCHOLARLY_DELAY)

    # Strategy 2: University directory (if institute is known)
    if institute:
        # Try to extract a domain from the institute name
        # e.g. "MIT" -> mit.edu, "Stanford University" -> stanford.edu
        domain_guess = _institute_to_domain(institute)
        if domain_guess:
            url = _search_university_directory(name, domain_guess)
            if url:
                return url

    # Strategy 3: Guess common patterns
    url = _guess_lab_url(name, institute)
    if url:
        return url

    return None


def _institute_to_domain(institute: str) -> Optional[str]:
    """Best-effort mapping of institute name to web domain.

    Uses a small lookup table for common universities plus a heuristic.
    """
    if not institute:
        return None

    inst_lower = institute.lower().strip()

    # Common mappings
    known: dict[str, str] = {
        "mit": "mit.edu",
        "massachusetts institute of technology": "mit.edu",
        "stanford": "stanford.edu",
        "stanford university": "stanford.edu",
        "harvard": "harvard.edu",
        "harvard university": "harvard.edu",
        "caltech": "caltech.edu",
        "uc berkeley": "berkeley.edu",
        "university of california, berkeley": "berkeley.edu",
        "yale": "yale.edu",
        "yale university": "yale.edu",
        "princeton": "princeton.edu",
        "princeton university": "princeton.edu",
        "columbia university": "columbia.edu",
        "university of chicago": "uchicago.edu",
        "university of michigan": "umich.edu",
        "university of washington": "uw.edu",
        "johns hopkins": "jhu.edu",
        "johns hopkins university": "jhu.edu",
        "eth zurich": "ethz.ch",
        "eth": "ethz.ch",
        "university of oxford": "ox.ac.uk",
        "oxford": "ox.ac.uk",
        "university of cambridge": "cam.ac.uk",
        "cambridge": "cam.ac.uk",
        "ucl": "ucl.ac.uk",
        "imperial college": "imperial.ac.uk",
        "kaist": "kaist.ac.kr",
        "university of tokyo": "u-tokyo.ac.jp",
        "max planck": "mpg.de",
        "nih": "nih.gov",
    }

    for key, domain in known.items():
        if key in inst_lower:
            return domain

    # Heuristic: "University of X" -> x.edu (very rough)
    match = re.search(r"university of (\w+)", inst_lower)
    if match:
        return f"{match.group(1)}.edu"

    return None


def find_lab_urls() -> dict:
    """Find lab URLs for all recommended PIs that lack one.

    Returns
    -------
    dict
        Summary: ``{"checked": int, "found": int, "failed": int}``.
    """
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, institute FROM pis "
            "WHERE is_recommended = 1 AND (lab_url IS NULL OR lab_url = '')"
        ).fetchall()
        pis_to_check = [dict(r) for r in rows]

    if not pis_to_check:
        logger.info("No PIs need lab URL lookup.")
        return {"checked": 0, "found": 0, "failed": 0}

    logger.info("Looking up lab URLs for %d PIs", len(pis_to_check))

    found = 0
    failed = 0

    for pi in pis_to_check:
        name = pi["name"]
        institute = pi.get("institute")
        pi_id = pi["id"]

        logger.info("Searching lab URL for %s (%s)", name, institute or "?")

        url = find_lab_url_for_pi(name, institute)

        if url:
            with db.get_connection() as conn:
                conn.execute(
                    "UPDATE pis SET lab_url = ?, updated_at = datetime('now') WHERE id = ?",
                    (url, pi_id),
                )
            found += 1
            logger.info("Found lab URL for %s: %s", name, url)
        else:
            failed += 1
            logger.debug("No lab URL found for %s", name)

        # Rate limit between PIs
        time.sleep(_SCHOLARLY_DELAY)

    summary = {"checked": len(pis_to_check), "found": found, "failed": failed}
    logger.info("Lab URL search complete: %s", summary)
    return summary
