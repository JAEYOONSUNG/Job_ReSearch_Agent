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

from src import db
from src.discovery.web_search import ddg_search

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

    Uses the fast direct-scrape ``search_scholar_author`` with built-in
    circuit breaker instead of the slow ``scholarly`` library.
    """
    try:
        from src.discovery.scholar_scraper import search_scholar_author
    except ImportError:
        logger.debug("scholar_scraper unavailable, skipping Scholar lookup")
        return None

    try:
        gs_data = search_scholar_author(name, institute)
        if gs_data and gs_data.get("scholar_url"):
            logger.debug("Scholar profile for %s: %s", name, gs_data["scholar_url"])
            # The Scholar profile URL itself is not a lab URL,
            # but we return None here — the caller stores scholar_url separately.
        return None
    except Exception:
        logger.debug("Scholar lookup failed for %s", name)
        return None


# ---------------------------------------------------------------------------
# Strategy 2: University directory search
# ---------------------------------------------------------------------------

def _search_university_directory(
    name: str, domain: str, suffix: str = "lab"
) -> Optional[str]:
    """Search ``site:<domain> <name> <suffix>`` via DuckDuckGo.

    Uses a simple HTTP request to DuckDuckGo HTML search (no API key needed).
    *suffix* defaults to ``"lab"`` but can be ``"department"`` or ``""``
    to broaden the search.
    """
    query = f"site:{domain} {name} {suffix}".strip()
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        if resp.status_code == 403:
            logger.debug("DDG 403 for %s at %s", name, domain)
            return None
        resp.raise_for_status()
        time.sleep(2.0)  # be polite to DDG

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
        logger.debug("Directory search failed for %s at %s", name, domain)

    return None


def _extract_ddg_url(ddg_url: str) -> Optional[str]:
    """Extract the actual URL from a DuckDuckGo redirect link."""
    from html import unescape
    from urllib.parse import unquote

    # DuckDuckGo HTML escapes & as &amp; in href attributes
    ddg_url = unescape(ddg_url)

    # DuckDuckGo HTML search uses //duckduckgo.com/l/?uddg=<encoded_url>&...
    if "uddg=" in ddg_url:
        match = re.search(r"uddg=([^&]+)", ddg_url)
        if match:
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
# Strategy 4: DDG multi-query search (duckduckgo-search library)
# ---------------------------------------------------------------------------

def _score_url(url: str, institute: Optional[str], domain: Optional[str]) -> int:
    """Score a candidate URL for relevance as a lab/faculty page."""
    score = 0
    url_lower = url.lower()

    # Institute domain match
    if domain and domain in url_lower:
        score += 3

    # Lab / faculty / research keywords in URL
    lab_keywords = ("lab", "faculty", "professor", "research", "people", "member", "group")
    for kw in lab_keywords:
        if kw in url_lower:
            score += 2
            break

    # .edu domain
    if ".edu" in url_lower or ".ac." in url_lower:
        score += 1

    return score


def find_lab_url_multi_strategy(
    name: str, institute: Optional[str] = None, domain: Optional[str] = None,
) -> Optional[str]:
    """Search DuckDuckGo with multiple query templates to find a lab URL.

    Tries several query patterns, collects valid URLs, scores them,
    and returns the best match.

    Parameters
    ----------
    name : str
        PI full name.
    institute : str, optional
        Institute name (e.g. "MIT").
    domain : str, optional
        Institute web domain (e.g. "mit.edu").  If not given, derived
        from *institute* via ``_institute_to_domain``.
    """
    if not domain and institute:
        domain = _institute_to_domain(institute)

    # Build query templates
    queries: list[str] = []
    if domain:
        queries.append(f'site:{domain} "{name}" lab')
        queries.append(f'site:{domain} "{name}" faculty')
    if institute:
        queries.append(f'"{name}" {institute} lab homepage')
        queries.append(f'"{name}" {institute} research group')
        queries.append(f'"{name}" {institute} faculty profile')
    # Generic fallback
    queries.append(f'"{name}" lab homepage')

    seen_urls: set[str] = set()
    scored: list[tuple[int, str]] = []

    for query in queries:
        results = ddg_search(query, max_results=5)
        for r in results:
            href = r.get("href", "")
            if not href or href in seen_urls:
                continue
            seen_urls.add(href)
            if not _is_valid_lab_url(href):
                continue
            s = _score_url(href, institute, domain)
            scored.append((s, href))

        # Early exit if we found a high-confidence match
        if scored and scored[-1][0] >= 4:
            break

    if not scored:
        return None

    # Return the highest-scored URL
    scored.sort(key=lambda x: x[0], reverse=True)
    best_url = scored[0][1]
    logger.debug("DDG multi-query best for %s: %s (score=%d)", name, best_url, scored[0][0])
    return best_url


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def find_lab_url_for_pi(name: str, institute: Optional[str] = None) -> Optional[str]:
    """Try multiple strategies to find a lab URL for a single PI.

    Returns the URL string if found, else None.
    """
    # Strategy 1: DDG multi-query search
    url = find_lab_url_multi_strategy(name, institute)
    if url:
        return url

    # Strategy 3: University directory (if institute is known)
    if institute:
        domain_guess = _institute_to_domain(institute)
        if domain_guess:
            url = _search_university_directory(name, domain_guess)
            if url:
                return url

    # Strategy 4: Guess common patterns
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
        # US
        "mit": "mit.edu",
        "massachusetts institute of technology": "mit.edu",
        "stanford": "stanford.edu",
        "stanford university": "stanford.edu",
        "harvard": "harvard.edu",
        "harvard university": "harvard.edu",
        "caltech": "caltech.edu",
        "uc berkeley": "berkeley.edu",
        "university of california, berkeley": "berkeley.edu",
        "university of california, san francisco": "ucsf.edu",
        "ucsf": "ucsf.edu",
        "university of california, los angeles": "ucla.edu",
        "ucla": "ucla.edu",
        "university of california, san diego": "ucsd.edu",
        "ucsd": "ucsd.edu",
        "university of california, davis": "ucdavis.edu",
        "yale": "yale.edu",
        "yale university": "yale.edu",
        "princeton": "princeton.edu",
        "princeton university": "princeton.edu",
        "columbia university": "columbia.edu",
        "university of chicago": "uchicago.edu",
        "university of michigan": "umich.edu",
        "university of washington": "uw.edu",
        "university of pennsylvania": "upenn.edu",
        "upenn": "upenn.edu",
        "johns hopkins": "jhu.edu",
        "johns hopkins university": "jhu.edu",
        "cornell": "cornell.edu",
        "cornell university": "cornell.edu",
        "duke university": "duke.edu",
        "duke": "duke.edu",
        "northwestern university": "northwestern.edu",
        "university of wisconsin": "wisc.edu",
        "university of north carolina": "unc.edu",
        "unc": "unc.edu",
        "university of texas": "utexas.edu",
        "iowa state university": "iastate.edu",
        "baylor": "baylor.edu",
        "emory university": "emory.edu",
        "georgia tech": "gatech.edu",
        "carnegie mellon": "cmu.edu",
        "university of colorado": "colorado.edu",
        "university of minnesota": "umn.edu",
        "university of illinois": "illinois.edu",
        "university of virginia": "virginia.edu",
        "purdue": "purdue.edu",
        "rice university": "rice.edu",
        "scripps": "scripps.edu",
        "rockefeller university": "rockefeller.edu",
        "nih": "nih.gov",
        "genentech": "gene.com",
        "broad institute": "broadinstitute.org",
        # UK
        "university of oxford": "ox.ac.uk",
        "oxford": "ox.ac.uk",
        "university of cambridge": "cam.ac.uk",
        "cambridge": "cam.ac.uk",
        "ucl": "ucl.ac.uk",
        "imperial college": "imperial.ac.uk",
        "university of edinburgh": "ed.ac.uk",
        "king's college london": "kcl.ac.uk",
        "university of manchester": "manchester.ac.uk",
        "university of bristol": "bristol.ac.uk",
        "university of glasgow": "gla.ac.uk",
        "university of birmingham": "birmingham.ac.uk",
        "university of leeds": "leeds.ac.uk",
        "university of sheffield": "sheffield.ac.uk",
        "university of nottingham": "nottingham.ac.uk",
        "institute of cancer research": "icr.ac.uk",
        # EU
        "eth zurich": "ethz.ch",
        "eth": "ethz.ch",
        "karolinska": "ki.se",
        "max planck": "mpg.de",
        "epfl": "epfl.ch",
        "university of helsinki": "helsinki.fi",
        "wageningen": "wur.nl",
        "tu delft": "tudelft.nl",
        "ku leuven": "kuleuven.be",
        "university of copenhagen": "ku.dk",
        "sorbonne": "sorbonne-universite.fr",
        "pasteur": "pasteur.fr",
        # Asia
        "kaist": "kaist.ac.kr",
        "snu": "snu.ac.kr",
        "seoul national university": "snu.ac.kr",
        "postech": "postech.ac.kr",
        "yonsei": "yonsei.ac.kr",
        "korea university": "korea.ac.kr",
        "university of tokyo": "u-tokyo.ac.jp",
        "kyoto university": "kyoto-u.ac.jp",
        "nus": "nus.edu.sg",
        "nanyang technological": "ntu.edu.sg",
        "tsinghua": "tsinghua.edu.cn",
        "peking university": "pku.edu.cn",
        # Australia
        "university of melbourne": "unimelb.edu.au",
        "university of sydney": "sydney.edu.au",
    }

    for key, domain in known.items():
        if key in inst_lower:
            return domain

    # Heuristic: "University of X" -> x.edu (very rough)
    match = re.search(r"university of (\w+)", inst_lower)
    if match:
        return f"{match.group(1)}.edu"

    # Heuristic: "X University" -> x.edu
    match = re.search(r"^(\w+)\s+university", inst_lower)
    if match:
        return f"{match.group(1)}.edu"

    # Heuristic: "X Institute" or "X College" — search DuckDuckGo for the domain
    return _ddg_find_domain(institute)


def _ddg_find_domain(institute: str) -> Optional[str]:
    """Search DuckDuckGo for the institute's main website domain."""
    query = f"{institute} official website"
    url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        if resp.status_code == 403:
            return None
        resp.raise_for_status()
        time.sleep(2.0)
        urls = re.findall(r'class="result__a"[^>]*href="([^"]+)"', resp.text)
        for candidate in urls[:3]:
            real = _extract_ddg_url(candidate)
            if real and _is_valid_lab_url(real):
                parsed = urlparse(real)
                domain = parsed.netloc.lstrip("www.")
                if domain:
                    logger.debug("DDG domain for %s: %s", institute, domain)
                    return domain
    except Exception:
        pass
    return None


def find_lab_urls(max_pis: int = 100) -> dict:
    """Find lab URLs and Scholar URLs for recommended PIs that lack them.

    Uses the fast direct-scrape ``search_scholar_author`` (with circuit
    breaker) instead of the slow ``scholarly`` library.  Stores both
    ``lab_url`` and ``scholar_url`` in the PI record.

    Parameters
    ----------
    max_pis : int
        Maximum number of PIs to process per run.  PIs are sorted by
        recommendation score (highest first) so the most relevant ones
        are enriched first.  Default 100.

    Returns
    -------
    dict
        Summary: ``{"checked": int, "found_lab": int, "found_scholar": int, "failed": int}``.
    """
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT id, name, institute FROM pis "
            "WHERE is_recommended = 1 "
            "AND ((lab_url IS NULL OR lab_url = '') "
            "  OR (scholar_url IS NULL OR scholar_url = '')) "
            "ORDER BY COALESCE(recommendation_score, 0) DESC"
        ).fetchall()
        pis_to_check = [dict(r) for r in rows]

    if not pis_to_check:
        logger.info("No PIs need lab/scholar URL lookup.")
        return {"checked": 0, "found_lab": 0, "found_scholar": 0, "failed": 0}

    total_eligible = len(pis_to_check)
    if max_pis and len(pis_to_check) > max_pis:
        pis_to_check = pis_to_check[:max_pis]

    logger.info(
        "Looking up lab/scholar URLs for %d PIs (of %d eligible)",
        len(pis_to_check), total_eligible,
    )

    found_lab = 0
    found_scholar = 0
    failed = 0

    # Import the fast direct-scrape Scholar searcher (has its own circuit breaker)
    try:
        from src.discovery.scholar_scraper import search_scholar_author
        _has_scholar_scraper = True
    except ImportError:
        _has_scholar_scraper = False

    consecutive_failures = 0
    _MAX_CONSECUTIVE_FAILURES = 10  # stop early if DDG is completely blocked

    for idx, pi in enumerate(pis_to_check, 1):
        name = pi["name"]
        institute = pi.get("institute")
        pi_id = pi["id"]

        if consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            logger.warning(
                "Stopping PI lookup after %d consecutive failures (%d/%d done)",
                _MAX_CONSECUTIVE_FAILURES, idx - 1, len(pis_to_check),
            )
            break

        if idx % 10 == 1:
            logger.info("PI URL lookup progress: %d/%d", idx, len(pis_to_check))

        scholar_url = None
        lab_url = None

        # Strategy 1: Fast Google Scholar direct scrape (scholar_url + citations)
        is_single_name = " " not in name.strip()
        if _has_scholar_scraper and not is_single_name:
            gs_data = search_scholar_author(name, institute)
            if gs_data:
                scholar_url = gs_data.get("scholar_url")

        # Strategy 2: DDG multi-query for lab_url
        lab_url = find_lab_url_multi_strategy(name, institute)

        # Strategy 3: University directory search
        if not lab_url and institute:
            domain = _institute_to_domain(institute)
            if domain:
                lab_url = _search_university_directory(name, domain)

        # Persist whatever we found
        updates: list[str] = []
        values: list = []
        if scholar_url:
            updates.append("scholar_url = ?")
            values.append(scholar_url)
            found_scholar += 1
        if lab_url:
            updates.append("lab_url = ?")
            values.append(lab_url)
            found_lab += 1

        if updates:
            updates.append("updated_at = datetime('now')")
            values.append(pi_id)
            with db.get_connection() as conn:
                conn.execute(
                    f"UPDATE pis SET {', '.join(updates)} WHERE id = ?",
                    values,
                )
            logger.debug("Found URLs for %s: scholar=%s lab=%s", name, scholar_url or "-", lab_url or "-")
            consecutive_failures = 0
        else:
            failed += 1
            consecutive_failures += 1

        time.sleep(_SCHOLARLY_DELAY)

    summary = {"checked": len(pis_to_check), "found_lab": found_lab,
               "found_scholar": found_scholar, "failed": failed}
    logger.info("Lab/scholar URL search complete: %s", summary)
    return summary
