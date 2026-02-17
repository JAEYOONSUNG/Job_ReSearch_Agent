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
