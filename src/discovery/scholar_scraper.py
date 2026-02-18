"""Direct Google Scholar scraping for author profiles.

Replaces the scholarly + FreeProxies approach that consistently gets blocked.
Parses the author search results HTML directly (same approach as R scraper).

Rate-limiting: 5-15s random delay between requests.
Circuit breaker: 5 consecutive failures -> 5 min cooldown.
User-Agent rotation for resilience.
"""

import difflib
import logging
import random
import re
import threading
import time
from html import unescape
from typing import Optional
from urllib.parse import quote_plus, urljoin

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate limiting & circuit breaker
# ---------------------------------------------------------------------------

_GS_MIN_DELAY = 5.0
_GS_MAX_DELAY = 15.0
_GS_MAX_FAILURES = 5
_GS_COOLDOWN = 300.0  # 5 minutes

_gs_lock = threading.Lock()
_gs_last_call = 0.0
_gs_consecutive_failures = 0
_gs_disabled = False
_gs_disabled_at = 0.0

# ---------------------------------------------------------------------------
# User-Agent rotation
# ---------------------------------------------------------------------------

_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
]

_GS_BASE = "https://scholar.google.com"
_GS_TIMEOUT = 20


def _get_headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://scholar.google.com/",
    }


# ---------------------------------------------------------------------------
# HTML parsing
# ---------------------------------------------------------------------------

def _parse_author_results(html: str) -> list[dict]:
    """Parse .gs_ai_chpr elements from Google Scholar author search HTML.

    Each result card has class ``gs_ai_chpr`` and contains:
    - Name link: ``<a class="gs_ai_pho" href="/citations?user=...">``
    - Name text: ``<h3 class="gs_ai_name"><a ...>Name</a></h3>``
    - Affiliation: ``<div class="gs_ai_aff">...``
    - Cited by: ``<div class="gs_ai_cby">Cited by NN</div>``
    - Interests: ``<div class="gs_ai_int"><a ...>topic</a>``

    Returns a list of dicts with keys:
        name, profile_url, scholar_id, affiliations, cited_by, interests
    """
    results: list[dict] = []

    # Split on gs_ai_chpr blocks
    blocks = re.split(r'<div\s+class="gs_ai_chpr"', html)
    for block in blocks[1:]:  # skip text before first match
        entry: dict = {}

        # Name + profile URL
        name_match = re.search(
            r'class="gs_ai_name"[^>]*>\s*<a\s+href="([^"]*)"[^>]*>([^<]+)</a>',
            block,
        )
        if name_match:
            href = name_match.group(1)
            entry["name"] = unescape(name_match.group(2)).strip()
            entry["profile_url"] = urljoin(_GS_BASE, href)
            # Extract scholar_id from href
            id_match = re.search(r"user=([^&]+)", href)
            entry["scholar_id"] = id_match.group(1) if id_match else ""
        else:
            continue  # skip blocks without a name

        # Affiliation
        aff_match = re.search(r'class="gs_ai_aff"[^>]*>([^<]+)', block)
        entry["affiliations"] = unescape(aff_match.group(1)).strip() if aff_match else ""

        # Cited by
        cite_match = re.search(r'class="gs_ai_cby"[^>]*>[^0-9]*(\d[\d,]*)', block)
        if cite_match:
            entry["cited_by"] = int(cite_match.group(1).replace(",", ""))
        else:
            entry["cited_by"] = 0

        # Interests (may have multiple <a> tags)
        interests: list[str] = []
        int_match = re.search(r'class="gs_ai_int"[^>]*>(.*?)</div>', block, re.DOTALL)
        if int_match:
            for topic in re.findall(r">([^<]+)</a>", int_match.group(1)):
                interests.append(unescape(topic).strip())
        entry["interests"] = interests

        results.append(entry)

    return results


def _name_similarity(a: str, b: str) -> float:
    """Compute name similarity using SequenceMatcher.

    Normalises names to lowercase and handles "First Last" vs "Last, First".
    """
    def _normalise(name: str) -> str:
        # "Last, First" -> "First Last"
        if "," in name:
            parts = name.split(",", 1)
            name = f"{parts[1].strip()} {parts[0].strip()}"
        return name.lower().strip()

    return difflib.SequenceMatcher(None, _normalise(a), _normalise(b)).ratio()


def _pick_best_match(
    query_name: str,
    institute: Optional[str],
    results: list[dict],
) -> Optional[dict]:
    """Select the best matching author from search results.

    Scoring:
    - Name similarity (SequenceMatcher) — must be >= 0.5
    - Affiliation match bonus (+0.3 if institute substring found)
    - Citation count tiebreaker (log-scaled)
    """
    if not results:
        return None

    institute_lower = (institute or "").lower()
    best_score = -1.0
    best_result = None

    for r in results:
        name_sim = _name_similarity(query_name, r["name"])
        if name_sim < 0.5:
            continue

        score = name_sim

        # Affiliation bonus
        if institute_lower and institute_lower in r.get("affiliations", "").lower():
            score += 0.3

        # Citation tiebreaker (small bonus, max ~0.1)
        cited = r.get("cited_by", 0)
        if cited > 0:
            import math
            score += min(0.1, math.log10(cited) / 60)

        if score > best_score:
            best_score = score
            best_result = r

    if best_result and best_score >= 0.5:
        return best_result
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def search_scholar_author(
    name: str, institute: Optional[str] = None,
) -> Optional[dict]:
    """Search Google Scholar for an author and return profile data.

    Returns a dict with keys:
        scholar_url, scholar_id, cited_by, affiliations, interests

    Returns ``None`` if no match or circuit breaker is open.
    """
    global _gs_last_call, _gs_consecutive_failures, _gs_disabled, _gs_disabled_at

    # Circuit breaker check
    with _gs_lock:
        if _gs_disabled:
            elapsed = time.time() - _gs_disabled_at
            if elapsed >= _GS_COOLDOWN:
                logger.info("GS circuit breaker half-open after %.0fs cooldown", elapsed)
                _gs_disabled = False
                _gs_consecutive_failures = 0
            else:
                logger.debug("GS circuit breaker open (%.0fs remaining)", _GS_COOLDOWN - elapsed)
                return None

    # Rate limit
    with _gs_lock:
        now = time.time()
        elapsed = now - _gs_last_call
        delay = random.uniform(_GS_MIN_DELAY, _GS_MAX_DELAY)
        if elapsed < delay:
            time.sleep(delay - elapsed)

    # Build query
    query = name
    if institute:
        query = f"{name} {institute}"

    url = f"{_GS_BASE}/citations?view_op=search_authors&mauthors={quote_plus(query)}&hl=en"

    try:
        resp = requests.get(url, headers=_get_headers(), timeout=_GS_TIMEOUT)

        with _gs_lock:
            _gs_last_call = time.time()

        if resp.status_code == 429 or resp.status_code == 403:
            logger.warning("GS blocked (HTTP %d) for query: %s", resp.status_code, name)
            with _gs_lock:
                _gs_consecutive_failures += 1
                if _gs_consecutive_failures >= _GS_MAX_FAILURES:
                    _gs_disabled = True
                    _gs_disabled_at = time.time()
                    logger.warning(
                        "GS circuit breaker tripped after %d failures, cooldown %ds",
                        _gs_consecutive_failures, int(_GS_COOLDOWN),
                    )
            return None

        resp.raise_for_status()

        # Parse results
        results = _parse_author_results(resp.text)
        if not results:
            logger.debug("No GS author results for %s", name)
            with _gs_lock:
                _gs_consecutive_failures = 0  # not a failure, just no results
            return None

        # Pick the best match
        best = _pick_best_match(name, institute, results)
        if not best:
            logger.debug("No good name match among %d GS results for %s", len(results), name)
            with _gs_lock:
                _gs_consecutive_failures = 0
            return None

        with _gs_lock:
            _gs_consecutive_failures = 0

        return {
            "scholar_url": best["profile_url"],
            "scholar_id": best["scholar_id"],
            "cited_by": best["cited_by"],
            "affiliations": best["affiliations"],
            "interests": best["interests"],
        }

    except requests.RequestException as exc:
        logger.debug("GS request failed for %s: %s", name, exc)
        with _gs_lock:
            _gs_last_call = time.time()
            _gs_consecutive_failures += 1
            if _gs_consecutive_failures >= _GS_MAX_FAILURES:
                _gs_disabled = True
                _gs_disabled_at = time.time()
                logger.warning(
                    "GS circuit breaker tripped after %d failures, cooldown %ds",
                    _gs_consecutive_failures, int(_GS_COOLDOWN),
                )
        return None
