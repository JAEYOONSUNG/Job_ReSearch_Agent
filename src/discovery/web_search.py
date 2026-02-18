"""Rate-limited DuckDuckGo search wrapper.

Uses direct HTTP requests to DuckDuckGo HTML search with built-in
rate limiting, thread safety, and a circuit breaker.
"""

import logging
import re
import threading
import time
from html import unescape
from urllib.parse import unquote

import requests

logger = logging.getLogger(__name__)

_DDG_DELAY = 2.5  # seconds between DDG requests
_DDG_MAX_FAILURES = 15  # circuit breaker threshold (was 8, too aggressive)
_DDG_COOLDOWN = 60.0  # seconds to wait before half-open retry after breaker trips
_REQUEST_TIMEOUT = 15

_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _USER_AGENT}

_ddg_lock = threading.Lock()
_ddg_last_call = 0.0
_ddg_consecutive_failures = 0
_ddg_disabled = False
_ddg_disabled_at = 0.0  # timestamp when breaker tripped


def _extract_ddg_url(ddg_url: str) -> str | None:
    """Extract the actual URL from a DuckDuckGo redirect link."""
    ddg_url = unescape(ddg_url)
    if "uddg=" in ddg_url:
        match = re.search(r"uddg=([^&]+)", ddg_url)
        if match:
            return unquote(match.group(1))
    if ddg_url.startswith("http"):
        return ddg_url
    return None


def ddg_search(query: str, max_results: int = 5) -> list[dict]:
    """Search DuckDuckGo and return a list of result dicts.

    Each result dict has keys: ``title``, ``href``, ``body``.
    Returns an empty list on failure or if the circuit breaker is open.

    Uses direct HTTP requests to ``html.duckduckgo.com`` which gives
    consistent English results regardless of IP geolocation (unlike
    the ``duckduckgo-search`` library API).

    Thread-safe with a global rate limiter (one request at a time,
    minimum *_DDG_DELAY* seconds between requests).
    """
    global _ddg_last_call, _ddg_consecutive_failures, _ddg_disabled, _ddg_disabled_at

    if _ddg_disabled:
        # Half-open: retry after cooldown period
        if time.time() - _ddg_disabled_at >= _DDG_COOLDOWN:
            logger.info("DDG circuit breaker half-open, retrying...")
            _ddg_disabled = False
            _ddg_consecutive_failures = 0
        else:
            logger.debug("DDG circuit breaker open, skipping: %s", query)
            return []

    with _ddg_lock:
        # Rate limit
        elapsed = time.time() - _ddg_last_call
        if elapsed < _DDG_DELAY:
            time.sleep(_DDG_DELAY - elapsed)

        try:
            url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}"
            resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)

            _ddg_last_call = time.time()

            if resp.status_code == 403:
                _ddg_consecutive_failures += 1
                if _ddg_consecutive_failures >= _DDG_MAX_FAILURES:
                    _ddg_disabled = True
                    _ddg_disabled_at = time.time()
                    logger.warning("DDG circuit breaker tripped (403s), cooldown %ds", int(_DDG_COOLDOWN))
                return []

            resp.raise_for_status()

            # Parse result links and titles from DDG HTML
            # Links: <a class="result__a" href="...">title</a>
            raw_links = re.findall(
                r'class="result__a"[^>]*href="([^"]+)"[^>]*>([^<]*)</a>',
                resp.text,
            )
            # Snippets: <a class="result__snippet" ...>body</a>
            raw_snippets = re.findall(
                r'class="result__snippet"[^>]*>([^<]*)</a>',
                resp.text,
            )

            results: list[dict] = []
            for i, (raw_href, title) in enumerate(raw_links[:max_results]):
                href = _extract_ddg_url(raw_href)
                if not href:
                    continue
                results.append({
                    "title": unescape(title).strip(),
                    "href": href,
                    "body": unescape(raw_snippets[i]).strip() if i < len(raw_snippets) else "",
                })

            _ddg_consecutive_failures = 0
            return results

        except Exception:
            _ddg_last_call = time.time()
            _ddg_consecutive_failures += 1
            if _ddg_consecutive_failures >= _DDG_MAX_FAILURES:
                _ddg_disabled = True
                _ddg_disabled_at = time.time()
                logger.warning(
                    "DDG circuit breaker tripped after %d consecutive failures, cooldown %ds",
                    _ddg_consecutive_failures,
                    int(_DDG_COOLDOWN),
                )
            else:
                logger.debug("DDG search failed for: %s", query, exc_info=True)
            return []
