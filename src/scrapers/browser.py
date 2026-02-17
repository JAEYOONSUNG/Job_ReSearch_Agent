"""Playwright-based browser scraper for sites that block simple HTTP requests.

Uses headless Chromium to bypass bot detection (Cloudflare, Madgex, etc).
Falls back gracefully if Playwright is not installed.

All Playwright operations run in a dedicated worker thread to avoid
greenlet/asyncio conflicts when called from different contexts.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Optional

logger = logging.getLogger(__name__)

# Single-threaded executor ensures all Playwright calls happen on the same thread.
_EXECUTOR = ThreadPoolExecutor(max_workers=1)

# These globals are only accessed from the worker thread.
_BROWSER = None
_CONTEXT = None
_PW = None


def _init_browser():
    """Launch browser (called only in worker thread)."""
    global _BROWSER, _CONTEXT, _PW
    if _BROWSER is None:
        from playwright.sync_api import sync_playwright
        _PW = sync_playwright().start()
        _BROWSER = _PW.chromium.launch(headless=True)
        _CONTEXT = _BROWSER.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
    return _CONTEXT


def _do_fetch(url: str, wait_selector: str, wait_ms: int, timeout: int) -> Optional[str]:
    """Fetch a page (runs in worker thread)."""
    try:
        ctx = _init_browser()
        page = ctx.new_page()
        try:
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=8000)
                except Exception:
                    pass
            if wait_ms:
                page.wait_for_timeout(wait_ms)
            return page.content()
        finally:
            page.close()
    except ImportError:
        logger.warning("Playwright not installed; browser-based scraping unavailable")
        return None
    except Exception:
        logger.exception("Browser fetch failed: %s", url)
        return None


def _do_close():
    """Close browser (runs in worker thread)."""
    global _BROWSER, _CONTEXT, _PW
    if _BROWSER:
        try:
            _BROWSER.close()
        except Exception:
            pass
        _BROWSER = None
        _CONTEXT = None
    if _PW:
        try:
            _PW.stop()
        except Exception:
            pass
        _PW = None


def fetch_page(
    url: str,
    wait_selector: str = None,
    wait_ms: int = 3000,
    timeout: int = 20000,
) -> Optional[str]:
    """Fetch a page using headless Chromium.

    All Playwright operations are dispatched to a dedicated worker thread
    to avoid greenlet/asyncio conflicts.

    Parameters
    ----------
    url : str
        The URL to load.
    wait_selector : str, optional
        CSS selector to wait for before extracting HTML.
    wait_ms : int
        Extra milliseconds to wait after page load for JS rendering.
    timeout : int
        Navigation timeout in ms.

    Returns
    -------
    str or None
        The page HTML, or None on failure.
    """
    future: Future = _EXECUTOR.submit(_do_fetch, url, wait_selector, wait_ms, timeout)
    return future.result(timeout=timeout / 1000 + 30)


def close_browser() -> None:
    """Close the shared browser instance."""
    future: Future = _EXECUTOR.submit(_do_close)
    future.result(timeout=10)
