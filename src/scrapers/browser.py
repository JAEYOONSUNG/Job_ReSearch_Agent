"""Playwright-based browser scraper for sites that block simple HTTP requests.

Uses headless Chromium to bypass bot detection (Cloudflare, Madgex, etc).
Falls back gracefully if Playwright is not installed.

Supports both sync and async operation:
- ``fetch_page()`` / ``close_browser()`` — sync API (dispatches to worker thread)
- ``async_fetch_page()`` / ``async_close_browser()`` — native async API

Browser instances are pooled and reused across requests. A page pool
avoids the overhead of creating a new browser context per request.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Optional

logger = logging.getLogger(__name__)

# ── Sync API (worker-thread based) ──────────────────────────────────────

# Single-threaded executor ensures all sync Playwright calls happen on one thread.
_EXECUTOR = ThreadPoolExecutor(max_workers=1)

# These globals are only accessed from the worker thread.
_BROWSER = None
_CONTEXT = None
_PW = None


def _init_browser():
    """Launch browser (called only in worker thread).

    Uses ``playwright-stealth`` to reduce bot-detection fingerprints and
    ``channel="chrome"`` to prefer the locally-installed Chrome binary
    (harder for sites like Glassdoor to detect than bundled Chromium).
    """
    global _BROWSER, _CONTEXT, _PW
    if _BROWSER is None:
        from playwright.sync_api import sync_playwright

        _PW = sync_playwright().start()

        # Prefer real Chrome (harder to detect) with fallback to Chromium
        try:
            _BROWSER = _PW.chromium.launch(headless=True, channel="chrome")
        except Exception:
            logger.debug("Real Chrome not found, falling back to bundled Chromium")
            _BROWSER = _PW.chromium.launch(headless=True)

        _CONTEXT = _BROWSER.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )

    return _CONTEXT


_STEALTH = None


def _get_stealth():
    """Get a singleton Stealth instance."""
    global _STEALTH
    if _STEALTH is None:
        try:
            from playwright_stealth import Stealth
            _STEALTH = Stealth(
                navigator_platform_override="MacIntel",
                navigator_vendor_override="Google Inc.",
            )
        except ImportError:
            _STEALTH = False  # sentinel: not installed
        except Exception:
            logger.debug("Stealth init failed", exc_info=True)
            _STEALTH = False
    return _STEALTH if _STEALTH is not False else None


def _apply_stealth_sync(page):
    """Apply playwright-stealth patches to a page."""
    s = _get_stealth()
    if s:
        try:
            s.apply_stealth_sync(page)
        except Exception:
            logger.debug("apply_stealth_sync failed", exc_info=True)


def _do_fetch(url: str, wait_selector: str, wait_ms: int, timeout: int) -> Optional[str]:
    """Fetch a page (runs in worker thread)."""
    try:
        ctx = _init_browser()
        page = ctx.new_page()
        _apply_stealth_sync(page)
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
    wait_ms: int = 1500,
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
    """Close the shared sync browser instance."""
    future: Future = _EXECUTOR.submit(_do_close)
    future.result(timeout=10)


# ── Async API (Playwright async, with page pooling) ────────────────────

_ASYNC_PW = None
_ASYNC_BROWSER = None
_ASYNC_CONTEXT = None
_ASYNC_LOCK: asyncio.Lock | None = None
_ASYNC_PAGE_POOL: asyncio.Queue | None = None
_ASYNC_PAGE_POOL_SIZE = 3


async def _async_init_browser():
    """Launch async browser with a pooled context (called once)."""
    global _ASYNC_PW, _ASYNC_BROWSER, _ASYNC_CONTEXT, _ASYNC_LOCK, _ASYNC_PAGE_POOL

    if _ASYNC_LOCK is None:
        _ASYNC_LOCK = asyncio.Lock()

    async with _ASYNC_LOCK:
        if _ASYNC_BROWSER is not None:
            return _ASYNC_CONTEXT

        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning("Playwright not installed; async browser unavailable")
            return None

        _ASYNC_PW = await async_playwright().start()

        # Prefer real Chrome with fallback to Chromium
        try:
            _ASYNC_BROWSER = await _ASYNC_PW.chromium.launch(
                headless=True, channel="chrome",
            )
        except Exception:
            _ASYNC_BROWSER = await _ASYNC_PW.chromium.launch(headless=True)

        _ASYNC_CONTEXT = await _ASYNC_BROWSER.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )

        # Pre-create a page pool for reuse
        _ASYNC_PAGE_POOL = asyncio.Queue(maxsize=_ASYNC_PAGE_POOL_SIZE)

        return _ASYNC_CONTEXT


async def _get_page():
    """Get a page from the pool or create a new one."""
    global _ASYNC_PAGE_POOL
    ctx = await _async_init_browser()
    if ctx is None:
        return None, None

    if _ASYNC_PAGE_POOL is not None and not _ASYNC_PAGE_POOL.empty():
        try:
            page = _ASYNC_PAGE_POOL.get_nowait()
            if not page.is_closed():
                return page, ctx
        except asyncio.QueueEmpty:
            pass

    return await ctx.new_page(), ctx


async def _return_page(page):
    """Return a page to the pool or close it if pool is full."""
    global _ASYNC_PAGE_POOL
    if page is None or page.is_closed():
        return

    # Clear page state for reuse
    try:
        await page.goto("about:blank", wait_until="commit", timeout=3000)
    except Exception:
        try:
            await page.close()
        except Exception:
            pass
        return

    if _ASYNC_PAGE_POOL is not None:
        try:
            _ASYNC_PAGE_POOL.put_nowait(page)
        except asyncio.QueueFull:
            try:
                await page.close()
            except Exception:
                pass


async def async_fetch_page(
    url: str,
    wait_selector: str | None = None,
    wait_ms: int = 1500,
    timeout: int = 20000,
) -> str | None:
    """Fetch a page using async headless Chromium with page pooling.

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
    page, ctx = await _get_page()
    if page is None:
        # Fallback: try sync version in executor
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, fetch_page, url, wait_selector, wait_ms, timeout,
        )

    try:
        # Apply stealth patches per page
        s = _get_stealth()
        if s:
            try:
                await s.apply_stealth_async(page)
            except Exception:
                pass

        await page.goto(url, timeout=timeout, wait_until="domcontentloaded")
        if wait_selector:
            try:
                await page.wait_for_selector(wait_selector, timeout=8000)
            except Exception:
                pass
        if wait_ms:
            await page.wait_for_timeout(wait_ms)
        content = await page.content()
        await _return_page(page)
        return content
    except Exception:
        logger.exception("Async browser fetch failed: %s", url)
        try:
            await page.close()
        except Exception:
            pass
        return None


async def async_close_browser() -> None:
    """Close the shared async browser instance and drain the page pool."""
    global _ASYNC_BROWSER, _ASYNC_CONTEXT, _ASYNC_PW, _ASYNC_PAGE_POOL

    # Drain page pool
    if _ASYNC_PAGE_POOL is not None:
        while not _ASYNC_PAGE_POOL.empty():
            try:
                page = _ASYNC_PAGE_POOL.get_nowait()
                await page.close()
            except Exception:
                pass
        _ASYNC_PAGE_POOL = None

    if _ASYNC_BROWSER:
        try:
            await _ASYNC_BROWSER.close()
        except Exception:
            pass
        _ASYNC_BROWSER = None
        _ASYNC_CONTEXT = None
    if _ASYNC_PW:
        try:
            await _ASYNC_PW.stop()
        except Exception:
            pass
        _ASYNC_PW = None
