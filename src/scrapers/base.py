"""Abstract base scraper with common HTTP, retry, rate-limit, and dedup logic.

Supports both synchronous (requests) and asynchronous (aiohttp) HTTP clients.
Scrapers can override ``scrape()`` (sync) and/or ``async_scrape()`` (async).
The pipeline uses ``async_run()`` for concurrent execution via asyncio.
"""

from __future__ import annotations

import abc
import asyncio
import hashlib
import json
import logging
import random
import re
import time
from datetime import datetime
from html import unescape as html_unescape
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import COUNTRY_TO_REGION, RANKINGS_PATH, load_rankings

# Extended country detection: maps common substrings to COUNTRY_TO_REGION keys
_COUNTRY_ALIASES = {
    # US
    "united states": "United States",
    "usa": "United States",
    "u.s.a": "United States",
    "u.s.": "United States",
    # UK
    "united kingdom": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "wales": "United Kingdom",
    # Common EU
    "germany": "Germany",
    "deutschland": "Germany",
    "france": "France",
    "netherlands": "Netherlands",
    "holland": "Netherlands",
    "switzerland": "Switzerland",
    "sweden": "Sweden",
    "denmark": "Denmark",
    "norway": "Norway",
    "finland": "Finland",
    "belgium": "Belgium",
    "austria": "Austria",
    "spain": "Spain",
    "italy": "Italy",
    "ireland": "Ireland",
    "israel": "Israel",
    "czech republic": "EU",
    "czechia": "EU",
    "poland": "EU",
    "portugal": "EU",
    "hungary": "EU",
    "greece": "EU",
    "romania": "EU",
    "croatia": "EU",
    "luxembourg": "EU",
    "estonia": "EU",
    "latvia": "EU",
    "lithuania": "EU",
    "slovenia": "EU",
    "slovakia": "EU",
    "cyprus": "EU",
    "malta": "EU",
    "iceland": "EU",
    # Asia
    "south korea": "South Korea",
    "korea": "South Korea",
    "japan": "Japan",
    "china": "China",
    "singapore": "Singapore",
    "taiwan": "Taiwan",
    "hong kong": "Hong Kong",
    "india": "India",
    # Other
    "canada": "Canada",
    "australia": "Australia",
    "new zealand": "New Zealand",
    "brazil": "Other",
    "mexico": "Other",
}

# US state abbreviations for detecting US locations like "Boston, MA"
_US_STATES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
    "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
    "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
    "dc",
}

# Well-known US cities for heuristic matching
_US_CITIES = {
    "boston", "new york", "san francisco", "los angeles", "chicago",
    "houston", "philadelphia", "san diego", "seattle", "berkeley",
    "stanford", "cambridge", "princeton", "baltimore", "atlanta",
    "pittsburgh", "ann arbor", "madison", "durham", "chapel hill",
    "pasadena", "bethesda", "nih", "mit", "caltech",
}

logger = logging.getLogger(__name__)

# Rotating user-agents to reduce detection
_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
]


def _build_session(
    total_retries: int = 3,
    backoff_factor: float = 1.0,
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a requests.Session with automatic retries."""
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=list(status_forcelist),
        allowed_methods=["GET", "HEAD", "POST"],
        raise_on_status=False,
        respect_retry_after_header=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


# ── Async HTTP helpers ──────────────────────────────────────────────────

_aiohttp_available = False
try:
    import aiohttp
    _aiohttp_available = True
except ImportError:
    pass


class _AsyncSessionManager:
    """Lazy singleton for a shared aiohttp.ClientSession.

    Created once per event loop, reused by all scrapers in the same run.
    """

    _session: aiohttp.ClientSession | None = None
    _lock: asyncio.Lock | None = None

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        async with cls._lock:
            if cls._session is None or cls._session.closed:
                timeout = aiohttp.ClientTimeout(total=60, connect=15)
                connector = aiohttp.TCPConnector(
                    limit=30,
                    limit_per_host=6,
                    enable_cleanup_closed=True,
                )
                cls._session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=connector,
                )
            return cls._session

    @classmethod
    async def close(cls) -> None:
        if cls._session and not cls._session.closed:
            await cls._session.close()
            cls._session = None


_SENTENCE_ENDERS = frozenset(".!?;:)\"'")
_CAPTCHA_MARKERS = {"security check", "captcha", "verification successful",
                    "ray id:", "unusual activity"}


def _description_needs_refresh(desc: str) -> bool:
    """Return True if description looks incomplete and should be re-fetched.

    Checks for:
    - Truncation marker ("...")
    - Mid-sentence cutoff (doesn't end with sentence-ending punctuation)
    - Captcha/bot-block text instead of real content
    - Very short descriptions (<500 chars)
    """
    if not desc:
        return True
    desc = desc.rstrip()
    if len(desc) < 500:
        return True
    if desc.endswith("..."):
        return True
    # Check for captcha/bot-block content
    desc_lower = desc.lower()
    if any(marker in desc_lower for marker in _CAPTCHA_MARKERS):
        return True
    # Check if description ends mid-sentence
    if desc[-1] not in _SENTENCE_ENDERS:
        return True
    return False


class BaseScraper(abc.ABC):
    """Base class every scraper must inherit from.

    Subclasses implement ``scrape()`` which returns raw job dicts.
    ``run()`` orchestrates scrape -> deduplicate -> upsert -> log (sync).
    ``async_run()`` does the same using asyncio for concurrent execution.
    """

    # Minimum seconds between HTTP requests (override per-scraper)
    rate_limit: float = 1.0

    def __init__(self) -> None:
        self.session = _build_session()
        self._last_request_time: float = 0.0
        self._async_last_request_time: float = 0.0
        self.logger = logging.getLogger(f"scraper.{self.name}")
        # Pre-load institution tier lookup
        self._tier_lookup: dict[str, int] = self._build_tier_lookup()

    # ── Abstract interface ────────────────────────────────────────────────

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier for this scraper (used in logs and DB)."""
        ...

    @abc.abstractmethod
    def scrape(self) -> list[dict[str, Any]]:
        """Fetch and return a list of raw job dicts.

        Each dict should contain as many of these keys as possible:
        title, pi_name, institute, department, country, region, tier,
        field, description, url, lab_url, scholar_url, posted_date,
        deadline, source, h_index, citations
        """
        ...

    # ── HTTP helpers ──────────────────────────────────────────────────────

    def _throttle(self) -> None:
        """Sleep to honour ``self.rate_limit``."""
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)

    def fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json_body: dict | None = None,
        headers: dict | None = None,
        timeout: int = 30,
    ) -> requests.Response:
        """Rate-limited HTTP request with rotating user-agent."""
        self._throttle()
        hdrs = {
            "User-Agent": _random_ua(),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if headers:
            hdrs.update(headers)

        self.logger.debug("%s %s", method, url)
        resp = self.session.request(
            method,
            url,
            params=params,
            data=data,
            json=json_body,
            headers=hdrs,
            timeout=timeout,
        )
        self._last_request_time = time.monotonic()
        resp.raise_for_status()
        return resp

    # ── Async HTTP helpers ───────────────────────────────────────────────

    async def _async_throttle(self) -> None:
        """Async-compatible rate limiter using ``asyncio.sleep``."""
        elapsed = time.monotonic() - self._async_last_request_time
        if elapsed < self.rate_limit:
            await asyncio.sleep(self.rate_limit - elapsed)

    async def async_fetch(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        data: dict | None = None,
        json_body: dict | None = None,
        headers: dict | None = None,
        timeout: int = 30,
        max_retries: int = 3,
    ) -> aiohttp.ClientResponse:
        """Async rate-limited HTTP request with rotating user-agent and retries.

        Returns an aiohttp.ClientResponse whose body has already been read
        (``response.text`` / ``response.json()`` are cached via ``_body``).
        """
        if not _aiohttp_available:
            raise RuntimeError("aiohttp is not installed; run: pip install aiohttp")

        await self._async_throttle()
        hdrs = {
            "User-Agent": _random_ua(),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        if headers:
            hdrs.update(headers)

        self.logger.debug("async %s %s", method, url)
        session = await _AsyncSessionManager.get_session()

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                resp = await session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json_body,
                    headers=hdrs,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                )
                self._async_last_request_time = time.monotonic()

                # Read body eagerly so callers can access .text / .json later
                await resp.read()

                if resp.status in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
                    backoff = (2 ** attempt) + random.random()
                    self.logger.debug(
                        "Retry %d/%d for %s (status %d), waiting %.1fs",
                        attempt + 1, max_retries, url, resp.status, backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue

                resp.raise_for_status()
                return resp

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                if attempt < max_retries - 1:
                    backoff = (2 ** attempt) + random.random()
                    self.logger.debug(
                        "Retry %d/%d for %s (%s), waiting %.1fs",
                        attempt + 1, max_retries, url, exc, backoff,
                    )
                    await asyncio.sleep(backoff)

        raise last_exc or RuntimeError(f"All {max_retries} retries failed for {url}")

    async def async_fetch_text(
        self,
        url: str,
        **kwargs: Any,
    ) -> str:
        """Convenience: fetch URL and return response body as text."""
        resp = await self.async_fetch(url, **kwargs)
        return await resp.text()

    # ── Async parallel enrichment ────────────────────────────────────────

    async def _async_parallel_enrich(
        self,
        jobs: list[dict[str, Any]],
        enrich_fn,
        concurrency: int = 4,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Async version of ``_parallel_enrich`` using a semaphore for concurrency.

        *enrich_fn* can be either a sync or async callable.
        Sync callables are run in the default executor.
        """
        to_enrich = jobs[:limit] if limit else jobs
        passthrough = jobs[limit:] if limit else []

        # Skip jobs whose URL is already in DB
        need_enrich: list[dict[str, Any]] = []
        already_done: list[dict[str, Any]] = []
        try:
            from src.db import get_connection
            with get_connection() as conn:
                for job in to_enrich:
                    url = job.get("url")
                    if url:
                        row = conn.execute(
                            "SELECT description FROM jobs WHERE url = ?", (url,)
                        ).fetchone()
                        if row and row["description"] and len(row["description"]) > 100:
                            if _description_needs_refresh(row["description"]):
                                need_enrich.append(job)
                                continue
                            already_done.append(job)
                            continue
                    need_enrich.append(job)
        except Exception:
            need_enrich = list(to_enrich)
            already_done = []

        self.logger.info(
            "Async enriching %d jobs (%d skipped, already in DB)",
            len(need_enrich), len(already_done),
        )

        if not need_enrich:
            return already_done + passthrough

        sem = asyncio.Semaphore(concurrency)
        is_coro_fn = asyncio.iscoroutinefunction(enrich_fn)

        async def _enrich_one(job: dict[str, Any]) -> dict[str, Any]:
            async with sem:
                try:
                    if is_coro_fn:
                        return await enrich_fn(job)
                    else:
                        loop = asyncio.get_running_loop()
                        return await loop.run_in_executor(None, enrich_fn, job)
                except Exception:
                    return job

        enriched = await asyncio.gather(*[_enrich_one(j) for j in need_enrich])
        return list(enriched) + already_done + passthrough

    # ── Mapping helpers ───────────────────────────────────────────────────

    @staticmethod
    def _build_tier_lookup() -> dict[str, int]:
        """Build a lowercase institution-name -> tier mapping.

        Includes tiers 1-4, top_companies (→ 2), and companies (→ 3).
        Also includes aliases from tier_lookup_aliases.
        """
        lookup: dict[str, int] = {}
        rankings = load_rankings()
        tiers = rankings.get("tiers", {})
        for tier_str, info in tiers.items():
            try:
                tier = int(tier_str)
            except (ValueError, TypeError):
                continue
            for inst in info.get("institutions", []):
                lookup[inst.lower()] = tier

        # Companies (nested dict with "institutions" key)
        companies = rankings.get("companies", {})
        top_co = companies.get("top_companies", {})
        top_list = top_co.get("institutions", []) if isinstance(top_co, dict) else top_co
        for inst in top_list:
            lookup[inst.lower()] = top_co.get("tier_equivalent", 2) if isinstance(top_co, dict) else 2
        std_co = companies.get("companies", {})
        std_list = std_co.get("institutions", []) if isinstance(std_co, dict) else std_co
        for inst in std_list:
            lookup[inst.lower()] = std_co.get("tier_equivalent", 3) if isinstance(std_co, dict) else 3

        # Aliases
        aliases = rankings.get("tier_lookup_aliases", {})
        for alias, canonical in aliases.items():
            canonical_lower = canonical.lower()
            if canonical_lower in lookup and alias.lower() not in lookup:
                lookup[alias.lower()] = lookup[canonical_lower]

        return lookup

    def resolve_region(self, country: str | None) -> str:
        """Map a country string to a region."""
        if not country:
            return "Other"
        # Exact match first
        if country in COUNTRY_TO_REGION:
            return COUNTRY_TO_REGION[country]
        # Case-insensitive match
        lower = country.strip().lower()
        for key, region in COUNTRY_TO_REGION.items():
            if key.lower() == lower:
                return region
        # Check aliases
        for alias, canonical in _COUNTRY_ALIASES.items():
            if alias in lower or lower in alias:
                # canonical might be a region directly (e.g. "EU") or a country name
                if canonical in COUNTRY_TO_REGION:
                    return COUNTRY_TO_REGION[canonical]
                return canonical  # "EU", "Other" etc.
        return "Other"

    def resolve_tier(self, institute: str | None) -> int | None:
        """Map an institution name to a ranking tier (1/2/3) or None."""
        if not institute:
            return None
        key = institute.strip().lower()
        if key in self._tier_lookup:
            return self._tier_lookup[key]
        # Partial match: check if any known institution name is contained
        for name, tier in self._tier_lookup.items():
            if name in key or key in name:
                return tier
        return None

    @staticmethod
    def guess_country(text: str | None) -> str | None:
        """Try to detect a country from free-form text (title, description, institute)."""
        if not text:
            return None
        lower = text.lower()

        # Check US state abbreviations in patterns like "City, MA" or "City, MA, USA"
        import re
        state_pattern = re.findall(r",\s*([a-z]{2})\b", lower)
        for abbr in state_pattern:
            if abbr in _US_STATES:
                return "United States"

        # Check US cities
        for city in _US_CITIES:
            if city in lower:
                return "United States"

        # Check country aliases (longest match first to avoid false positives)
        for alias in sorted(_COUNTRY_ALIASES, key=len, reverse=True):
            if alias in lower:
                canonical = _COUNTRY_ALIASES[alias]
                if canonical in COUNTRY_TO_REGION:
                    return canonical
                # For direct region values like "EU", return a placeholder country
                return canonical

        return None

    @staticmethod
    def _extract_description_fallback(html: str, min_length: int = 200) -> str | None:
        """Extract the largest meaningful text block from a page.

        Used when scraper-specific CSS selectors fail to find a description.
        Looks for the ``<main>``, ``<article>``, or largest ``<div>`` with
        substantial text content.
        """
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Remove noise elements
        for tag in soup.select("nav, header, footer, script, style, aside, "
                               "form, noscript, iframe"):
            tag.decompose()

        # Priority containers
        for sel in ("main", "article", "[role='main']"):
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) >= min_length:
                    return text[:15000]

        # Fallback: find the div with the most text
        best_text = ""
        for div in soup.find_all("div"):
            text = div.get_text(separator="\n", strip=True)
            if len(text) > len(best_text):
                best_text = text

        if len(best_text) >= min_length:
            return best_text[:15000]

        return None

    @staticmethod
    def _clean_title(title: str) -> str:
        """Decode HTML entities and remove garbage from a job title."""
        if not title:
            return title
        # Decode HTML entities: &#39; -> ', &amp; -> &, etc.
        title = html_unescape(title)
        # Remove stray >>> or <<< (LinkedIn artefact)
        title = re.sub(r"\s*>{2,}\s*", " ", title)
        title = re.sub(r"\s*<{2,}\s*", " ", title)
        # Remove "Empty heading" artefact
        title = re.sub(r"\s*Empty heading\s*", " ", title, flags=re.IGNORECASE)
        # Collapse whitespace
        title = re.sub(r"\s{2,}", " ", title).strip()
        return title

    def enrich(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fill in region, tier, source, and parse structured fields."""
        job.setdefault("source", self.name)

        # Clean title
        if job.get("title"):
            job["title"] = self._clean_title(job["title"])

        # Try to detect country if missing
        country = job.get("country")
        if not country:
            # Try from title, institute, description
            blob = " ".join(filter(None, [
                job.get("title"),
                job.get("institute"),
                (job.get("description") or "")[:500],
                job.get("field"),
            ]))
            country = self.guess_country(blob)
            if country:
                job["country"] = country

        # Set region
        if country and not job.get("region"):
            job["region"] = self.resolve_region(country)

        # Set tier from institute name
        institute = job.get("institute")
        if institute and not job.get("tier"):
            tier = self.resolve_tier(institute)
            if tier is not None:
                job["tier"] = tier

        # Parse structured fields from description
        desc = job.get("description", "")

        # Extract PI name from title Lab patterns (e.g. "Badran Lab")
        if not job.get("pi_name") and job.get("title"):
            from src.matching.job_parser import extract_pi_from_title
            pi_from_title = extract_pi_from_title(job["title"])
            if pi_from_title:
                job["pi_name"] = pi_from_title

        # Expand single last name from title to full name using description
        if job.get("pi_name") and " " not in job["pi_name"] and desc:
            from src.matching.job_parser import expand_pi_last_name
            full_name = expand_pi_last_name(job["pi_name"], desc)
            if full_name:
                job["pi_name"] = full_name

        if desc:
            from src.matching.job_parser import parse_job_posting, extract_pi_name, extract_deadline, expand_pi_last_name
            # Always try PI name extraction from description
            if not job.get("pi_name"):
                pi = extract_pi_name(desc)
                if pi:
                    job["pi_name"] = pi

            # Expand single last name from description extraction too
            if job.get("pi_name") and " " not in job["pi_name"]:
                full_name = expand_pi_last_name(job["pi_name"], desc)
                if full_name:
                    job["pi_name"] = full_name
            # Extract deadline if not already set
            if not job.get("deadline"):
                deadline = extract_deadline(desc)
                if deadline:
                    job["deadline"] = deadline
            # Parse requirements/conditions/keywords if not already set
            if not job.get("requirements"):
                parsed = parse_job_posting(desc)
                if parsed.get("requirements"):
                    job["requirements"] = parsed["requirements"]
                if parsed.get("conditions") and not job.get("conditions"):
                    job["conditions"] = parsed["conditions"]
                if parsed.get("keywords") and not job.get("keywords"):
                    job["keywords"] = parsed["keywords"]

        # Infer research field if still empty
        if not job.get("field"):
            from src.matching.job_parser import infer_field
            blob = " ".join(filter(None, [
                job.get("title"),
                desc,
                job.get("keywords"),
            ]))
            field = infer_field(blob)
            if field:
                job["field"] = field

        # NOTE: PI URL lookup (Scholar, lab, dept) is handled in a batch
        # post-processing step in pipeline.py for performance reasons.

        return job

    # ── Parallel enrichment ─────────────────────────────────────────────

    def _parallel_enrich(
        self,
        jobs: list[dict[str, Any]],
        enrich_fn,
        max_workers: int = 4,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Enrich jobs by calling *enrich_fn* concurrently.

        Parameters
        ----------
        jobs : list
            Jobs to enrich (first *limit* are enriched, rest passed through).
        enrich_fn : callable
            Function taking a job dict and returning the enriched dict.
        max_workers : int
            Thread pool size.
        limit : int or None
            How many jobs to enrich (None = all).
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        to_enrich = jobs[:limit] if limit else jobs
        passthrough = jobs[limit:] if limit else []

        # Skip jobs whose URL is already in DB (already enriched on a prior run)
        need_enrich: list[dict[str, Any]] = []
        already_done: list[dict[str, Any]] = []
        try:
            from src.db import get_connection
            with get_connection() as conn:
                for job in to_enrich:
                    url = job.get("url")
                    if url:
                        row = conn.execute(
                            "SELECT description FROM jobs WHERE url = ?", (url,)
                        ).fetchone()
                        if row and row["description"] and len(row["description"]) > 100:
                            if _description_needs_refresh(row["description"]):
                                need_enrich.append(job)
                                continue
                            already_done.append(job)
                            continue
                    need_enrich.append(job)
        except Exception:
            need_enrich = list(to_enrich)
            already_done = []

        self.logger.info(
            "Enriching %d jobs (%d skipped, already in DB)",
            len(need_enrich), len(already_done),
        )

        if not need_enrich:
            return already_done + passthrough

        enriched: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(enrich_fn, j): j for j in need_enrich}
            for future in as_completed(futures):
                try:
                    enriched.append(future.result())
                except Exception:
                    enriched.append(futures[future])

        return enriched + already_done + passthrough

    # ── Dedup ─────────────────────────────────────────────────────────────

    @staticmethod
    def _job_fingerprint(job: dict[str, Any]) -> str:
        """Deterministic hash for duplicate detection within a single run."""
        raw = f"{job.get('url', '')}|{job.get('title', '')}|{job.get('institute', '')}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def _deduplicate(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        seen: set[str] = set()
        unique: list[dict[str, Any]] = []
        for j in jobs:
            fp = self._job_fingerprint(j)
            if fp not in seen:
                seen.add(fp)
                unique.append(j)
        return unique

    # ── Orchestrator ──────────────────────────────────────────────────────

    def run(self) -> list[dict[str, Any]]:
        """Full scrape cycle: fetch -> enrich -> dedup -> upsert -> log."""
        # Import here to avoid circular imports at module-load time
        from src.db import log_scrape, upsert_job

        self.logger.info("Starting scraper: %s", self.name)
        start = datetime.utcnow()
        try:
            raw_jobs = self.scrape()
            self.logger.info("Raw results: %d", len(raw_jobs))

            enriched = [self.enrich(j) for j in raw_jobs]
            unique = self._deduplicate(enriched)
            self.logger.info("After dedup: %d", len(unique))

            new_count = 0
            for job in unique:
                try:
                    _, is_new = upsert_job(job)
                    if is_new:
                        new_count += 1
                except Exception:
                    self.logger.exception("Failed to upsert job: %s", job.get("url"))

            self.logger.info(
                "Scraper %s finished: %d found, %d new",
                self.name,
                len(unique),
                new_count,
            )
            log_scrape(
                source=self.name,
                status="success",
                jobs_found=len(unique),
                new_jobs=new_count,
            )
            return unique

        except Exception as exc:
            self.logger.exception("Scraper %s failed", self.name)
            log_scrape(source=self.name, status="error", error=str(exc))
            return []

    async def async_run(self) -> list[dict[str, Any]]:
        """Async full scrape cycle: fetch -> enrich -> dedup -> upsert -> log.

        Falls back to running the sync ``scrape()`` in a thread executor
        if the scraper does not override ``async_scrape()``.
        """
        from src.db import log_scrape, upsert_job

        self.logger.info("Starting async scraper: %s", self.name)
        try:
            # Use async_scrape if overridden, else run sync scrape in executor
            if self._has_async_scrape():
                raw_jobs = await self.async_scrape()
            else:
                loop = asyncio.get_running_loop()
                raw_jobs = await loop.run_in_executor(None, self.scrape)
            self.logger.info("Raw results: %d", len(raw_jobs))

            enriched = [self.enrich(j) for j in raw_jobs]
            unique = self._deduplicate(enriched)
            self.logger.info("After dedup: %d", len(unique))

            new_count = 0
            for job in unique:
                try:
                    _, is_new = upsert_job(job)
                    if is_new:
                        new_count += 1
                except Exception:
                    self.logger.exception("Failed to upsert job: %s", job.get("url"))

            self.logger.info(
                "Scraper %s finished: %d found, %d new",
                self.name,
                len(unique),
                new_count,
            )
            log_scrape(
                source=self.name,
                status="success",
                jobs_found=len(unique),
                new_jobs=new_count,
            )
            return unique

        except Exception as exc:
            self.logger.exception("Scraper %s failed", self.name)
            log_scrape(source=self.name, status="error", error=str(exc))
            return []

    async def async_scrape(self) -> list[dict[str, Any]]:
        """Async version of ``scrape()``.

        Override in subclasses that benefit from async HTTP (aiohttp).
        Default implementation runs the sync ``scrape()`` in a thread executor.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.scrape)

    def _has_async_scrape(self) -> bool:
        """Check if this scraper has a custom async_scrape implementation."""
        return type(self).async_scrape is not BaseScraper.async_scrape
