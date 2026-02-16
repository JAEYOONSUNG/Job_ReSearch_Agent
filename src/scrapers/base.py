"""Abstract base scraper with common HTTP, retry, rate-limit, and dedup logic."""

from __future__ import annotations

import abc
import hashlib
import json
import logging
import random
import time
from datetime import datetime
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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
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
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


class BaseScraper(abc.ABC):
    """Base class every scraper must inherit from.

    Subclasses implement ``scrape()`` which returns raw job dicts.
    ``run()`` orchestrates scrape -> deduplicate -> upsert -> log.
    """

    # Minimum seconds between HTTP requests (override per-scraper)
    rate_limit: float = 1.0

    def __init__(self) -> None:
        self.session = _build_session()
        self._last_request_time: float = 0.0
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

    # ── Mapping helpers ───────────────────────────────────────────────────

    @staticmethod
    def _build_tier_lookup() -> dict[str, int]:
        """Build a lowercase institution-name -> tier mapping."""
        lookup: dict[str, int] = {}
        rankings = load_rankings()
        tiers = rankings.get("tiers", {})
        for tier_str, info in tiers.items():
            tier = int(tier_str)
            for inst in info.get("institutions", []):
                lookup[inst.lower()] = tier
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

    def enrich(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fill in region, tier, source, and parse structured fields."""
        job.setdefault("source", self.name)

        # Try to detect country if missing
        country = job.get("country")
        if not country:
            # Try from title, institute, description
            blob = " ".join(filter(None, [
                job.get("title"),
                job.get("institute"),
                job.get("description", "")[:500],
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
        if desc and not job.get("requirements"):
            from src.matching.job_parser import parse_job_posting
            parsed = parse_job_posting(desc)
            # Only fill fields that aren't already set
            if parsed.get("pi_name") and not job.get("pi_name"):
                job["pi_name"] = parsed["pi_name"]
            if parsed.get("requirements"):
                job["requirements"] = parsed["requirements"]
            if parsed.get("conditions"):
                job["conditions"] = parsed["conditions"]
            if parsed.get("keywords"):
                job["keywords"] = parsed["keywords"]

        return job

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
