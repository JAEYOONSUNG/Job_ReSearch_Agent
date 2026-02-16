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
        return COUNTRY_TO_REGION.get(country, "Other")

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

    def enrich(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fill in region, tier, and source if missing."""
        job.setdefault("source", self.name)
        country = job.get("country")
        if country and not job.get("region"):
            job["region"] = self.resolve_region(country)
        institute = job.get("institute")
        if institute and not job.get("tier"):
            tier = self.resolve_tier(institute)
            if tier is not None:
                job["tier"] = tier
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
