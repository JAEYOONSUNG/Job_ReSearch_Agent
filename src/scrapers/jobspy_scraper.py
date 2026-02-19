"""JobSpy wrapper – searches Google Jobs, LinkedIn, Indeed, Glassdoor, ZipRecruiter."""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from src.config import COUNTRY_TO_REGION, SEARCH_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Sites the jobspy library supports
JOBSPY_SITES = ["indeed", "linkedin", "google", "glassdoor", "zip_recruiter"]

# We limit results per query to avoid huge payloads
RESULTS_PER_QUERY = 25

# LinkedIn-specific CSS selectors for job descriptions (ordered by specificity)
_LINKEDIN_DESC_SELECTORS = [
    "div.show-more-less-html__markup",
    "div.description__text",
    "section.description",
    "div[class*='description']",
]

# Regex to extract LinkedIn job ID from URL
_LINKEDIN_JOB_ID_RE = re.compile(r"linkedin\.com/jobs/view/(\d+)")

# LinkedIn guest API endpoint (works without authentication)
_LINKEDIN_GUEST_API = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"


def _safe_str(val: Any) -> str | None:
    """Convert pandas/numpy value to plain str or None."""
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat", "<na>", "n/a", ""):
        return None
    return s


class JobSpyScraper(BaseScraper):
    """Use the ``jobspy`` Python package to aggregate job results from
    multiple job boards in one go.

    ``pip install python-jobspy``
    """

    rate_limit: float = 3.0  # jobspy does its own internal pacing

    @property
    def name(self) -> str:
        return "jobspy"

    @staticmethod
    def _is_linkedin_url(url: str) -> bool:
        """Check if a URL is a LinkedIn job posting."""
        return "linkedin.com/jobs" in url

    @staticmethod
    def _extract_linkedin_job_id(url: str) -> str | None:
        """Extract the numeric job ID from a LinkedIn job URL."""
        m = _LINKEDIN_JOB_ID_RE.search(url)
        return m.group(1) if m else None

    @staticmethod
    def _extract_linkedin_description(html: str) -> str | None:
        """Extract job description from LinkedIn HTML using known selectors.

        Tries LinkedIn-specific selectors first, then falls back to JSON-LD,
        which LinkedIn sometimes embeds in the page.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Try LinkedIn-specific selectors
        for sel in _LINKEDIN_DESC_SELECTORS:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) >= 100:
                    return text[:5000]

        # Try JSON-LD structured data
        for script in soup.select("script[type='application/ld+json']"):
            try:
                ld = json.loads(script.string or "")
                if isinstance(ld, dict) and ld.get("description"):
                    desc = str(ld["description"]).strip()
                    if len(desc) >= 100:
                        return desc[:5000]
            except (json.JSONDecodeError, TypeError):
                pass

        return None

    def _fetch_linkedin_description(self, url: str) -> str | None:
        """Fetch a LinkedIn job description using the guest API or direct URL.

        Strategy:
        1. Try the lightweight guest API endpoint (smaller payload, more reliable)
        2. Fall back to the full job page URL
        Both approaches work without LinkedIn authentication.
        """
        job_id = self._extract_linkedin_job_id(url)

        # Strategy 1: Guest API (lighter payload, ~80KB vs ~330KB)
        if job_id:
            try:
                api_url = _LINKEDIN_GUEST_API.format(job_id=job_id)
                resp = self.fetch(api_url, timeout=15)
                desc = self._extract_linkedin_description(resp.text)
                if desc:
                    return desc
            except Exception:
                self.logger.debug("LinkedIn guest API failed for job %s", job_id)

        # Strategy 2: Direct page URL
        try:
            resp = self.fetch(url, timeout=15)
            desc = self._extract_linkedin_description(resp.text)
            if desc:
                return desc
        except Exception:
            self.logger.debug("LinkedIn direct fetch failed for %s", url)

        return None

    def _fetch_description_browser(self, url: str) -> str | None:
        """Use Playwright to fetch job description when HTTP fails.

        This is the last-resort fallback for pages that require JavaScript
        rendering or block plain HTTP requests.
        """
        try:
            from src.scrapers.browser import fetch_page

            html = fetch_page(
                url,
                wait_selector="div.description__text, div.show-more-less-html__markup, div[class*='description']",
                wait_ms=5000,
                timeout=20000,
            )
            if not html:
                return None

            # For LinkedIn, use the targeted extractor
            if self._is_linkedin_url(url):
                desc = self._extract_linkedin_description(html)
                if desc:
                    return desc

            # Generic fallback
            return self._extract_description_fallback(html)
        except Exception:
            self.logger.debug("Browser fetch failed for %s", url)
            return None

    def _fetch_description(self, url: str) -> str | None:
        """Fetch a job page and extract description when jobspy returned None.

        For LinkedIn URLs, uses a multi-strategy approach:
        1. LinkedIn guest API (lightweight, no auth needed)
        2. Direct HTTP to LinkedIn page
        3. Playwright browser fallback

        For other sites, uses standard HTTP with generic extraction,
        falling back to Playwright if needed.
        """
        if self._is_linkedin_url(url):
            # LinkedIn-specific path with targeted selectors
            desc = self._fetch_linkedin_description(url)
            if desc:
                return desc
            # Playwright fallback for LinkedIn
            desc = self._fetch_description_browser(url)
            if desc:
                return desc
            return None

        # Non-LinkedIn: standard HTTP fetch
        try:
            resp = self.fetch(url, timeout=15)
            desc = self._extract_description_fallback(resp.text)
            if desc:
                return desc
        except Exception:
            self.logger.debug("HTTP fetch failed for %s", url)

        # Playwright fallback for non-LinkedIn
        return self._fetch_description_browser(url)

    def scrape(self) -> list[dict[str, Any]]:
        try:
            from jobspy import scrape_jobs
        except ImportError:
            self.logger.error(
                "python-jobspy is not installed. "
                "Install it with: pip install python-jobspy"
            )
            return []

        all_jobs: list[dict[str, Any]] = []

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("JobSpy search: %s", keyword)
            try:
                df = scrape_jobs(
                    site_name=JOBSPY_SITES,
                    search_term=keyword,
                    results_wanted=RESULTS_PER_QUERY,
                    country_indeed="USA",
                    # Limit to recent postings
                    hours_old=168,  # 7 days
                )

                if df is None or df.empty:
                    self.logger.info("No results for '%s'", keyword)
                    continue

                self.logger.info(
                    "JobSpy returned %d rows for '%s'", len(df), keyword
                )

                for _, row in df.iterrows():
                    job = self._map_row(row)
                    if job:
                        all_jobs.append(job)

            except Exception:
                self.logger.exception(
                    "JobSpy query failed for keyword: %s", keyword
                )

        # Back-fill descriptions for all jobs that have none
        empty_desc = [j for j in all_jobs if not j.get("description")]
        if empty_desc:
            self.logger.info(
                "Back-filling descriptions for %d/%d jobs",
                len(empty_desc),
                len(all_jobs),
            )
            filled = 0
            for i, job in enumerate(empty_desc, 1):
                desc = self._fetch_description(job["url"])
                if desc:
                    job["description"] = desc
                    filled += 1
                if i % 10 == 0:
                    self.logger.info(
                        "Back-fill progress: %d/%d checked, %d filled",
                        i,
                        len(empty_desc),
                        filled,
                    )
            self.logger.info(
                "Back-fill complete: %d/%d descriptions filled",
                filled,
                len(empty_desc),
            )

        self.logger.info("Total JobSpy jobs collected: %d", len(all_jobs))
        return all_jobs

    # ── Row mapping ───────────────────────────────────────────────────────

    def _map_row(self, row: Any) -> dict[str, Any] | None:
        """Map a jobspy DataFrame row to our job schema."""
        title = _safe_str(getattr(row, "title", None))
        url = _safe_str(getattr(row, "job_url", None))
        if not title or not url:
            return None

        company = _safe_str(getattr(row, "company_name", None)) or _safe_str(
            getattr(row, "company", None)
        )
        location = _safe_str(getattr(row, "location", None))
        description = _safe_str(getattr(row, "description", None))
        date_posted = _safe_str(getattr(row, "date_posted", None))
        site = _safe_str(getattr(row, "site", None))

        # Attempt to extract country from location
        country = self._guess_country(location)

        # Extract additional fields from jobspy DataFrame
        salary_min = _safe_str(getattr(row, "min_amount", None))
        salary_max = _safe_str(getattr(row, "max_amount", None))
        # Strip trailing ".0" from salary values (e.g. "35000.0" -> "35000")
        if salary_min and salary_min.endswith(".0"):
            salary_min = salary_min[:-2]
        if salary_max and salary_max.endswith(".0"):
            salary_max = salary_max[:-2]
        interval = _safe_str(getattr(row, "interval", None))
        job_type = _safe_str(getattr(row, "job_type", None))

        conditions_parts = []
        if salary_min and salary_max:
            try:
                smin = f"{int(float(salary_min)):,}"
                smax = f"{int(float(salary_max)):,}"
            except (ValueError, OverflowError):
                smin, smax = salary_min, salary_max
            conditions_parts.append(f"Salary: ${smin}-${smax}")
            if interval:
                conditions_parts[-1] += f"/{interval}"
        elif salary_min:
            try:
                smin = f"{int(float(salary_min)):,}"
            except (ValueError, OverflowError):
                smin = salary_min
            conditions_parts.append(f"Salary: ${smin}+")
        if job_type:
            conditions_parts.append(f"Type: {job_type}")

        return {
            "title": title,
            "institute": company,
            "country": country,
            "description": (description or "")[:5000],
            "url": url,
            "posted_date": date_posted,
            "source": f"jobspy_{site}" if site else "jobspy",
            "field": None,
            "conditions": " | ".join(conditions_parts) if conditions_parts else None,
        }

    @staticmethod
    def _guess_country(location: str | None) -> str | None:
        """Very rough heuristic to extract a country from a location string."""
        if not location:
            return None
        loc_lower = location.lower()

        # US state abbreviations
        us_states = {
            "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
            "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
            "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
            "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
            "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
        }
        parts = [p.strip() for p in location.split(",")]
        if len(parts) >= 2:
            last = parts[-1].strip().lower()
            # If last part is a 2-letter US state abbreviation
            if last in us_states or last == "usa" or last == "united states":
                return "United States"

        # Check for explicit country names
        for country_name in COUNTRY_TO_REGION:
            if country_name.lower() in loc_lower:
                return country_name

        return None
