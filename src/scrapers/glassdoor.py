"""Glassdoor scraper — Playwright-based postdoc job search.

Glassdoor aggressively blocks bots so we use headless Chromium via the
shared ``browser.py`` module.  Listing pages are parsed for job cards,
then the top results are enriched from their detail pages.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.glassdoor.com"

# Glassdoor job search URL
SEARCH_URL = f"{BASE_URL}/Job/jobs.htm"

# Curated search terms targeting postdoc/research positions
SEARCH_TERMS = [
    "postdoc biology",
    "postdoctoral researcher",
    "postdoc CRISPR",
    "postdoc synthetic biology",
    "postdoc protein engineering",
    "postdoc microbiology",
    "research scientist biology",
]

MAX_PAGES = 2  # Glassdoor bot detection is aggressive; keep page count low


class GlassdoorScraper(BaseScraper):
    """Scrape Glassdoor for postdoc positions using Playwright."""

    rate_limit: float = 4.0  # conservative to avoid captchas

    @property
    def name(self) -> str:
        return "glassdoor"

    # ── Browser helpers ────────────────────────────────────────────────────

    @staticmethod
    def _fetch_with_browser(url: str, wait_sel: str = "li.JobsList_jobListItem__wjTHv, div.jobCard, li[data-test='jobListing']") -> str | None:
        """Fetch a Glassdoor page using the shared Playwright browser."""
        from src.scrapers.browser import fetch_page
        return fetch_page(url, wait_selector=wait_sel, wait_ms=5000)

    # ── Listing page parsing ──────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a Glassdoor search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Glassdoor uses list items for job cards (CSS classes change frequently)
        cards = soup.select(
            "li.JobsList_jobListItem__wjTHv, "
            "li[data-test='jobListing'], "
            "li.react-job-listing, "
            "div.jobCard, "
            "li[class*='jobListItem']"
        )

        if not cards:
            # Fallback: extract any links pointing to job detail pages
            for link in soup.select("a[href*='/job-listing/'], a[href*='/partner/jobListing']"):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and len(title) > 5 and href:
                    jobs.append({
                        "title": title,
                        "url": urljoin(BASE_URL, href),
                        "source": self.name,
                    })
            return jobs

        for card in cards:
            job = self._parse_card(card)
            if job:
                jobs.append(job)

        return jobs

    def _parse_card(self, card: Tag) -> dict[str, Any] | None:
        """Extract job info from a Glassdoor job card."""
        # Title + link
        link = card.select_one(
            "a[data-test='job-title'], "
            "a.jobTitle, "
            "a[href*='/job-listing/'], "
            "a[href*='/partner/jobListing'], "
            "a[class*='JobCard_jobTitle']"
        )
        if not link:
            return None

        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Company / Institute
        company_el = card.select_one(
            "span[class*='EmployerProfile_compactEmployerName'], "
            "div.employer-name, "
            "span.companyName, "
            "a[data-test='employer-name']"
        )
        institute = company_el.get_text(strip=True) if company_el else None

        # Location
        loc_el = card.select_one(
            "span[class*='JobCard_location'], "
            "span.compactLocation, "
            "div.location, "
            "span[data-test='emp-location']"
        )
        location = loc_el.get_text(strip=True) if loc_el else None
        country = self._extract_country(location)

        # Salary (if shown)
        salary_el = card.select_one(
            "span[data-test='detailSalary'], "
            "span.salary-estimate, "
            "div[class*='SalaryEstimate']"
        )
        conditions = None
        if salary_el:
            conditions = f"Salary: {salary_el.get_text(strip=True)}"

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "conditions": conditions,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page for full description."""
        url = job.get("url")
        if not url:
            return job

        html = self._fetch_with_browser(
            url,
            wait_sel="div.jobDescriptionContent, div[class*='JobDetails'], div[class*='description']",
        )
        if not html:
            return job

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Description
            found_desc = False
            for sel in (
                "div.jobDescriptionContent",
                "div[class*='JobDetails_jobDescription']",
                "div[class*='description']",
                "div#JobDescriptionContainer",
                "article",
            ):
                desc_el = soup.select_one(sel)
                if desc_el:
                    text = desc_el.get_text(separator="\n", strip=True)
                    if len(text) > 50:
                        job["description"] = text[:3000]
                        found_desc = True
                        break

            if not found_desc:
                fallback = self._extract_description_fallback(html)
                if fallback:
                    job["description"] = fallback

            # Company if missing
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "div[data-test='employerName'], "
                    "span.employer-name"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Location if missing
            if not job.get("country"):
                loc_el = soup.select_one(
                    "div[data-test='location'], "
                    "span.location"
                )
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

        except Exception:
            self.logger.debug("Could not parse detail for %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for term in SEARCH_TERMS:
            self.logger.info("Glassdoor search: %s", term)

            for page in range(MAX_PAGES):
                params = {
                    "sc.keyword": term,
                    "fromAge": "7",  # last 7 days
                }
                if page > 0:
                    params["p"] = str(page + 1)

                search_url = f"{SEARCH_URL}?{urlencode(params)}"
                html = self._fetch_with_browser(search_url)
                if not html:
                    self.logger.warning(
                        "Browser fetch failed for '%s' page %d", term, page + 1
                    )
                    break

                page_jobs = self._parse_listing_page(html)
                if not page_jobs:
                    self.logger.info(
                        "No results for '%s' page %d", term, page + 1
                    )
                    break

                self.logger.info(
                    "'%s' page %d: %d results", term, page + 1, len(page_jobs)
                )
                all_jobs.extend(page_jobs)

                import time
                time.sleep(4.0)  # extra caution with Glassdoor

        # Keyword filter
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')}"
            if self._keyword_match(blob):
                filtered.append(job)

        # Enrich top results with detail pages
        enriched: list[dict[str, Any]] = []
        for job in filtered[:25]:
            job = self._enrich_from_detail(job)
            enriched.append(job)
            import time
            time.sleep(3.0)
        enriched.extend(filtered[25:])

        self.logger.info(
            "Glassdoor: %d total, %d after filter, %d enriched",
            len(all_jobs),
            len(filtered),
            len(enriched),
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        if any(term in lower for term in ("postdoc", "postdoctoral", "post-doc")):
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _extract_country(location: str | None) -> str | None:
        if not location:
            return None
        from src.config import COUNTRY_TO_REGION

        # Check explicit country names
        for country_name in COUNTRY_TO_REGION:
            if country_name.lower() in location.lower():
                return country_name

        # US state abbreviations
        parts = [p.strip() for p in location.split(",")]
        if len(parts) >= 2:
            last = parts[-1].strip().upper()
            us_states = {
                "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
                "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
                "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
                "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
                "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
                "DC",
            }
            if last in us_states or last in ("USA", "US", "UNITED STATES"):
                return "United States"

        return parts[-1].strip() if parts else None
