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

    # Glassdoor CAPTCHA marker strings
    _CAPTCHA_MARKERS = (
        "Help Us Protect Glassdoor",
        "Aidez-nous à protéger Glassdoor",
        "Helfen Sie mit, Glassdoor zu schützen",
        "Help ons Glassdoor te beschermen",
        "Ayúdanos a proteger Glassdoor",
        "verifying that you're a real person",
    )

    @classmethod
    def _is_captcha_page(cls, html: str) -> bool:
        """Return True if the HTML looks like a Glassdoor CAPTCHA page."""
        return any(marker in html for marker in cls._CAPTCHA_MARKERS)

    def _fetch_with_browser(
        self,
        url: str,
        wait_sel: str = (
            "li[data-test='jobListing'], "
            "li.JobsList_jobListItem__wjTHv, "
            "div.jobCard, "
            "li[class*='jobListItem']"
        ),
        retries: int = 2,
    ) -> str | None:
        """Fetch a Glassdoor page using the shared Playwright browser.

        Detects CAPTCHA pages and retries with a longer delay.  If the
        CAPTCHA persists after *retries* attempts, returns ``None``.
        """
        import time
        from src.scrapers.browser import fetch_page

        for attempt in range(1, retries + 1):
            html = fetch_page(url, wait_selector=wait_sel, wait_ms=6000, timeout=30000)
            if html is None:
                return None
            if not self._is_captcha_page(html):
                return html
            self.logger.warning(
                "Glassdoor CAPTCHA detected (attempt %d/%d), waiting...",
                attempt, retries,
            )
            # Exponential backoff: 10s, 20s, ...
            time.sleep(10 * attempt)

        self.logger.warning("Glassdoor CAPTCHA persisted after %d retries", retries)
        return None

    # ── Listing page parsing ──────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a Glassdoor search results page.

        Glassdoor search results are rendered as a two-column SPA: job
        cards on the left (``data-test="jobListing"``), with the first
        card's full description auto-loaded in a right-side detail panel
        (``div[class*='JobDetails_jobDescription']``).
        """
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Glassdoor uses list items for job cards — data-test is most stable
        cards = soup.select(
            "li[data-test='jobListing'], "
            "li.JobsList_jobListItem__wjTHv, "
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

        # The search page's right panel shows the full description for the
        # first (auto-selected) job.  Grab it so at least one job per page
        # gets a full description without a separate detail-page fetch.
        if jobs:
            detail_desc = self._extract_right_panel_description(soup)
            if detail_desc and (not jobs[0].get("description") or
                                len(detail_desc) > len(jobs[0].get("description", ""))):
                jobs[0]["description"] = detail_desc[:15000]

        return jobs

    @staticmethod
    def _extract_right_panel_description(soup: BeautifulSoup) -> str | None:
        """Extract the job description from the search-page right panel."""
        for sel in (
            "div[class*='JobDetails_jobDescription']",
            "div.jobDescriptionContent",
        ):
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    return text
        return None

    def _parse_card(self, card: Tag) -> dict[str, Any] | None:
        """Extract job info from a Glassdoor job card.

        Glassdoor frequently changes CSS class hashes but keeps stable
        ``data-test`` attributes.  We prioritise those for reliability.
        """
        # Title + link — data-test="job-title" is the most stable selector
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
            "a[data-test='employer-name'], "
            "span[class*='EmployerProfile_compactEmployerName'], "
            "span[class*='EmployerProfile_employerName'], "
            "div.employer-name, "
            "span.companyName"
        )
        institute = company_el.get_text(strip=True) if company_el else None

        # Location — data-test="emp-location" is a <div>, not <span>
        loc_el = card.select_one(
            "div[data-test='emp-location'], "
            "span[data-test='emp-location'], "
            "span[class*='JobCard_location'], "
            "span.compactLocation, "
            "div.location"
        )
        location = loc_el.get_text(strip=True) if loc_el else None
        country = self._extract_country(location)

        # Salary (if shown) — data-test="detailSalary" is a <div>
        salary_el = card.select_one(
            "div[data-test='detailSalary'], "
            "span[data-test='detailSalary'], "
            "span.salary-estimate, "
            "div[class*='SalaryEstimate']"
        )
        conditions = None
        if salary_el:
            conditions = f"Salary: {salary_el.get_text(strip=True)}"

        # Description snippet — Glassdoor shows a 2-3 line preview on cards
        description = None
        snippet_el = card.select_one(
            "div[data-test='descSnippet'], "
            "div[class*='JobCard_jobDescriptionSnippet']"
        )
        if snippet_el:
            snippet_text = snippet_el.get_text(separator="\n", strip=True)
            # Remove the trailing HTML entity and "Skills:" line
            snippet_text = re.sub(r"\s*&hellip;.*", "", snippet_text)
            snippet_text = re.sub(r"\nSkills:.*", "", snippet_text, flags=re.DOTALL)
            if len(snippet_text) > 20:
                description = snippet_text.strip()

        # Posting age
        age_el = card.select_one("div[data-test='job-age']")
        posted_age = age_el.get_text(strip=True) if age_el else None

        result: dict[str, Any] = {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "conditions": conditions,
            "source": self.name,
        }
        if description:
            result["description"] = description
        if posted_age:
            result["posted_age"] = posted_age

        return result

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page for full description.

        Glassdoor detail pages (and the right-panel on search results)
        contain the full job description inside a div whose class starts
        with ``JobDetails_jobDescription``.  The description text is present
        in the DOM even when visually truncated by CSS, so ``get_text()``
        retrieves the full content.
        """
        url = job.get("url")
        if not url:
            return job

        html = self._fetch_with_browser(
            url,
            wait_sel=(
                "div[class*='JobDetails_jobDescription'], "
                "div[class*='JobDetails'], "
                "div.jobDescriptionContent, "
                "body"
            ),
            retries=1,  # only 1 retry for detail pages to save time
        )
        if not html:
            return job

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Description — ordered from most-specific to least-specific
            found_desc = False
            for sel in (
                "div[class*='JobDetails_jobDescription']",
                "div.jobDescriptionContent",
                "div#JobDescriptionContainer",
                "article",
            ):
                desc_el = soup.select_one(sel)
                if desc_el:
                    text = desc_el.get_text(separator="\n", strip=True)
                    if len(text) > 50 and not self._is_captcha_page(text):
                        job["description"] = text[:15000]
                        found_desc = True
                        break

            if not found_desc:
                fallback = self._extract_description_fallback(html)
                if fallback and not self._is_captcha_page(fallback):
                    job["description"] = fallback

            # Company if missing
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "div[data-test='employerName'], "
                    "span.employer-name, "
                    "a[data-test='employer-name']"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Location if missing
            if not job.get("country"):
                loc_el = soup.select_one(
                    "div[data-test='location'], "
                    "div[data-test='emp-location'], "
                    "span.location"
                )
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

        except Exception:
            self.logger.debug("Could not parse detail for %s", url, exc_info=True)

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
