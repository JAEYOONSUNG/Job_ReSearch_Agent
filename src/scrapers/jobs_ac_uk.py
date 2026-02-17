"""jobs.ac.uk scraper — UK/global academic postdoc positions (Playwright-based)."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jobs.ac.uk"

SEARCH_KEYWORDS = [
    "postdoc synthetic biology",
    "postdoctoral CRISPR",
    "postdoc protein engineering",
    "postdoc microbiology",
    "postdoctoral metabolic engineering",
    "postdoc genome engineering",
    "postdoctoral directed evolution",
    "postdoc systems biology",
    "postdoc biotechnology",
]

MAX_PAGES = 3


class JobsAcUkScraper(BaseScraper):
    """Scrape jobs.ac.uk for postdoc positions using Playwright (bypasses Cloudflare)."""

    rate_limit: float = 3.0

    @property
    def name(self) -> str:
        return "jobs_ac_uk"

    def _fetch_with_browser(self, url: str) -> str | None:
        """Fetch URL using Playwright headless browser."""
        from src.scrapers.browser import fetch_page
        return fetch_page(url, wait_selector="div.j-search-result, div.search-results, article", wait_ms=3000)

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a jobs.ac.uk search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Primary selector: the actual result card wrapper
        cards = soup.select("div.j-search-result__result")

        if not cards:
            # Fallback: find links to job detail pages
            for link in soup.select("a[href*='/job/']"):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and len(title) > 10 and href and re.search(r"/job/\w+", href):
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
        """Extract job info from a search result card."""
        link = card.select_one("a[href*='/job/']")
        if not link:
            return None

        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Employer / Institute
        inst_el = card.select_one("div.j-search-result__employer")
        institute = inst_el.get_text(strip=True) if inst_el else None

        # Department
        dept_el = card.select_one("div.j-search-result__department")
        department = dept_el.get_text(strip=True) if dept_el else None

        # Location — plain <div> containing "Location:" text
        location = None
        for div in card.select("div"):
            text = div.get_text(strip=True)
            if text.startswith("Location:"):
                location = text.replace("Location:", "").strip()
                break
        country = self._extract_country(location)

        # Salary — inside j-search-result__info
        salary = None
        info_el = card.select_one("div.j-search-result__info")
        if info_el:
            raw = info_el.get_text(strip=True)
            salary = re.sub(r"\s+", " ", re.sub(r"^Salary:\s*", "", raw)).strip()

        # Closing date
        close_el = card.select_one("span.j-search-result__date--blue")
        deadline = None
        if close_el:
            raw = close_el.get_text(strip=True)
            deadline = self._parse_date(raw)

        return {
            "title": title,
            "institute": institute,
            "department": department,
            "country": country,
            "url": url,
            "deadline": deadline,
            "conditions": f"Salary: {salary}" if salary else None,
            "source": self.name,
        }

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch detail page for full description."""
        url = job.get("url")
        if not url:
            return job

        html = self._fetch_with_browser(url)
        if not html:
            return job

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Full description — div#job-description
            found_desc = False
            for sel in ("div#job-description", "div.job-description",
                        "div[class*='description']", "article"):
                desc_el = soup.select_one(sel)
                if desc_el:
                    text = desc_el.get_text(separator="\n", strip=True)
                    if len(text) > 100:
                        job["description"] = text[:3000]
                        found_desc = True
                        break

            if not found_desc and html:
                fallback = self._extract_description_fallback(html)
                if fallback:
                    job["description"] = fallback

            # Parse detail columns for structured info
            first_col = soup.select_one("div.j-advert-details__first-col")
            if first_col:
                col_text = first_col.get_text(separator=" | ", strip=True)
                # Location
                loc_match = re.search(r"Location:\s*\|\s*(.+?)(?:\s*\||\s*$)", col_text)
                if loc_match and not job.get("country"):
                    location = loc_match.group(1).strip()
                    job["country"] = self._extract_country(location)
                # Salary
                sal_match = re.search(r"Salary:\s*\|\s*(.+?)(?:\s*\||\s*$)", col_text)
                if sal_match and not job.get("conditions"):
                    salary = re.sub(r"\s+", " ", sal_match.group(1).strip())
                    job["conditions"] = f"Salary: {salary}"
                # Contract type
                ct_match = re.search(r"Contract Type:\s*\|\s*(.+?)(?:\s*\||\s*$)", col_text)
                if ct_match:
                    contract = ct_match.group(1).strip()
                    cond = job.get("conditions", "")
                    job["conditions"] = f"{cond}; {contract}" if cond else contract

            second_col = soup.select_one("div.j-advert-details__second-col")
            if second_col:
                col_text = second_col.get_text(separator=" | ", strip=True)
                # Deadline
                close_match = re.search(r"Closes:\s*\|\s*(.+?)(?:\s*\||\s*$)", col_text)
                if close_match and not job.get("deadline"):
                    job["deadline"] = self._parse_date(close_match.group(1).strip())

        except Exception:
            self.logger.debug("Could not enrich detail: %s", url)

        return job

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("jobs.ac.uk search: %s", keyword)

            for page in range(MAX_PAGES):
                params = {
                    "keywords": keyword,
                    "activeFacet": "typeOfJobFacet",
                    "typeOfJobId": "2",  # Research/Academic
                    "pageNo": str(page + 1),
                }
                search_url = f"{BASE_URL}/search/?{urlencode(params)}"

                html = self._fetch_with_browser(search_url)
                if not html:
                    self.logger.warning("Browser fetch failed for '%s' page %d", keyword, page + 1)
                    break

                page_jobs = self._parse_listing_page(html)
                if not page_jobs:
                    self.logger.info("No results for '%s' page %d", keyword, page + 1)
                    break

                self.logger.info("'%s' page %d: %d results", keyword, page + 1, len(page_jobs))
                all_jobs.extend(page_jobs)

                # Rate limit between pages
                import time
                time.sleep(2.0)

        # Keyword filter
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')}"
            if self._keyword_match(blob):
                filtered.append(job)

        # Enrich top results with detail pages
        enriched: list[dict[str, Any]] = []
        for job in filtered[:30]:
            job = self._enrich_from_detail(job)
            enriched.append(job)
            import time
            time.sleep(2.0)
        enriched.extend(filtered[30:])

        self.logger.info("jobs.ac.uk: %d total, %d after filter, %d enriched", len(all_jobs), len(filtered), len(enriched))
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        if "postdoc" in lower or "postdoctoral" in lower:
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _extract_country(location: str | None) -> str | None:
        if not location:
            return None
        lower = location.lower()
        # UK-specific
        if any(x in lower for x in ("united kingdom", "uk", "england", "scotland", "wales", "london", "oxford", "cambridge", "edinburgh", "manchester", "birmingham", "bristol", "leeds", "sheffield", "nottingham", "liverpool", "glasgow", "belfast")):
            return "United Kingdom"
        parts = [p.strip() for p in location.split(",")]
        return parts[-1] if parts else None

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        # Remove ordinal suffixes: "2nd" → "2", "3rd" → "3", "1st" → "1"
        cleaned = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", raw)
        # Try with year first
        for fmt in ("%Y-%m-%d", "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Short format without year (e.g. "02 Mar") — assume current year
        for fmt in ("%d %b", "%d %B"):
            try:
                dt = datetime.strptime(cleaned, fmt).replace(year=datetime.now().year)
                if (datetime.now() - dt).days > 180:
                    dt = dt.replace(year=datetime.now().year + 1)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None
