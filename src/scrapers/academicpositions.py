"""AcademicPositions.com scraper for postdoc listings in life sciences."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://academicpositions.com"

# AcademicPositions has category-based URLs for browsing
SEARCH_URLS = [
    f"{BASE_URL}/find-jobs/postdoc/life-sciences",
    f"{BASE_URL}/find-jobs/postdoc/biological-sciences",
    f"{BASE_URL}/find-jobs/postdoc/chemistry",
    f"{BASE_URL}/find-jobs/postdoc/biotechnology",
]

# Also search with keywords
SEARCH_API_URL = f"{BASE_URL}/find-jobs"
SEARCH_KEYWORDS_AP = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc microbiology",
    "postdoc metabolic engineering",
]

MAX_PAGES = 3


class AcademicPositionsScraper(BaseScraper):
    """Scrape AcademicPositions.com for postdoc listings in life sciences."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "academicpositions"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a search/category results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # AcademicPositions renders job cards in list items or divs
        cards = soup.select(
            "div.job-card, div.search-result, article.job-listing, "
            "li.job-item, div.position-card, div[class*='JobCard'], "
            "a[class*='JobCard']"
        )

        if not cards:
            # Fallback: any links pointing to /ad/ or /jobs/ detail pages
            for link in soup.select(
                "a[href*='/ad/'], a[href*='/jobs/'], a[href*='/position/']"
            ):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and len(title) > 5 and href:
                    full_url = urljoin(BASE_URL, href)
                    # Avoid navigation/category links
                    if re.search(r"/ad/|/position/|/jobs/\d+", href):
                        jobs.append({
                            "title": title,
                            "url": full_url,
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
        # Title + link
        link = card if card.name == "a" else card.select_one(
            "a[href*='/ad/'], a[href*='/position/'], a[href*='/jobs/'], h2 a, h3 a"
        )
        if not link:
            return None

        title_el = card.select_one("h2, h3, span.title, div.title, strong")
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Institute
        inst_el = card.select_one(
            "span.employer, span.institution, div.employer, "
            "p.employer, span.company, div.university"
        )
        institute = inst_el.get_text(strip=True) if inst_el else None

        # Location / Country
        loc_el = card.select_one(
            "span.location, div.location, span.country, "
            "p.location, span.place"
        )
        location = loc_el.get_text(strip=True) if loc_el else None
        country = self._extract_country(location)

        # Deadline
        deadline_el = card.select_one(
            "span.deadline, time, span.date, div.deadline"
        )
        deadline = None
        if deadline_el:
            raw = deadline_el.get("datetime") or deadline_el.get_text(strip=True)
            deadline = self._parse_date(raw)

        # Field
        field_el = card.select_one(
            "span.discipline, span.field, span.category, "
            "div.discipline"
        )
        field = field_el.get_text(strip=True) if field_el else None

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "deadline": deadline,
            "field": field,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch detail page for full description and metadata."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Description (larger for structured parsing)
            found_desc = False
            for sel in (
                "div.job-description", "div.position-description",
                "div[class*='Description']", "div.content-body",
                "div.ad-description", "section.description",
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
                fallback = self._extract_description_fallback(resp.text)
                if fallback:
                    job["description"] = fallback

            # Institute
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "a.employer, span.employer, "
                    "div.employer-name, h2.employer"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Country
            if not job.get("country"):
                loc_el = soup.select_one(
                    "span.location, div.location, "
                    "span[itemprop='addressCountry']"
                )
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

            # Deadline
            if not job.get("deadline"):
                dl_el = soup.select_one(
                    "span.deadline, div.application-deadline, "
                    "time.deadline"
                )
                if dl_el:
                    raw = dl_el.get("datetime") or dl_el.get_text(strip=True)
                    job["deadline"] = self._parse_date(raw)

            # Department
            dept_el = soup.select_one(
                "span.department, div.department, "
                "span[itemprop='department']"
            )
            if dept_el:
                job["department"] = dept_el.get_text(strip=True)

        except Exception:
            self.logger.debug("Could not fetch detail: %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        # 1. Browse category pages
        for category_url in SEARCH_URLS:
            for page in range(MAX_PAGES):
                try:
                    url = category_url if page == 0 else f"{category_url}?page={page + 1}"
                    resp = self.fetch(url)
                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break
                    self.logger.info(
                        "%s page %d: %d results", category_url, page + 1, len(page_jobs)
                    )
                    all_jobs.extend(page_jobs)
                except Exception:
                    self.logger.exception(
                        "Failed to fetch %s page %d", category_url, page + 1
                    )
                    break

        # 2. Keyword searches
        for keyword in SEARCH_KEYWORDS_AP:
            try:
                params = {"q": keyword, "type": "postdoc"}
                resp = self.fetch(SEARCH_API_URL, params=params)
                kw_jobs = self._parse_listing_page(resp.text)
                self.logger.info(
                    "Keyword '%s': %d results", keyword, len(kw_jobs)
                )
                all_jobs.extend(kw_jobs)
            except Exception:
                self.logger.exception(
                    "Failed keyword search: %s", keyword
                )

        # Keyword filter
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')} {job.get('field', '')}"
            if self._keyword_match(blob):
                filtered.append(job)

        # Enrich top results (parallel, skip already-in-DB)
        enriched = self._parallel_enrich(
            filtered, self._enrich_from_detail, max_workers=4, limit=40,
        )

        self.logger.info(
            "AcademicPositions: %d total, %d after filter",
            len(all_jobs),
            len(enriched),
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

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
        parts = [p.strip() for p in location.split(",")]
        return parts[-1] if parts else None

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%B %d, %Y",
            "%d %B %Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None
