"""ScholarshipDB scraper for postdoc opportunities."""

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

BASE_URL = "https://scholarshipdb.net"

# ScholarshipDB search URLs for postdoc positions
SEARCH_URL = f"{BASE_URL}/scholarships"
SEARCH_QUERIES = [
    "postdoc biology",
    "postdoctoral life sciences",
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc microbiology",
    "postdoc biotechnology",
    "postdoctoral chemistry biology",
    "postdoc metabolic engineering",
]

MAX_PAGES = 3


class ScholarshipDBScraper(BaseScraper):
    """Scrape ScholarshipDB for postdoc fellowship and position opportunities."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "scholarshipdb"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a ScholarshipDB search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # ScholarshipDB renders results as list items / cards
        cards = soup.select(
            "div.scholarship-card, div.result-item, "
            "li.scholarship-item, div.listing-item, "
            "div.card, article.post, div[class*='Scholarship']"
        )

        if not cards:
            # Fallback: find links to scholarship detail pages
            for link in soup.select(
                "a[href*='/scholarships/'], a[href*='/postdoc/'], "
                "a[href*='/scholarship/']"
            ):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if (
                    title
                    and len(title) > 10
                    and href
                    and not href.endswith("/scholarships/")
                ):
                    full_url = urljoin(BASE_URL, href)
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
        """Extract info from a search result card."""
        # Title + link
        link = card.select_one("a[href*='/scholarship'], a[href], h2 a, h3 a")
        if not link:
            return None

        title_el = card.select_one("h2, h3, h4, span.title, strong.title")
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Institute / Provider
        inst_el = card.select_one(
            "span.provider, span.institution, div.university, "
            "span.university, p.institution"
        )
        institute = inst_el.get_text(strip=True) if inst_el else None

        # Country
        country_el = card.select_one(
            "span.country, span.location, div.country, p.location"
        )
        country = country_el.get_text(strip=True) if country_el else None

        # Deadline
        deadline_el = card.select_one(
            "span.deadline, time, span.date, div.deadline, "
            "p.deadline"
        )
        deadline = None
        if deadline_el:
            raw = deadline_el.get("datetime") or deadline_el.get_text(strip=True)
            deadline = self._parse_date(raw)

        # Short description
        desc_el = card.select_one(
            "p.description, div.summary, span.excerpt, p.snippet"
        )
        description = desc_el.get_text(strip=True)[:500] if desc_el else None

        # Field
        field_el = card.select_one(
            "span.subject, span.field, span.discipline, "
            "div.subject"
        )
        field = field_el.get_text(strip=True) if field_el else None

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "deadline": deadline,
            "description": description,
            "field": field,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page for more information."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Full description
            desc_el = soup.select_one(
                "div.scholarship-description, div.content, "
                "article, div.detail-body, div[class*='Description'], "
                "div.entry-content"
            )
            if desc_el:
                job["description"] = desc_el.get_text(separator="\n", strip=True)[:3000]

            # Institute
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "span.provider, a.university, "
                    "div.institution, span.institution"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Country
            if not job.get("country"):
                country_el = soup.select_one(
                    "span.country, div.country, "
                    "span[itemprop='addressCountry']"
                )
                if country_el:
                    job["country"] = country_el.get_text(strip=True)

            # Deadline
            if not job.get("deadline"):
                dl_el = soup.select_one(
                    "span.deadline, div.deadline, "
                    "td:contains('Deadline') + td, "
                    "th:contains('Deadline') + td"
                )
                if dl_el:
                    job["deadline"] = self._parse_date(dl_el.get_text(strip=True))

            # Extract country from description if still missing
            if not job.get("country") and job.get("description"):
                job["country"] = self._guess_country_from_text(
                    job["description"]
                )

        except Exception:
            self.logger.debug("Could not enrich detail: %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for query in SEARCH_QUERIES:
            self.logger.info("ScholarshipDB search: %s", query)

            for page in range(MAX_PAGES):
                try:
                    params = {
                        "q": query,
                        "page": str(page + 1),
                        "type": "postdoc",
                    }
                    resp = self.fetch(SEARCH_URL, params=params)
                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break

                    self.logger.info(
                        "'%s' page %d: %d results", query, page + 1, len(page_jobs)
                    )
                    all_jobs.extend(page_jobs)

                except Exception:
                    self.logger.exception(
                        "ScholarshipDB failed: '%s' page %d", query, page + 1
                    )
                    break

        # Filter for relevance
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')} {job.get('field', '')}"
            if self._keyword_match(blob):
                filtered.append(job)

        # Enrich top results
        enriched: list[dict[str, Any]] = []
        for job in filtered[:30]:
            job = self._enrich_from_detail(job)
            enriched.append(job)
        enriched.extend(filtered[30:])

        self.logger.info(
            "ScholarshipDB: %d total, %d after filter",
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
    def _parse_date(raw: str | None) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        for fmt in (
            "%Y-%m-%d",
            "%B %d, %Y",
            "%d %B %Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None

    @staticmethod
    def _guess_country_from_text(text: str) -> str | None:
        """Try to find a country name in text."""
        from src.config import COUNTRY_TO_REGION

        lower = text.lower()
        for country in COUNTRY_TO_REGION:
            if country.lower() in lower:
                return country
        return None
