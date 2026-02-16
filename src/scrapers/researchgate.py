"""ResearchGate jobs scraper for postdoc positions."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS, SEARCH_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.researchgate.net"
JOBS_URL = f"{BASE_URL}/jobs"

# We build search queries from a curated subset of SEARCH_KEYWORDS
# (ResearchGate search is simpler than full-text job boards)
SEARCH_TERMS = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc metabolic engineering",
    "postdoc microbiology",
    "postdoctoral researcher biology",
    "postdoc directed evolution",
    "postdoc genome engineering",
]

MAX_PAGES = 3


class ResearchGateScraper(BaseScraper):
    """Scrape ResearchGate job listings for postdoc positions in the life sciences."""

    rate_limit: float = 3.0  # ResearchGate is aggressive about bots

    @property
    def name(self) -> str:
        return "researchgate"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a ResearchGate jobs search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # ResearchGate renders job cards in various containers
        cards = soup.select(
            "div.nova-legacy-o-stack__item, "
            "div.search-result-item, "
            "div.job-card, "
            "li.search-result, "
            "div[itemprop='itemListElement']"
        )

        if not cards:
            # Fallback: try to find any job links
            for link in soup.select("a[href*='/job/']"):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and href:
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
        # Title + link
        link = card.select_one(
            "a[href*='/job/'], a.nova-legacy-e-text--size-l, "
            "a.search-result__title, h3 a, h2 a"
        )
        if not link:
            return None

        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Institute
        inst_el = card.select_one(
            "a[href*='/institution/'], span.institution, "
            "div.nova-legacy-v-entity-item__info-section-list-item, "
            ".company-name"
        )
        institute = inst_el.get_text(strip=True) if inst_el else None

        # Location
        loc_el = card.select_one(
            "span.location, div.location, "
            "span.nova-legacy-e-text--color--grey-700"
        )
        location = loc_el.get_text(strip=True) if loc_el else None
        country = self._extract_country(location)

        # Date
        date_el = card.select_one("span.date, time, span.posted-date")
        posted_date = None
        if date_el:
            raw = date_el.get("datetime") or date_el.get_text(strip=True)
            posted_date = self._parse_date(raw)

        # Snippet / description
        desc_el = card.select_one(
            "div.nova-legacy-e-text--size-m, p.description, "
            "div.search-result__snippet"
        )
        description = desc_el.get_text(strip=True)[:1000] if desc_el else None

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "posted_date": posted_date,
            "description": description,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Optionally fetch the full job detail page."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Full description
            desc_el = soup.select_one(
                "div.job-description, "
                "div.nova-legacy-o-stack, "
                "div[itemprop='description'], "
                "div.research-detail-middle-section"
            )
            if desc_el and not job.get("description"):
                job["description"] = desc_el.get_text(separator=" ", strip=True)[:2000]

            # Institute
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "a[href*='/institution/'], "
                    "span[itemprop='hiringOrganization']"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Country
            if not job.get("country"):
                loc_el = soup.select_one(
                    "span[itemprop='addressCountry'], "
                    "span.location, div.location"
                )
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

            # Deadline
            dl_el = soup.select_one(
                "span.deadline, div.application-deadline, "
                "span:contains('Deadline'), span:contains('deadline')"
            )
            if dl_el:
                job["deadline"] = self._parse_date(dl_el.get_text(strip=True))

        except Exception:
            self.logger.debug("Could not fetch detail for %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for term in SEARCH_TERMS:
            self.logger.info("ResearchGate search: %s", term)

            for page in range(MAX_PAGES):
                try:
                    params = {
                        "query": term,
                        "page": str(page + 1),
                    }
                    resp = self.fetch(JOBS_URL, params=params)

                    # ResearchGate may return a 403/captcha page
                    if resp.status_code == 403:
                        self.logger.warning(
                            "ResearchGate returned 403 for '%s'; stopping", term
                        )
                        break

                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break

                    self.logger.info(
                        "'%s' page %d: %d results", term, page + 1, len(page_jobs)
                    )
                    all_jobs.extend(page_jobs)

                except Exception:
                    self.logger.exception(
                        "ResearchGate search failed: '%s' page %d",
                        term,
                        page + 1,
                    )
                    break

        # Keyword filter
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')}"
            if self._keyword_match(blob):
                job = self._enrich_from_detail(job)
                filtered.append(job)

        self.logger.info(
            "ResearchGate: %d total, %d after filter", len(all_jobs), len(filtered)
        )
        return filtered

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        # Always match if "postdoc" is in the title
        if "postdoc" in lower:
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
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%d %B %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None
