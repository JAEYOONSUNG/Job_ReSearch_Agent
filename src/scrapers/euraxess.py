"""EURAXESS scraper – European postdoc positions in Life Sciences."""

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

BASE_URL = "https://euraxess.ec.europa.eu"

# EURAXESS search URL for postdoc positions in Biological Sciences
SEARCH_URL = (
    f"{BASE_URL}/jobs/search"
)

# Filters passed as query parameters
SEARCH_PARAMS = {
    "keywords": "",
    "research_field[]": "3",            # Biological Sciences
    "research_profile[]": "2",          # Recognised Researcher (R2) = postdoc level
    "sort": "publicationDateDesc",      # newest first
}

MAX_PAGES = 5  # scrape up to 5 pages of results


class EuraxessScraper(BaseScraper):
    """Scrape EURAXESS for postdoc positions in life sciences across Europe."""

    rate_limit: float = 2.5

    @property
    def name(self) -> str:
        return "euraxess"

    # ── Listing page parsing ──────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a EURAXESS search results page and return partial job dicts."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # EURAXESS renders results as article blocks or div.views-row
        result_items = soup.select(
            "div.views-row, article.node--type-job-offer, div.result-item"
        )
        if not result_items:
            # Fallback: look for any links containing /jobs/
            result_items = soup.select("div.view-content a[href*='/jobs/']")
            for link in result_items:
                href = link.get("href", "")
                if not href:
                    continue
                full_url = urljoin(BASE_URL, href)
                title = link.get_text(strip=True)
                if title:
                    jobs.append({"title": title, "url": full_url})
            return jobs

        for item in result_items:
            job = self._parse_result_item(item)
            if job:
                jobs.append(job)

        return jobs

    def _parse_result_item(self, item: Tag) -> dict[str, Any] | None:
        """Extract job info from a single search result card."""
        # Title and link
        title_link = item.select_one("h2 a, h3 a, a.title, a[href*='/jobs/']")
        if not title_link:
            return None
        title = title_link.get_text(strip=True)
        href = title_link.get("href", "")
        if not href:
            return None
        url = urljoin(BASE_URL, href)

        # Organisation / institute
        org_el = item.select_one(
            "span.field--name-field-organisation, "
            "div.field--name-field-organisation, "
            ".organisation, .company"
        )
        institute = org_el.get_text(strip=True) if org_el else None

        # Country
        country_el = item.select_one(
            "span.field--name-field-country, "
            "div.field--name-field-country, "
            ".country, .location"
        )
        country = country_el.get_text(strip=True) if country_el else None

        # Deadline
        deadline_el = item.select_one(
            "span.field--name-field-application-deadline, "
            "div.date, .deadline"
        )
        deadline = None
        if deadline_el:
            raw = deadline_el.get_text(strip=True)
            deadline = self._parse_date_text(raw)

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "deadline": deadline,
            "source": self.name,
        }

    # ── Detail page (optional enrichment) ─────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page for richer data (description, deadline, etc)."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Description
            desc_el = soup.select_one(
                "div.field--name-field-eo-job-description, "
                "div.field--name-body, "
                "div.job-description, "
                "article .field--name-body"
            )
            if desc_el:
                job["description"] = desc_el.get_text(separator=" ", strip=True)[:2000]

            # Organisation if still missing
            if not job.get("institute"):
                org_el = soup.select_one(
                    "div.field--name-field-organisation a, "
                    "span.field--name-field-organisation"
                )
                if org_el:
                    job["institute"] = org_el.get_text(strip=True)

            # Country if still missing
            if not job.get("country"):
                country_el = soup.select_one(
                    "div.field--name-field-country, "
                    "span.field--name-field-country"
                )
                if country_el:
                    job["country"] = country_el.get_text(strip=True)

            # Deadline
            if not job.get("deadline"):
                dl_el = soup.select_one(
                    "div.field--name-field-application-deadline, "
                    "span.date-display-single"
                )
                if dl_el:
                    job["deadline"] = self._parse_date_text(
                        dl_el.get_text(strip=True)
                    )

            # Research field
            field_el = soup.select_one(
                "div.field--name-field-research-field, "
                "span.field--name-field-research-field"
            )
            if field_el:
                job["field"] = field_el.get_text(strip=True)

        except Exception:
            self.logger.debug("Could not enrich detail for %s", url)

        return job

    # ── Main scrape logic ─────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for page in range(MAX_PAGES):
            params = dict(SEARCH_PARAMS)
            params["page"] = str(page)

            try:
                resp = self.fetch(SEARCH_URL, params=params)
            except Exception:
                self.logger.exception("Failed to fetch EURAXESS page %d", page)
                break

            page_jobs = self._parse_listing_page(resp.text)
            if not page_jobs:
                self.logger.info("No more results on page %d, stopping", page)
                break

            self.logger.info("Page %d: %d results", page, len(page_jobs))
            all_jobs.extend(page_jobs)

        # Optionally enrich top results with detail pages
        enriched: list[dict[str, Any]] = []
        for job in all_jobs:
            job = self._enrich_from_detail(job)
            # Keyword filter on enriched description
            blob = f"{job.get('title', '')} {job.get('description', '')} {job.get('field', '')}"
            if self._keyword_match(blob):
                enriched.append(job)

        self.logger.info(
            "EURAXESS: %d total, %d after keyword filter", len(all_jobs), len(enriched)
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        """Return True if text matches any CV keyword."""
        lower = text.lower()
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _parse_date_text(raw: str) -> str | None:
        """Try common European date formats."""
        raw = raw.strip()
        for fmt in (
            "%d/%m/%Y",
            "%d %B %Y",
            "%d %b %Y",
            "%Y-%m-%d",
            "%B %d, %Y",
        ):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Last resort: extract anything that looks like a date
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        if m:
            return m.group(0)
        return None
