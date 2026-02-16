"""EURAXESS scraper – European postdoc positions in Life Sciences."""

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

BASE_URL = "https://euraxess.ec.europa.eu"

# EURAXESS (redesigned ~2025) uses faceted filters: f[0]=type:value&f[1]=type:value
# Filter format discovered from their current site
SEARCH_URL = f"{BASE_URL}/jobs/search"

# We build multiple filter combos to cover postdoc positions in life sciences
# Each entry is a list of f[] filters to combine
FILTER_COMBOS = [
    # Job offers only, sorted by newest
    [
        "offer_type:job_offer",
    ],
]

# Sort params
SORT_PARAMS = {
    "sort[name]": "created",
    "sort[direction]": "DESC",
}

# We rely on keyword-based search since faceted filters may 403
# These are appended as 'keywords' param
SEARCH_KEYWORDS_EURAXESS = [
    "postdoc biology",
    "postdoctoral life sciences",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc synthetic biology",
    "postdoctoral microbiology",
    "postdoc chemistry biology",
]

MAX_PAGES = 3


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

        # EURAXESS renders results as article blocks, views-rows, or generic cards
        result_items = soup.select(
            "div.views-row, article.node--type-job-offer, div.result-item, "
            "div.teaser, article.teaser, div[class*='search-result'], "
            "div.card, li.result"
        )
        if not result_items:
            # Fallback: look for any links to job detail pages
            for link in soup.select("a[href*='/jobs/']"):
                href = link.get("href", "")
                if not href or "/jobs/search" in href:
                    continue
                # Must look like a detail page (has a numeric ID or slug)
                if re.search(r"/jobs/\d+|/jobs/[a-z].*-\d+", href):
                    full_url = urljoin(BASE_URL, href)
                    title = link.get_text(strip=True)
                    if title and len(title) > 5:
                        jobs.append({"title": title, "url": full_url, "source": self.name})
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
            ".organisation, .company, span.employer"
        )
        institute = org_el.get_text(strip=True) if org_el else None

        # Country
        country_el = item.select_one(
            "span.field--name-field-country, "
            "div.field--name-field-country, "
            ".country, .location, span.country"
        )
        country = country_el.get_text(strip=True) if country_el else None

        # Deadline
        deadline_el = item.select_one(
            "span.field--name-field-application-deadline, "
            "div.date, .deadline, time"
        )
        deadline = None
        if deadline_el:
            raw = deadline_el.get("datetime") or deadline_el.get_text(strip=True)
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

            # Full description (keep more for structured parsing)
            for sel in (
                "div.field--name-field-eo-job-description",
                "div.field--name-body",
                "div.job-description",
                "article .field--name-body",
                "div[class*='description']",
                "main article",
            ):
                desc_el = soup.select_one(sel)
                if desc_el:
                    job["description"] = desc_el.get_text(separator="\n", strip=True)[:3000]
                    break

            # Organisation if still missing
            if not job.get("institute"):
                for sel in (
                    "div.field--name-field-organisation a",
                    "span.field--name-field-organisation",
                    "span.employer",
                    "div.organisation",
                ):
                    org_el = soup.select_one(sel)
                    if org_el:
                        job["institute"] = org_el.get_text(strip=True)
                        break

            # Country if still missing
            if not job.get("country"):
                for sel in (
                    "div.field--name-field-country",
                    "span.field--name-field-country",
                    "span.country",
                ):
                    country_el = soup.select_one(sel)
                    if country_el:
                        job["country"] = country_el.get_text(strip=True)
                        break

            # Deadline
            if not job.get("deadline"):
                for sel in (
                    "div.field--name-field-application-deadline",
                    "span.date-display-single",
                    "time.deadline",
                ):
                    dl_el = soup.select_one(sel)
                    if dl_el:
                        raw = dl_el.get("datetime") or dl_el.get_text(strip=True)
                        job["deadline"] = self._parse_date_text(raw)
                        break

            # Research field
            for sel in (
                "div.field--name-field-research-field",
                "span.field--name-field-research-field",
                "span.research-field",
            ):
                field_el = soup.select_one(sel)
                if field_el:
                    job["field"] = field_el.get_text(strip=True)
                    break

            # EURAXESS-specific: Research profile (R1-R4)
            for sel in (
                "div.field--name-field-research-profile",
                "span.research-profile",
            ):
                rp_el = soup.select_one(sel)
                if rp_el:
                    rp_text = rp_el.get_text(strip=True)
                    existing = job.get("conditions") or ""
                    job["conditions"] = f"{existing} | Research profile: {rp_text}".strip(" |")
                    break

            # EURAXESS-specific: Type of contract
            for sel in (
                "div.field--name-field-type-of-contract",
                "span.contract-type",
            ):
                ct_el = soup.select_one(sel)
                if ct_el:
                    ct_text = ct_el.get_text(strip=True)
                    existing = job.get("conditions") or ""
                    job["conditions"] = f"{existing} | Contract: {ct_text}".strip(" |")
                    break

        except Exception:
            self.logger.debug("Could not enrich detail for %s", url)

        return job

    # ── Main scrape logic ─────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for keyword in SEARCH_KEYWORDS_EURAXESS:
            self.logger.info("EURAXESS search: %s", keyword)

            for page in range(MAX_PAGES):
                params = dict(SORT_PARAMS)
                params["keywords"] = keyword
                params["page"] = str(page)

                try:
                    resp = self.fetch(SEARCH_URL, params=params)
                except Exception:
                    self.logger.exception(
                        "Failed to fetch EURAXESS '%s' page %d", keyword, page
                    )
                    break

                page_jobs = self._parse_listing_page(resp.text)
                if not page_jobs:
                    self.logger.info(
                        "No results for '%s' page %d, stopping", keyword, page
                    )
                    break

                self.logger.info(
                    "'%s' page %d: %d results", keyword, page, len(page_jobs)
                )
                all_jobs.extend(page_jobs)

        # Enrich top results with detail pages (limit to avoid hammering)
        enriched: list[dict[str, Any]] = []
        for job in all_jobs[:40]:
            job = self._enrich_from_detail(job)
            # Keyword filter on enriched description
            blob = f"{job.get('title', '')} {job.get('description', '')} {job.get('field', '')}"
            if self._keyword_match(blob):
                enriched.append(job)

        # Add remaining (no detail enrichment) with keyword filter
        for job in all_jobs[40:]:
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
        # Always match postdoc-related terms
        if any(term in lower for term in ("postdoc", "postdoctoral", "post-doc")):
            return True
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
            "%Y-%m-%dT%H:%M:%S",
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
