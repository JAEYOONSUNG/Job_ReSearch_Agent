"""AcademicPositions.com scraper for postdoc listings in life sciences.

The site uses Laravel Livewire for its frontend but renders job listings
server-side, so plain HTTP GET requests work for both listing and detail
pages.  Category browsing uses URL patterns like::

    /jobs/position/post-doc          (all postdocs)
    /jobs/field/molecular-biology    (by research field)
    /jobs/field/microbiology?page=2  (pagination)

There is no public GET-based keyword search; the search form uses a
Livewire POST mechanism.  We therefore rely on browsing multiple
field categories relevant to life sciences.
"""

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

# ── Category URLs ────────────────────────────────────────────────────────
# The site organises jobs by position type and by field.
# We scrape the postdoc position listing plus several life-science fields.

CATEGORY_URLS = [
    # Position-based (all postdocs)
    f"{BASE_URL}/jobs/position/post-doc",
    # Field-based (life-science & adjacent)
    f"{BASE_URL}/jobs/field/molecular-biology",
    f"{BASE_URL}/jobs/field/cell-biology",
    f"{BASE_URL}/jobs/field/biotechnology",
    f"{BASE_URL}/jobs/field/microbiology",
    f"{BASE_URL}/jobs/field/genetics",
]

MAX_PAGES = 5  # 30 results per page => up to 150 per category


class AcademicPositionsScraper(BaseScraper):
    """Scrape AcademicPositions.com for postdoc listings in life sciences."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "academicpositions"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a search/category results page.

        Each job card is a ``div.list-group-item`` containing:
        - An employer link ``a.job-link[href*="/employer/"]``
        - A location div ``div.job-locations``
        - A detail link ``a[href*="/ad/"]`` wrapping an ``<h4>`` title
        - A metadata row ``div.row.row-tight`` with published date,
          optional closing date, and position type
        """
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        cards = soup.select("div.list-group-item")

        if not cards:
            # Fallback: any links pointing to /ad/ detail pages
            for link in soup.select("a[href*='/ad/']"):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and len(title) > 5 and href:
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
        """Extract job info from a ``div.list-group-item`` card."""

        # ── Title + detail link ──────────────────────────────────────────
        detail_link = card.select_one("a[href*='/ad/']")
        if not detail_link:
            return None

        title_el = detail_link.select_one("h4")
        title = title_el.get_text(strip=True) if title_el else detail_link.get_text(strip=True)
        href = detail_link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # ── Employer / institute ─────────────────────────────────────────
        inst_link = card.select_one("a.job-link[href*='/employer/']")
        institute = inst_link.get_text(strip=True) if inst_link else None

        # ── Location / country ───────────────────────────────────────────
        loc_div = card.select_one("div.job-locations")
        location = loc_div.get_text(strip=True) if loc_div else None
        country = self._extract_country(location)

        # ── Deadline (from the metadata row) ─────────────────────────────
        deadline = None
        meta_row = card.select_one("div.row.row-tight")
        if meta_row:
            for col in meta_row.select("div.col-auto"):
                col_text = col.get_text(strip=True)
                if "Closing on:" in col_text:
                    deadline = self._parse_date(col_text)
                    break

        # ── Summary (the <p> snippet below the title) ────────────────────
        summary_el = detail_link.select_one("p")
        summary = summary_el.get_text(strip=True) if summary_el else None

        job: dict[str, Any] = {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "deadline": deadline,
            "source": self.name,
        }
        if summary:
            job["_summary"] = summary  # used for keyword filtering only
        return job

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch detail page for full description and metadata."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Description ──────────────────────────────────────────────
            # The main job description lives in a div with classes
            # "editor ck-content" (CKEditor output).
            found_desc = False
            for sel in (
                "div.editor.ck-content",
                "div.ck-content",
                "div.editor",
            ):
                desc_el = soup.select_one(sel)
                if desc_el:
                    text = desc_el.get_text(separator="\n", strip=True)
                    if len(text) > 50:
                        job["description"] = text[:15000]
                        found_desc = True
                        break

            if not found_desc:
                fallback = self._extract_description_fallback(resp.text)
                if fallback:
                    job["description"] = fallback

            # ── Institute (from header h6 > a) ───────────────────────────
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "h6 a[href*='/employer/']"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # ── Country (from header job-locations) ──────────────────────
            if not job.get("country"):
                header = soup.select_one("#premium-template-header-content")
                if header:
                    loc_el = header.select_one("div.job-locations")
                else:
                    loc_el = soup.select_one("div.job-locations")
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

            # ── Metadata rows (label → value pairs) ─────────────────────
            # The detail page has rows like:
            #   <div class="row mb-3">
            #     <div class="col-12 col-md-4"><div class="font-weight-bold">Label</div></div>
            #     <div class="col-auto col-md-8">Value</div>
            #   </div>
            metadata = self._parse_metadata_rows(soup)

            if not job.get("deadline") and metadata.get("Application deadline"):
                raw_dl = metadata["Application deadline"]
                if raw_dl.lower() not in ("unspecified", "not specified", "n/a", ""):
                    job["deadline"] = self._parse_date(raw_dl)

            if not job.get("field") and metadata.get("Field"):
                # Field value may have commas and "and N more" suffix
                raw_field = metadata["Field"]
                # Clean up: "Biotechnology,,Molecular Biology,,..." -> list
                parts = [p.strip() for p in re.split(r",{1,2}", raw_field) if p.strip()]
                # Remove "and N more..." suffix from last part
                cleaned = []
                for p in parts:
                    m = re.match(r"^and \d+ more", p)
                    if not m:
                        cleaned.append(p)
                job["field"] = ", ".join(cleaned[:5]) if cleaned else raw_field

        except Exception:
            self.logger.debug("Could not fetch detail: %s", url)

        return job

    @staticmethod
    def _parse_metadata_rows(soup: BeautifulSoup) -> dict[str, str]:
        """Extract label-value metadata from the detail page.

        Returns a dict like ``{"Application deadline": "2026-03-01", ...}``.
        """
        meta: dict[str, str] = {}
        for label_div in soup.find_all("div", class_="font-weight-bold"):
            label = label_div.get_text(strip=True)
            if label not in (
                "Title", "Employer", "Location", "Published",
                "Application deadline", "Job type", "Field",
            ):
                continue
            # Walk up: label_div -> col div -> row div
            col = label_div.parent
            if not col:
                continue
            row = col.parent
            if not row:
                continue
            # The value is in the second col child
            value_div = row.select_one("div.col-auto.col-md-8, div.col-md-8")
            if value_div:
                meta[label] = value_div.get_text(strip=True)
        return meta

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for category_url in CATEGORY_URLS:
            for page in range(MAX_PAGES):
                try:
                    url = (
                        category_url
                        if page == 0
                        else f"{category_url}?page={page + 1}"
                    )
                    resp = self.fetch(url)
                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break
                    # Deduplicate across categories
                    new_jobs = []
                    for j in page_jobs:
                        if j["url"] not in seen_urls:
                            seen_urls.add(j["url"])
                            new_jobs.append(j)
                    self.logger.info(
                        "%s page %d: %d results (%d new)",
                        category_url, page + 1, len(page_jobs), len(new_jobs),
                    )
                    all_jobs.extend(new_jobs)
                except Exception:
                    self.logger.exception(
                        "Failed to fetch %s page %d", category_url, page + 1,
                    )
                    break

        # Keyword filter
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = (
                f"{job.get('title', '')} {job.get('_summary', '')} "
                f"{job.get('description', '')} {job.get('field', '')}"
            )
            if self._keyword_match(blob):
                filtered.append(job)

        self.logger.info(
            "AcademicPositions: %d total, %d after keyword filter",
            len(all_jobs), len(filtered),
        )

        # Enrich all filtered results (parallel, skip already-in-DB)
        enriched = self._parallel_enrich(
            filtered, self._enrich_from_detail, max_workers=4,
        )

        # Remove internal-only keys before returning to pipeline
        for job in enriched:
            job.pop("_summary", None)

        self.logger.info(
            "AcademicPositions: %d enriched", len(enriched),
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
        # Try direct date formats first
        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M",
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
        # Extract YYYY-MM-DD anywhere in string (e.g. "Closing on: 2026-02-28 (CET)")
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None
