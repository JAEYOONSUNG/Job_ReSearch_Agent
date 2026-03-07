"""jobs.ac.uk scraper — UK/global academic postdoc positions (Playwright-based)."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS
from src.matching.job_parser import (
    extract_application_materials,
    extract_contact_email,
    extract_deadline,
    extract_department,
)
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jobs.ac.uk"

SEARCH_KEYWORDS = [
    # Field-specific
    "postdoc synthetic biology",
    "postdoctoral CRISPR",
    "postdoc protein engineering",
    "postdoc microbiology",
    "postdoc biotechnology",
    # Institution-specific (premier UK research institutes)
    "Wellcome Sanger Institute",
    "Francis Crick Institute",
    "MRC Laboratory of Molecular Biology",
    "European Bioinformatics Institute",
    "Babraham Institute",
    "University of Oxford biology",
    "University of Cambridge biology",
    "Imperial College London biology",
]

MAX_PAGES = 2


class JobsAcUkScraper(BaseScraper):
    """Scrape jobs.ac.uk for postdoc positions using Playwright (bypasses Cloudflare)."""

    rate_limit: float = 3.0

    @property
    def name(self) -> str:
        return "jobs_ac_uk"

    def _fetch_with_browser(self, url: str) -> str | None:
        """Fetch URL using Playwright headless browser."""
        from src.scrapers.browser import fetch_page
        return fetch_page(url, wait_selector="div.j-search-result, div.search-results, article", wait_ms=5000, timeout=30000)

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
                        job["description"] = text[:15000]
                        found_desc = True
                        break

            if not found_desc and html:
                fallback = self._extract_description_fallback(html)
                if fallback:
                    job["description"] = fallback

            # Parse detail columns for structured info
            first_col = soup.select_one("div.j-advert-details__first-col")
            if first_col:
                fields = self._extract_labeled_details(first_col.get_text(separator="\n", strip=True))
                location = fields.get("Location")
                if location and not job.get("country"):
                    job["country"] = self._extract_country(location)
                cond_parts = []
                if fields.get("Salary"):
                    salary = re.sub(r"\s+", " ", fields["Salary"]).strip()
                    cond_parts.append(f"Salary: {salary}")
                if fields.get("Hours"):
                    cond_parts.append(f"Hours: {fields['Hours']}")
                if fields.get("Contract Type"):
                    cond_parts.append(f"Contract Type: {fields['Contract Type']}")
                if cond_parts:
                    existing = job.get("conditions") or ""
                    joined = " | ".join(cond_parts)
                    job["conditions"] = f"{existing} | {joined}".strip(" |")

            second_col = soup.select_one("div.j-advert-details__second-col")
            if second_col:
                fields = self._extract_labeled_details(second_col.get_text(separator="\n", strip=True))
                if fields.get("Placed On") and not job.get("posted_date"):
                    job["posted_date"] = self._parse_date(fields["Placed On"])
                if fields.get("Closes") and not job.get("deadline"):
                    job["deadline"] = self._parse_date(fields["Closes"])

            desc = job.get("description") or ""
            if desc:
                if not job.get("department"):
                    dept = extract_department(desc)
                    if dept:
                        job["department"] = dept
                if not job.get("application_materials"):
                    app_materials = extract_application_materials(desc)
                    if app_materials:
                        job["application_materials"] = app_materials
                if not job.get("contact_email"):
                    contact = extract_contact_email(desc)
                    if contact:
                        job["contact_email"] = contact
                if not job.get("deadline"):
                    deadline = extract_deadline(desc)
                    if deadline:
                        job["deadline"] = deadline

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

        # Enrich top results with detail pages (parallel, skip already-in-DB)
        enriched = self._parallel_enrich(
            filtered, self._enrich_from_detail, max_workers=2, limit=30,
        )

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
    def _extract_labeled_details(text: str) -> dict[str, str]:
        """Parse label/value metadata blocks from jobs.ac.uk detail columns."""
        lines = [
            re.sub(r"\s+", " ", line).strip(" |")
            for line in text.splitlines()
            if line.strip()
        ]
        known = {"Location", "Salary", "Hours", "Contract Type", "Placed On", "Closes", "Job Ref"}
        fields: dict[str, str] = {}
        i = 0
        while i < len(lines):
            line = lines[i]
            label = ""
            value = ""
            if ":" in line:
                maybe_label, maybe_value = [part.strip() for part in line.split(":", 1)]
                if maybe_label in known:
                    label = maybe_label
                    value = maybe_value
                    if not value and i + 1 < len(lines):
                        value = lines[i + 1].strip()
                        i += 1
            elif line.rstrip(":") in known:
                label = line.rstrip(":")
                if i + 1 < len(lines):
                    value = lines[i + 1].strip()
                    i += 1

            if label:
                fields[label] = value
            i += 1
        return fields

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
