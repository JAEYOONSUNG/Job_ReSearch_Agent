"""jobs.ac.kr scraper - Korean academic job board for postdoc/researcher positions."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jobs.ac.kr"

# Search parameters for postdoc/researcher positions in life sciences
SEARCH_QUERIES = [
    {"keyword": "포스닥", "field": ""},          # postdoc in Korean
    {"keyword": "박사후연구원", "field": ""},     # postdoctoral researcher
    {"keyword": "연구원 생명과학", "field": ""},  # researcher + life sciences
    {"keyword": "연구원 생물학", "field": ""},    # researcher + biology
    {"keyword": "연구원 화학", "field": ""},      # researcher + chemistry
    {"keyword": "postdoc", "field": ""},          # English postdoc
    {"keyword": "연구원 합성생물학", "field": ""}, # researcher + synthetic biology
    {"keyword": "연구원 단백질공학", "field": ""}, # researcher + protein engineering
]

# Alternative Korean academic job sites that redirect or mirror
SEARCH_URL = f"{BASE_URL}/jobs/search"

MAX_PAGES = 3


class JobsAcKrScraper(BaseScraper):
    """Scrape jobs.ac.kr for postdoc and researcher positions
    in biology/chemistry at Korean academic institutions.
    """

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "jobs_ac_kr"

    # ── Listing page parsing ──────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse search result page HTML."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # jobs.ac.kr typically renders results in table rows or list items
        rows = soup.select(
            "table.board-list tbody tr, "
            "div.list-item, "
            "ul.job-list li, "
            "div.result-item, "
            "div.job-item"
        )

        if not rows:
            # Fallback: find all links that look like job detail pages
            for link in soup.select("a[href]"):
                href = link.get("href", "")
                # Common patterns: /jobs/detail/12345, /recruit/view/12345
                if re.search(r"/(jobs|recruit|view|detail)/\d+", href):
                    title = link.get_text(strip=True)
                    if title and len(title) > 5:
                        full_url = urljoin(BASE_URL, href)
                        jobs.append({
                            "title": title,
                            "url": full_url,
                            "country": "South Korea",
                            "source": self.name,
                        })
            return jobs

        for row in rows:
            job = self._parse_row(row)
            if job:
                jobs.append(job)

        return jobs

    def _parse_row(self, row: Tag) -> dict[str, Any] | None:
        """Extract job info from a table row or list item."""
        # Title and link
        link = row.select_one("a[href]")
        if not link:
            return None

        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        href = link.get("href", "")
        url = urljoin(BASE_URL, href) if href else None
        if not url:
            return None

        # Institute (often in a separate column or span)
        inst_el = row.select_one(
            "td.company, td.institute, span.company, span.institute, "
            "div.company, .org-name"
        )
        institute = inst_el.get_text(strip=True) if inst_el else None

        # Department
        dept_el = row.select_one("td.department, span.department, div.dept")
        department = dept_el.get_text(strip=True) if dept_el else None

        # Deadline / dates
        date_el = row.select_one(
            "td.date, td.deadline, span.date, span.deadline, .period"
        )
        deadline = None
        posted_date = None
        if date_el:
            date_text = date_el.get_text(strip=True)
            deadline = self._parse_korean_date(date_text)

        # Field / category
        field_el = row.select_one("td.field, td.category, span.category")
        field = field_el.get_text(strip=True) if field_el else None

        return {
            "title": title,
            "institute": institute,
            "department": department,
            "country": "South Korea",
            "url": url,
            "deadline": deadline,
            "posted_date": posted_date,
            "field": field,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch detail page for richer information."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Description
            desc_el = soup.select_one(
                "div.content, div.job-detail, div.view-content, "
                "div.board-view, div.detail-content, article"
            )
            if desc_el:
                job["description"] = desc_el.get_text(separator=" ", strip=True)[:2000]

            # Institute if missing
            if not job.get("institute"):
                for selector in (
                    "span.company",
                    "div.company-name",
                    "th:contains('기관') + td",
                    "th:contains('대학') + td",
                ):
                    el = soup.select_one(selector)
                    if el:
                        job["institute"] = el.get_text(strip=True)
                        break

            # Deadline if missing
            if not job.get("deadline"):
                for selector in (
                    "th:contains('마감') + td",
                    "th:contains('접수기간') + td",
                    "span.deadline",
                ):
                    el = soup.select_one(selector)
                    if el:
                        job["deadline"] = self._parse_korean_date(
                            el.get_text(strip=True)
                        )
                        break

        except Exception:
            self.logger.debug("Could not enrich detail page: %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for query in SEARCH_QUERIES:
            keyword = query["keyword"]
            self.logger.info("Searching jobs.ac.kr for: %s", keyword)

            for page in range(MAX_PAGES):
                try:
                    params = {
                        "keyword": keyword,
                        "page": str(page + 1),
                    }
                    resp = self.fetch(SEARCH_URL, params=params)
                    page_jobs = self._parse_listing_page(resp.text)

                    if not page_jobs:
                        break

                    self.logger.info(
                        "'%s' page %d: %d results", keyword, page + 1, len(page_jobs)
                    )
                    all_jobs.extend(page_jobs)
                except Exception:
                    self.logger.exception(
                        "Failed to fetch jobs.ac.kr for '%s' page %d",
                        keyword,
                        page + 1,
                    )
                    break

        # Enrich a subset with detail pages (limit to avoid hammering)
        enriched: list[dict[str, Any]] = []
        for job in all_jobs[:50]:
            job = self._enrich_from_detail(job)
            enriched.append(job)

        # Add remaining without enrichment
        enriched.extend(all_jobs[50:])

        self.logger.info("Total jobs.ac.kr jobs: %d", len(enriched))
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _parse_korean_date(text: str) -> str | None:
        """Parse Korean-style dates like '2024.03.15' or '2024년 3월 15일'."""
        if not text:
            return None
        text = text.strip()

        # 2024.03.15 or 2024-03-15
        m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # 2024년 3월 15일
        m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
        if m:
            try:
                dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return None
