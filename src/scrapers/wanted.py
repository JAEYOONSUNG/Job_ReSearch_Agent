"""Wanted (원티드) scraper - Korean job platform for research/biotech positions."""

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

BASE_URL = "https://www.wanted.co.kr"

# Wanted has a public API for job listings
# Tag IDs for relevant categories on Wanted:
#   - 10110: Biotech/제약/바이오
#   - 10111: Research/연구
# We also search by keywords
WANTED_API_URL = "https://www.wanted.co.kr/api/v4/jobs"

# Search queries covering research + postdoc roles
SEARCH_QUERIES = [
    "연구원 바이오",            # researcher + bio
    "연구원 생명과학",          # researcher + life sciences
    "포스닥",                   # postdoc
    "postdoc",
    "연구원 합성생물학",       # researcher + synthetic biology
    "연구원 단백질",           # researcher + protein
    "연구원 유전체",           # researcher + genomics
    "바이오텍 연구",           # biotech research
    "researcher biology",
    "연구원 화학생물학",       # researcher + chemical biology
]

# Wanted tag IDs for filtering (biotech/pharma category)
WANTED_TAG_IDS = [518, 10110, 10111]

MAX_RESULTS_PER_QUERY = 20


class WantedScraper(BaseScraper):
    """Scrape Wanted (wanted.co.kr) for biotech and research positions in Korea."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "wanted"

    # ── API-based scraping ────────────────────────────────────────────────

    def _search_api(self, query: str, offset: int = 0) -> list[dict[str, Any]]:
        """Use Wanted's public API to search for jobs."""
        jobs: list[dict[str, Any]] = []
        try:
            params = {
                "country": "kr",
                "tag_type_ids": "518",  # Bio/Pharma
                "job_sort": "job.latest_order",
                "locations": "all",
                "years": "-1",
                "limit": str(MAX_RESULTS_PER_QUERY),
                "offset": str(offset),
                "query": query,
            }
            resp = self.fetch(
                WANTED_API_URL,
                params=params,
                headers={
                    "Accept": "application/json",
                    "Referer": f"{BASE_URL}/search?query={query}",
                },
            )

            data = resp.json()
            job_list = data.get("data", [])
            if not job_list:
                return jobs

            for item in job_list:
                job = self._map_api_item(item)
                if job:
                    jobs.append(job)

        except Exception:
            self.logger.debug("Wanted API failed for query '%s'", query)

        return jobs

    def _map_api_item(self, item: dict[str, Any]) -> dict[str, Any] | None:
        """Map a Wanted API response item to our job schema."""
        job_id = item.get("id")
        if not job_id:
            return None

        company = item.get("company", {})
        title = item.get("position", "")
        if not title:
            return None

        company_name = company.get("name", "")
        industry = company.get("industry_name", "")

        # Build the job URL
        url = f"{BASE_URL}/wd/{job_id}"

        return {
            "title": title,
            "institute": company_name,
            "country": "South Korea",
            "url": url,
            "field": industry,
            "source": self.name,
        }

    # ── Web scraping fallback ─────────────────────────────────────────────

    def _search_web(self, query: str) -> list[dict[str, Any]]:
        """Fallback: scrape the Wanted website directly."""
        jobs: list[dict[str, Any]] = []
        try:
            search_url = f"{BASE_URL}/search"
            params = {"query": query, "tab": "position"}
            resp = self.fetch(search_url, params=params)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Wanted renders job cards
            cards = soup.select(
                "div.JobCard_container, "
                "a[href*='/wd/'], "
                "div[data-cy='job-card'], "
                "li.Card_container"
            )

            for card in cards:
                job = self._parse_card(card)
                if job:
                    jobs.append(job)

        except Exception:
            self.logger.debug("Wanted web scrape failed for '%s'", query)

        return jobs

    def _parse_card(self, card: Tag) -> dict[str, Any] | None:
        """Parse a job card from the Wanted search page."""
        # Find the link
        link = card if card.name == "a" else card.select_one("a[href*='/wd/']")
        if not link:
            return None

        href = link.get("href", "")
        if not href:
            return None
        url = urljoin(BASE_URL, href)

        # Title
        title_el = card.select_one(
            "p.JobCard_title, span.job-card-title, "
            "strong.position, h3, p:first-child"
        )
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        if not title:
            return None

        # Company
        company_el = card.select_one(
            "p.JobCard_company, span.company-name, "
            "span.job-card-company, h4"
        )
        company = company_el.get_text(strip=True) if company_el else None

        return {
            "title": title,
            "institute": company,
            "country": "South Korea",
            "url": url,
            "source": self.name,
        }

    # ── Detail page enrichment ────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the Wanted job detail page to get full description."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Description
            desc_el = soup.select_one(
                "div.JobDescription_JobDescription, "
                "section.JobDetail_JobDetail, "
                "div.job-detail, "
                "div[class*='JobDescription']"
            )
            if desc_el:
                job["description"] = desc_el.get_text(separator=" ", strip=True)[:2000]

            # Deadline (Wanted jobs usually don't have deadlines, but check)
            deadline_el = soup.select_one("span.deadline, p.deadline")
            if deadline_el:
                raw = deadline_el.get_text(strip=True)
                m = re.search(r"\d{4}[.\-/]\d{1,2}[.\-/]\d{1,2}", raw)
                if m:
                    job["deadline"] = m.group(0).replace(".", "-").replace("/", "-")

        except Exception:
            self.logger.debug("Could not enrich Wanted detail: %s", url)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for query in SEARCH_QUERIES:
            self.logger.info("Wanted search: %s", query)

            # Try API first, fall back to web scraping
            api_jobs = self._search_api(query)
            if api_jobs:
                all_jobs.extend(api_jobs)
                self.logger.info(
                    "Wanted API '%s': %d results", query, len(api_jobs)
                )
            else:
                web_jobs = self._search_web(query)
                all_jobs.extend(web_jobs)
                self.logger.info(
                    "Wanted web '%s': %d results", query, len(web_jobs)
                )

        # Keyword filter
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
            "Wanted: %d total, %d after filter", len(all_jobs), len(enriched)
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        # Korean keywords that indicate research roles
        kr_keywords = [
            "연구", "바이오", "생명", "화학", "유전", "단백질",
            "합성생물", "postdoc", "포스닥", "박사후",
        ]
        if any(kw in lower for kw in kr_keywords):
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)
