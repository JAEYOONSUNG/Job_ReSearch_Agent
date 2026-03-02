"""Aggregated Korean academic job scraper.

Scrapes five Korean research job portals for postdoc, researcher,
and faculty positions in life sciences:

- IBRIC BioJob  (ibric.org)
- HiBrainNet    (hibrain.net)
- IBS           (ibs.re.kr)
- KIST          (kist.re.kr)
- RPiK / NRF    (rpik.or.kr)
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlencode

from bs4 import BeautifulSoup, Tag

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

MAX_PAGES = 5
DETAIL_LIMIT = 30  # max detail pages to fetch per sub-scraper


class KoreanJobsScraper(BaseScraper):
    """Aggregate scraper for Korean academic/research job portals."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "korean_jobs"

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        sub_scrapers = [
            ("IBRIC", self._scrape_ibric),
            ("HiBrainNet", self._scrape_hibrain),
            ("IBS", self._scrape_ibs),
            ("KIST", self._scrape_kist),
            ("RPiK", self._scrape_rpik),
        ]

        for label, fn in sub_scrapers:
            try:
                jobs = fn()
                self.logger.info("%s: found %d jobs", label, len(jobs))
                all_jobs.extend(jobs)
            except Exception:
                self.logger.exception("%s sub-scraper failed", label)

        self.logger.info("Total Korean jobs: %d", len(all_jobs))
        return all_jobs

    # ── IBRIC BioJob ──────────────────────────────────────────────────────

    def _scrape_ibric(self) -> list[dict[str, Any]]:
        """Scrape IBRIC BioJob (ibric.org) for postdoc/researcher positions."""
        base = "https://www.ibric.org/bric/biojob/recruit.do"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                resp = self.fetch(f"{base}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.list-item, "
                    "ul.recruit-list li"
                )
                if not rows:
                    # Fallback: look for links matching detail patterns
                    rows = self._find_job_links(soup, base, r"recruit.*do.*id=\d+|detail|view")

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, base, "ibric")
                    if job:
                        jobs.append(job)

            except Exception:
                self.logger.debug("IBRIC page %d failed", page)
                break

        return self._enrich_korean_detail(jobs)

    # ── HiBrainNet ────────────────────────────────────────────────────────

    def _scrape_hibrain(self) -> list[dict[str, Any]]:
        """Scrape HiBrainNet (hibrain.net) for professor/postdoc positions."""
        base = "https://www.hibrain.net/recruitment/recruits"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                resp = self.fetch(f"{base}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.recruit-item, "
                    "ul.recruit-list li, "
                    "div.list-item"
                )
                if not rows:
                    rows = self._find_job_links(
                        soup, base, r"recruit|detail|view"
                    )

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, base, "hibrain")
                    if job:
                        jobs.append(job)

            except Exception:
                self.logger.debug("HiBrainNet page %d failed", page)
                break

        return self._enrich_korean_detail(jobs)

    # ── IBS ────────────────────────────────────────────────────────────────

    def _scrape_ibs(self) -> list[dict[str, Any]]:
        """Scrape IBS (ibs.re.kr) for research fellow/staff positions."""
        base = "https://www.ibs.re.kr/prog/recruit/eng/sub04_01/list.do"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"pageIndex": str(page)}
                resp = self.fetch(f"{base}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.board-list tbody tr, "
                    "ul.list li"
                )
                if not rows:
                    rows = self._find_job_links(
                        soup, "https://www.ibs.re.kr", r"recruit|view|detail"
                    )

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(
                        row, "https://www.ibs.re.kr", "ibs"
                    )
                    if job:
                        job["institute"] = job.get("institute") or "Institute for Basic Science (IBS)"
                        jobs.append(job)

            except Exception:
                self.logger.debug("IBS page %d failed", page)
                break

        return self._enrich_korean_detail(jobs)

    # ── KIST ───────────────────────────────────────────────────────────────

    def _scrape_kist(self) -> list[dict[str, Any]]:
        """Scrape KIST recruitment portal for researcher positions."""
        base = "https://kist.re.kr/eng/recruit/announce-list.do"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                resp = self.fetch(f"{base}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.board-list tbody tr, "
                    "ul.list li, "
                    "div.list-item"
                )
                if not rows:
                    rows = self._find_job_links(
                        soup, "https://kist.re.kr", r"recruit|announce|view|detail"
                    )

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(
                        row, "https://kist.re.kr", "kist"
                    )
                    if job:
                        job["institute"] = job.get("institute") or "Korea Institute of Science and Technology (KIST)"
                        jobs.append(job)

            except Exception:
                self.logger.debug("KIST page %d failed", page)
                break

        return self._enrich_korean_detail(jobs)

    # ── RPiK (NRF) ────────────────────────────────────────────────────────

    def _scrape_rpik(self) -> list[dict[str, Any]]:
        """Scrape RPiK / NRF (rpik.or.kr) for postdoc/professor positions."""
        base = "https://www.rpik.or.kr/eng/sub/recruit/job_notice_list.do"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                resp = self.fetch(f"{base}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.board-list tbody tr, "
                    "ul.list li, "
                    "div.list-item"
                )
                if not rows:
                    rows = self._find_job_links(
                        soup, "https://www.rpik.or.kr", r"recruit|notice|view|detail"
                    )

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(
                        row, "https://www.rpik.or.kr", "rpik"
                    )
                    if job:
                        jobs.append(job)

            except Exception:
                self.logger.debug("RPiK page %d failed", page)
                break

        return self._enrich_korean_detail(jobs)

    # ── Shared helpers ─────────────────────────────────────────────────────

    def _find_job_links(
        self, soup: BeautifulSoup, base_url: str, pattern: str
    ) -> list[Tag]:
        """Fallback: find all links that look like job detail pages."""
        results: list[Tag] = []
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if re.search(pattern, href) and a.get_text(strip=True):
                results.append(a)
        return results

    def _parse_generic_row(
        self, row: Tag, base_url: str, sub_source: str
    ) -> dict[str, Any] | None:
        """Parse a table row or list item into a job dict."""
        # Get the primary link
        if row.name == "a":
            link = row
        else:
            link = row.select_one("a[href]")
        if not link:
            return None

        title = link.get_text(strip=True)
        if not title or len(title) < 3:
            return None

        href = link.get("href", "")
        url = urljoin(base_url, href) if href else None
        if not url:
            return None

        # Institute
        inst_el = row.select_one(
            "td.company, td.institute, span.company, span.institute, "
            "div.company, .org-name, td:nth-child(2), td:nth-child(3)"
        )
        institute = None
        if inst_el and inst_el != link:
            inst_text = inst_el.get_text(strip=True)
            if inst_text and inst_text != title:
                institute = inst_text

        # Department
        dept_el = row.select_one(
            "td.department, span.department, div.dept, td.field"
        )
        department = dept_el.get_text(strip=True) if dept_el else None

        # Deadline
        date_el = row.select_one(
            "td.date, td.deadline, span.date, span.deadline, "
            ".period, td:last-child"
        )
        deadline = None
        if date_el:
            date_text = date_el.get_text(strip=True)
            deadline = _parse_korean_date(date_text)

        # Field
        field_el = row.select_one("td.field, td.category, span.category")
        field = field_el.get_text(strip=True) if field_el else None

        return {
            "title": title,
            "institute": institute,
            "department": department,
            "country": "South Korea",
            "url": url,
            "deadline": deadline,
            "field": field,
            "source": f"korean_jobs:{sub_source}",
        }

    def _enrich_korean_detail(
        self, jobs: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Fetch detail pages for the top N jobs to get full descriptions."""
        enriched: list[dict[str, Any]] = []

        for job in jobs[:DETAIL_LIMIT]:
            url = job.get("url")
            if not url:
                enriched.append(job)
                continue
            try:
                resp = self.fetch(url)
                soup = BeautifulSoup(resp.text, "html.parser")

                # Description: try common detail selectors
                desc_el = soup.select_one(
                    "div.content, div.view-content, div.board-view, "
                    "div.detail-content, div.recruit-view, "
                    "article, div.job-detail, div.bbs-view"
                )
                if desc_el:
                    job["description"] = desc_el.get_text(
                        separator=" ", strip=True
                    )[:3000]

                # Institute if missing
                if not job.get("institute"):
                    for sel in (
                        "th:contains('기관') + td",
                        "th:contains('대학') + td",
                        "th:contains('연구소') + td",
                        "span.company",
                        "div.company-name",
                    ):
                        el = soup.select_one(sel)
                        if el:
                            job["institute"] = el.get_text(strip=True)
                            break

                # Deadline if missing
                if not job.get("deadline"):
                    for sel in (
                        "th:contains('마감') + td",
                        "th:contains('접수기간') + td",
                        "th:contains('Deadline') + td",
                        "span.deadline",
                    ):
                        el = soup.select_one(sel)
                        if el:
                            parsed = _parse_korean_date(el.get_text(strip=True))
                            if parsed:
                                job["deadline"] = parsed
                                break

            except Exception:
                self.logger.debug("Detail fetch failed: %s", url)

            enriched.append(job)

        # Add remaining jobs without enrichment
        enriched.extend(jobs[DETAIL_LIMIT:])
        return enriched


# ── Module-level helpers ──────────────────────────────────────────────────


def _parse_korean_date(text: str | None) -> str | None:
    """Parse Korean-style dates: '2026.03.15', '2026-03-15', '2026년 3월 15일'."""
    if not text:
        return None
    text = text.strip()

    # 2026.03.15 or 2026-03-15 or 2026/03/15
    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # 2026년 3월 15일
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None
