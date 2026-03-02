"""Aggregated Korean academic job scraper.

Scrapes Korean research job portals for postdoc, researcher, and faculty
positions in life sciences:

Aggregator portals:
- IBRIC BioJob      (ibric.org)
- HiBrainNet        (hibrain.net)
- RPiK / NRF        (rpik.or.kr)
- NST onest         (onest.recruitment.kr) — centralized 출연연 portal

Government research institutes (출연연):
- IBS               (ibs.re.kr)
- KIST              (kist.re.kr)
- KRIBB             (kribb.re.kr)
- KRICT             (krict.re.kr)
- KBSI              (kbsi.re.kr)
- KIOST             (kiost.ac.kr)
- KFRI              (kfri.re.kr)
- KAERI             (kaeri.re.kr)
- WIKIM             (wikim.re.kr)

Universities:
- KAIST             (kaist.ac.kr)
- SNU               (snu.ac.kr)
- POSTECH           (postech.ac.kr)
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
            # ── Aggregator portals ──
            ("IBRIC", self._scrape_ibric),
            ("HiBrainNet", self._scrape_hibrain),
            ("RPiK", self._scrape_rpik),
            ("NST_onest", self._scrape_nst_onest),
            # ── Government research institutes (출연연) ──
            ("IBS", self._scrape_ibs),
            ("KIST", self._scrape_kist),
            ("KRIBB", self._scrape_kribb),
            ("KRICT", self._scrape_krict),
            ("KBSI", self._scrape_kbsi),
            ("KIOST", self._scrape_kiost),
            ("KFRI", self._scrape_kfri),
            ("KAERI", self._scrape_kaeri),
            ("WIKIM", self._scrape_wikim),
            # ── Universities ──
            ("KAIST", self._scrape_kaist),
            ("SNU", self._scrape_snu),
            ("POSTECH", self._scrape_postech),
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

    # ══════════════════════════════════════════════════════════════════════
    # Aggregator portals
    # ══════════════════════════════════════════════════════════════════════

    def _scrape_ibric(self) -> list[dict[str, Any]]:
        """IBRIC BioJob (ibric.org) — postdoc/researcher positions."""
        return self._scrape_board_site(
            base_url="https://www.ibric.org/bric/biojob/recruit.do",
            page_param="page",
            sub_source="ibric",
            link_pattern=r"recruit.*do.*id=\d+|detail|view",
        )

    def _scrape_hibrain(self) -> list[dict[str, Any]]:
        """HiBrainNet (hibrain.net) — professor/postdoc positions."""
        return self._scrape_board_site(
            base_url="https://www.hibrain.net/recruitment/recruits",
            page_param="page",
            sub_source="hibrain",
            link_pattern=r"recruit|detail|view",
        )

    def _scrape_rpik(self) -> list[dict[str, Any]]:
        """RPiK / NRF (rpik.or.kr) — postdoc/professor positions."""
        return self._scrape_board_site(
            base_url="https://www.rpik.or.kr/eng/sub/recruit/job_notice_list.do",
            page_param="page",
            sub_source="rpik",
            link_pattern=r"recruit|notice|view|detail",
        )

    def _scrape_nst_onest(self) -> list[dict[str, Any]]:
        """NST onest portal (onest.recruitment.kr) — centralized 출연연 recruitment."""
        base = "https://onest.recruitment.kr"
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                # Try the main job listing page
                params = {"page": str(page)}
                resp = self.fetch(f"{base}/app/jobnotice/list?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.recruit-list li, "
                    "ul.list-group li, "
                    "div.job-item, "
                    "div.list-item, "
                    "div.card"
                )
                if not rows:
                    rows = self._find_job_links(soup, base, r"jobnotice|detail|view")

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, base, "nst_onest")
                    if job:
                        jobs.append(job)

            except Exception:
                self.logger.debug("NST onest page %d failed", page)
                break

        # Also try the NST main recruitment page
        try:
            resp = self.fetch("https://www.nst.re.kr/www/sub.do?key=56")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr")
            if not rows:
                rows = self._find_job_links(soup, "https://www.nst.re.kr", r"sub\.do|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.nst.re.kr", "nst")
                if job:
                    job["institute"] = job.get("institute") or "National Research Council of Science & Technology (NST)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("NST main page failed")

        return self._enrich_korean_detail(jobs)

    # ══════════════════════════════════════════════════════════════════════
    # Government research institutes (출연연)
    # ══════════════════════════════════════════════════════════════════════

    def _scrape_ibs(self) -> list[dict[str, Any]]:
        """IBS (ibs.re.kr) — research fellow/staff positions."""
        return self._scrape_board_site(
            base_url="https://www.ibs.re.kr/prog/recruit/eng/sub04_01/list.do",
            page_param="pageIndex",
            sub_source="ibs",
            default_institute="Institute for Basic Science (IBS)",
            link_pattern=r"recruit|view|detail",
            site_root="https://www.ibs.re.kr",
        )

    def _scrape_kist(self) -> list[dict[str, Any]]:
        """KIST (kist.re.kr) — researcher positions."""
        return self._scrape_board_site(
            base_url="https://kist.re.kr/eng/recruit/announce-list.do",
            page_param="page",
            sub_source="kist",
            default_institute="Korea Institute of Science and Technology (KIST)",
            link_pattern=r"recruit|announce|view|detail",
            site_root="https://kist.re.kr",
        )

    def _scrape_kribb(self) -> list[dict[str, Any]]:
        """KRIBB (kribb.re.kr) — bioscience & biotechnology positions."""
        jobs: list[dict[str, Any]] = []

        # Primary: recruitment notice list
        try:
            resp = self.fetch("https://recruit.kribb.re.kr/recruit/notice2/list.aspx")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul li.item")
            if not rows:
                rows = self._find_job_links(soup, "https://recruit.kribb.re.kr", r"notice|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://recruit.kribb.re.kr", "kribb")
                if job:
                    job["institute"] = job.get("institute") or "Korea Research Institute of Bioscience and Biotechnology (KRIBB)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KRIBB recruitment page failed")

        # Fallback: recruitment.kr portal
        try:
            resp = self.fetch("https://kribb.recruitment.kr/app/jobnotice/list")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, div.card, ul li.item")
            if not rows:
                rows = self._find_job_links(soup, "https://kribb.recruitment.kr", r"jobnotice|detail|view")
            for row in rows:
                job = self._parse_generic_row(row, "https://kribb.recruitment.kr", "kribb")
                if job:
                    job["institute"] = job.get("institute") or "Korea Research Institute of Bioscience and Biotechnology (KRIBB)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KRIBB recruitment.kr failed")

        return self._enrich_korean_detail(jobs)

    def _scrape_krict(self) -> list[dict[str, Any]]:
        """KRICT (krict.re.kr) — chemical technology positions."""
        jobs: list[dict[str, Any]] = []

        # Primary: official site recruitment board
        try:
            resp = self.fetch("https://www.krict.re.kr/prog/jobOffer/kor/sub04_04_02_01/list.do")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.krict.re.kr", r"jobOffer|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.krict.re.kr", "krict")
                if job:
                    job["institute"] = job.get("institute") or "Korea Research Institute of Chemical Technology (KRICT)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KRICT official page failed")

        # Fallback: recruitment.kr portal
        jobs.extend(self._scrape_recruitment_kr_portal(
            "https://krict.recruitment.kr/app/jobnotice/list",
            "krict",
            "Korea Research Institute of Chemical Technology (KRICT)",
        ))

        return self._enrich_korean_detail(jobs)

    def _scrape_kbsi(self) -> list[dict[str, Any]]:
        """KBSI (kbsi.re.kr) — basic science positions."""
        jobs: list[dict[str, Any]] = []

        # Primary: official site
        try:
            resp = self.fetch("https://www.kbsi.re.kr/recruit0102")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul li.item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.kbsi.re.kr", r"recruit|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.kbsi.re.kr", "kbsi")
                if job:
                    job["institute"] = job.get("institute") or "Korea Basic Science Institute (KBSI)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KBSI official page failed")

        # Fallback: recruiter.co.kr portal
        jobs.extend(self._scrape_recruiter_portal(
            "https://kbsi.recruiter.co.kr/app/jobnotice/list",
            "kbsi",
            "Korea Basic Science Institute (KBSI)",
        ))

        return self._enrich_korean_detail(jobs)

    def _scrape_kiost(self) -> list[dict[str, Any]]:
        """KIOST (kiost.ac.kr) — ocean science & technology positions."""
        jobs: list[dict[str, Any]] = []

        try:
            resp = self.fetch(
                "https://www.kiost.ac.kr/cop/bbs/BBSMSTR_000000000073/selectBoardList.do"
            )
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.kiost.ac.kr", r"selectBoard|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.kiost.ac.kr", "kiost")
                if job:
                    job["institute"] = job.get("institute") or "Korea Institute of Ocean Science and Technology (KIOST)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KIOST board page failed")

        return self._enrich_korean_detail(jobs)

    def _scrape_kfri(self) -> list[dict[str, Any]]:
        """KFRI (kfri.re.kr) — food research positions."""
        jobs: list[dict[str, Any]] = []

        # Official site
        try:
            resp = self.fetch("https://www.kfri.re.kr/web/board/13/")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul li.item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.kfri.re.kr", r"board|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.kfri.re.kr", "kfri")
                if job:
                    job["institute"] = job.get("institute") or "Korea Food Research Institute (KFRI)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KFRI official page failed")

        # Fallback: recruiter.co.kr portal
        jobs.extend(self._scrape_recruiter_portal(
            "https://kfri.recruiter.co.kr/app/jobnotice/list",
            "kfri",
            "Korea Food Research Institute (KFRI)",
        ))

        return self._enrich_korean_detail(jobs)

    def _scrape_kaeri(self) -> list[dict[str, Any]]:
        """KAERI (kaeri.re.kr) — atomic energy / radiation biology positions."""
        jobs: list[dict[str, Any]] = []

        try:
            resp = self.fetch("https://www.kaeri.re.kr/board?menuId=MENU00428")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul.board-list li")
            if not rows:
                rows = self._find_job_links(soup, "https://www.kaeri.re.kr", r"board|view|detail")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.kaeri.re.kr", "kaeri")
                if job:
                    job["institute"] = job.get("institute") or "Korea Atomic Energy Research Institute (KAERI)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KAERI board page failed")

        return self._enrich_korean_detail(jobs)

    def _scrape_wikim(self) -> list[dict[str, Any]]:
        """WIKIM / 세계김치연구소 (wikim.re.kr) — food fermentation research."""
        jobs: list[dict[str, Any]] = []

        try:
            resp = self.fetch("https://www.wikim.re.kr/menu.es?mid=a10403020000")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul li.item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.wikim.re.kr", r"menu\.es|view|detail|bbs")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.wikim.re.kr", "wikim")
                if job:
                    job["institute"] = job.get("institute") or "World Institute of Kimchi (WIKIM)"
                    jobs.append(job)
        except Exception:
            self.logger.debug("WIKIM page failed")

        return self._enrich_korean_detail(jobs)

    # ══════════════════════════════════════════════════════════════════════
    # Universities
    # ══════════════════════════════════════════════════════════════════════

    def _scrape_kaist(self) -> list[dict[str, Any]]:
        """KAIST recruitment — faculty & researcher positions."""
        jobs: list[dict[str, Any]] = []

        # Faculty recruitment page
        try:
            resp = self.fetch("https://www.kaist.ac.kr/kr/html/footer/0811.html")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, div.list-item, ul li")
            if not rows:
                rows = self._find_job_links(soup, "https://www.kaist.ac.kr", r"footer|view|detail|recruit")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.kaist.ac.kr", "kaist")
                if job:
                    job["institute"] = job.get("institute") or "KAIST"
                    jobs.append(job)
        except Exception:
            self.logger.debug("KAIST faculty page failed")

        # Staff/researcher recruitment portal
        jobs.extend(self._scrape_recruitment_kr_portal(
            "https://kaist.recruitment.kr/app/jobnotice/list",
            "kaist",
            "KAIST",
        ))

        return self._enrich_korean_detail(jobs)

    def _scrape_snu(self) -> list[dict[str, Any]]:
        """SNU (snu.ac.kr) — job openings."""
        jobs: list[dict[str, Any]] = []

        try:
            resp = self.fetch("https://www.snu.ac.kr/snunow/notice/job-openings")
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select("table tbody tr, ul.board-list li, div.list-item")
            if not rows:
                rows = self._find_job_links(soup, "https://www.snu.ac.kr", r"notice|view|detail|job")
            for row in rows:
                job = self._parse_generic_row(row, "https://www.snu.ac.kr", "snu")
                if job:
                    job["institute"] = job.get("institute") or "Seoul National University"
                    jobs.append(job)
        except Exception:
            self.logger.debug("SNU job openings page failed")

        return self._enrich_korean_detail(jobs)

    def _scrape_postech(self) -> list[dict[str, Any]]:
        """POSTECH — recruitment positions."""
        jobs: list[dict[str, Any]] = []

        # recruiter.co.kr portal
        jobs.extend(self._scrape_recruiter_portal(
            "https://postech.recruiter.co.kr/app/jobnotice/list",
            "postech",
            "POSTECH",
        ))

        return self._enrich_korean_detail(jobs)

    # ══════════════════════════════════════════════════════════════════════
    # Shared helpers
    # ══════════════════════════════════════════════════════════════════════

    def _scrape_board_site(
        self,
        base_url: str,
        page_param: str,
        sub_source: str,
        default_institute: str | None = None,
        link_pattern: str = r"view|detail",
        site_root: str | None = None,
    ) -> list[dict[str, Any]]:
        """Generic paginated board scraper used by many Korean institute sites."""
        root = site_root or base_url.rsplit("/", 1)[0]
        jobs: list[dict[str, Any]] = []

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {page_param: str(page)}
                resp = self.fetch(f"{base_url}?{urlencode(params)}")
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.board-list tbody tr, "
                    "div.list-item, "
                    "ul.list li, "
                    "div.recruit-item, "
                    "ul.recruit-list li"
                )
                if not rows:
                    rows = self._find_job_links(soup, root, link_pattern)

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, root, sub_source)
                    if job:
                        if default_institute:
                            job["institute"] = job.get("institute") or default_institute
                        jobs.append(job)

            except Exception:
                self.logger.debug("%s page %d failed", sub_source, page)
                break

        return self._enrich_korean_detail(jobs)

    def _scrape_recruitment_kr_portal(
        self,
        url: str,
        sub_source: str,
        default_institute: str,
    ) -> list[dict[str, Any]]:
        """Scrape a *.recruitment.kr portal (shared system used by many 출연연)."""
        jobs: list[dict[str, Any]] = []
        base = url.rsplit("/app/", 1)[0] if "/app/" in url else url.rsplit("/", 1)[0]

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                full_url = f"{url}?{urlencode(params)}" if "?" not in url else f"{url}&{urlencode(params)}"
                resp = self.fetch(full_url)
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.list-item, "
                    "div.card, "
                    "ul li.item, "
                    "div.job-item"
                )
                if not rows:
                    rows = self._find_job_links(soup, base, r"jobnotice|detail|view")

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, base, sub_source)
                    if job:
                        job["institute"] = job.get("institute") or default_institute
                        jobs.append(job)

            except Exception:
                self.logger.debug("%s recruitment.kr page %d failed", sub_source, page)
                break

        return jobs

    def _scrape_recruiter_portal(
        self,
        url: str,
        sub_source: str,
        default_institute: str,
    ) -> list[dict[str, Any]]:
        """Scrape a *.recruiter.co.kr portal (shared system used by many 출연연)."""
        jobs: list[dict[str, Any]] = []
        base = url.rsplit("/app/", 1)[0] if "/app/" in url else url.rsplit("/", 1)[0]

        for page in range(1, MAX_PAGES + 1):
            try:
                params = {"page": str(page)}
                full_url = f"{url}?{urlencode(params)}" if "?" not in url else f"{url}&{urlencode(params)}"
                resp = self.fetch(full_url)
                soup = BeautifulSoup(resp.text, "html.parser")

                rows = soup.select(
                    "table tbody tr, "
                    "div.list-item, "
                    "div.card, "
                    "ul li.item, "
                    "div.job-item"
                )
                if not rows:
                    rows = self._find_job_links(soup, base, r"jobnotice|detail|view")

                if not rows and page > 1:
                    break

                for row in rows:
                    job = self._parse_generic_row(row, base, sub_source)
                    if job:
                        job["institute"] = job.get("institute") or default_institute
                        jobs.append(job)

            except Exception:
                self.logger.debug("%s recruiter.co.kr page %d failed", sub_source, page)
                break

        return jobs

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
