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
DETAIL_LIMIT = 50  # max detail pages to fetch per sub-scraper

# ── Site-specific CSS selector hints (tried first, in order) ──
_SITE_HINTS: dict[str, list[str]] = {
    "ibric":     ["div.view-content", "div.board-view", "div#content div.content", "td.content"],
    "hibrain":   ["div.recruit-view", "div.view-body", "div.content-view", "div#content"],
    "rpik":      ["div.board-view", "div.view-content"],
    "nst_onest": ["div.detail-content", "div.view-content", "div.job-detail"],
    "nst":       ["div.board-view", "div.view-content"],
    "ibs":       ["div.recruit-view", "div.board-view", "div.view-content", "div#content"],
    "kist":      ["div.board-view", "div.view-content", "div.content-view"],
    "kribb":     ["div.view-content", "div.board-view", "div#content"],
    "krict":     ["div.board-view", "div.view-content"],
    "kbsi":      ["div.board-view", "div.view-content"],
    "kiost":     ["div.board-view", "div.bbs-view", "div.view-content"],
    "kfri":      ["div.board-view", "div.view-content"],
    "kaeri":     ["div.board-view", "div.view-content", "div.bbs-view"],
    "wikim":     ["div.board-view", "div.bbs-view", "div.view-content"],
    "kaist":     ["div.detail-content", "div.view-content", "div.job-detail"],
    "snu":       ["div.board-view", "div.view-content", "article.board-view"],
    "postech":   ["div.detail-content", "div.view-content", "div.job-detail"],
}

# ── Generic Korean government board selectors (fallback after site hints) ──
_KOREAN_BOARD_SELECTORS = [
    "div.board-view", "div.view-content", "div.bbs-view",
    "div.detail-content", "div.recruit-view", "div.content-view",
    "div#content div.content", "td.content", "article",
    "div.job-detail", "div#content",
    "div[class*='view']", "div[class*='content']", "div[class*='detail']",
]

# ── Default institute names per source ──
_DEFAULT_INSTITUTES: dict[str, str] = {
    "ibs": "Institute for Basic Science (IBS)",
    "kist": "Korea Institute of Science and Technology (KIST)",
    "kribb": "Korea Research Institute of Bioscience and Biotechnology (KRIBB)",
    "krict": "Korea Research Institute of Chemical Technology (KRICT)",
    "kbsi": "Korea Basic Science Institute (KBSI)",
    "kiost": "Korea Institute of Ocean Science and Technology (KIOST)",
    "kfri": "Korea Food Research Institute (KFRI)",
    "kaeri": "Korea Atomic Energy Research Institute (KAERI)",
    "wikim": "World Institute of Kimchi (WIKIM)",
    "nst": "National Research Council of Science & Technology (NST)",
    "kaist": "KAIST",
    "snu": "Seoul National University",
    "postech": "POSTECH",
}


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
        """HiBrainNet (hibrain.net) — professor/postdoc positions.

        HiBrainNet lists ALL professional positions (banks, government,
        factories, etc.), so we filter to bio/research-relevant titles.
        """
        jobs = self._scrape_board_site(
            base_url="https://www.hibrain.net/recruitment/recruits",
            page_param="page",
            sub_source="hibrain",
            link_pattern=r"recruit|detail|view",
        )
        pre = len(jobs)
        jobs = [j for j in jobs if _is_bio_relevant_korean(j.get("title", ""))]
        if pre > len(jobs):
            self.logger.info("HiBrainNet bio filter: %d → %d", pre, len(jobs))
        return jobs

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

        # NST main recruitment boards (bbsNo=15: NST자체, bbsNo=19: 출연연 공동)
        for bbs_no, key in [("15", "56"), ("19", "61")]:
            try:
                list_url = (
                    f"https://www.nst.re.kr/www/selectBbsNttList.do"
                    f"?bbsNo={bbs_no}&key={key}"
                )
                resp = self.fetch(list_url)
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select("table tbody tr")
                if not rows:
                    rows = self._find_job_links(
                        soup, list_url, r"selectBbs|View|view|detail"
                    )
                for row in rows:
                    job = self._parse_generic_row(row, list_url, "nst")
                    if job:
                        job["institute"] = job.get("institute") or (
                            "National Research Council of Science & Technology (NST)"
                        )
                        jobs.append(job)
            except Exception:
                self.logger.debug("NST bbsNo=%s page failed", bbs_no)

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
            base_url="https://kist.re.kr/ko/notice/employment-announcement.do",
            page_param="page",
            sub_source="kist",
            default_institute="Korea Institute of Science and Technology (KIST)",
            link_pattern=r"notice|view|detail|announcement",
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
            resp = self.fetch("https://www.kbsi.re.kr/board?menuId=MENU002051102000000&boardId=BOARD00086")
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
            resp = self.fetch("https://www.kfri.re.kr/web/board/13/postList")
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

        # Staff/researcher recruitment portal (primary — structured job listings)
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
                page_url = f"{base_url}?{urlencode(params)}"
                resp = self.fetch(page_url)
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
                    # Use page_url (not root) so relative hrefs like
                    # "?mode=view&id=123" resolve correctly.
                    job = self._parse_generic_row(row, page_url, sub_source)
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
        # Get the primary link (prefer one with text content)
        if row.name == "a":
            link = row
        else:
            link = None
            for a in row.select("a[href]"):
                if a.get_text(strip=True):
                    link = a
                    break
            # Fallback: any <a> with href (even if text is empty)
            if not link:
                link = row.select_one("a[href]")

        # Fallback: extract URL from onclick on <a> or <tr>
        href = ""
        if link:
            href = link.get("href", "")
            # Some Korean sites use href="#view" with real URL in onclick
            if not href or href in ("#", "#view", "javascript:void(0)", "javascript:;"):
                onclick = link.get("onclick", "") or row.get("onclick", "")
                onclick_href = _extract_onclick_href(onclick)
                if onclick_href:
                    href = onclick_href
        elif row.get("onclick"):
            # No <a> tag at all — URL is on the <tr> onclick
            onclick_href = _extract_onclick_href(row["onclick"])
            if onclick_href:
                href = onclick_href
                link = None  # title will come from td text below

        # Fallback: form-based navigation (e.g. KIOST uses <form action=URL>
        # with <input type="submit" value="TITLE">)
        if not link and not href:
            form = row.select_one("form[action]")
            if form:
                action = form.get("action", "")
                # Strip jsessionid from action URL
                action = re.sub(r";jsessionid=[^?]*", "", action)
                if action:
                    href = action
                submit = form.select_one("input[type='submit']")
                if submit and submit.get("value"):
                    link = submit  # use submit button as "link" for title

        # Determine title
        if link:
            title = link.get("value", "") or link.get_text(strip=True)
        else:
            title = ""

        # If primary title is missing or invalid, try alternative sources
        title = re.sub(r"^종료\s*", "", (title or "")).strip()
        title = re.sub(r"새글$|첨부파일\s*있음$|new$", "", title, flags=re.IGNORECASE).strip()

        if not title or len(title) < 3 or not _is_valid_korean_job_title(title):
            # Try form submit button (KIOST pattern)
            form = row.select_one("form[action]")
            if form:
                submit = form.select_one("input[type='submit']")
                if submit and submit.get("value"):
                    title = submit["value"].strip()
                if not href:
                    action = form.get("action", "")
                    action = re.sub(r";jsessionid=[^?]*", "", action)
                    if action:
                        href = action
            # Try longest td text
            if not title or len(title) < 3 or not _is_valid_korean_job_title(title):
                for td in row.select("td"):
                    t = td.get_text(strip=True)
                    if (len(t) > len(title or "")
                            and not _looks_like_date(t)
                            and not t.isdigit()
                            and _is_valid_korean_job_title(t)):
                        title = t

        if not title or len(title) < 3:
            return None

        # Strip "종료" / "진행중" prefix
        title = re.sub(r"^(?:종료|진행중)\s*", "", title).strip()
        # Strip trailing "새글" / "첨부파일 있음" noise
        title = re.sub(r"새글$|첨부파일\s*있음$|new$", "", title, flags=re.IGNORECASE).strip()

        # Reject invalid titles (navigation, results, non-job)
        if not _is_valid_korean_job_title(title):
            return None

        url = urljoin(base_url, href) if href else None
        if not url:
            return None

        # Reject non-content URLs (javascript, anchors, login, etc.)
        if any(x in url.lower() for x in ("javascript:", "mailto:", "login", "signin", "#")):
            return None

        # Institute — use class-specific selectors only (not positional td:nth-child)
        inst_el = row.select_one(
            "td.company, td.institute, span.company, span.institute, "
            "div.company, .org-name"
        )
        institute = None
        if inst_el and inst_el != link:
            inst_text = inst_el.get_text(strip=True)
            # Reject if it looks like a date (e.g., "2026. 2. 27.")
            if inst_text and inst_text != title and not _looks_like_date(inst_text):
                institute = inst_text

        # Department
        dept_el = row.select_one(
            "td.department, span.department, div.dept"
        )
        department = dept_el.get_text(strip=True) if dept_el else None

        # Deadline — try class-specific selectors first
        date_el = row.select_one(
            "td.date, td.deadline, span.date, span.deadline, .period"
        )
        deadline = None
        if date_el:
            date_text = date_el.get_text(strip=True)
            deadline = _parse_korean_date(date_text)

        # Fallback: scan all <td> elements for date-like text (last date = deadline)
        if not deadline:
            for td in reversed(row.select("td")):
                if td == link or td == inst_el:
                    continue
                td_text = td.get_text(strip=True)
                if len(td_text) > 30:
                    continue  # Too long to be a date cell
                parsed = _parse_korean_date(td_text)
                if parsed:
                    deadline = parsed
                    break

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
        """Fetch detail pages for the top N jobs to get full descriptions
        and structured Korean fields (department, PI, requirements, etc.)."""
        enriched: list[dict[str, Any]] = []
        desc_found = 0

        for job in jobs[:DETAIL_LIMIT]:
            url = job.get("url")
            if not url:
                enriched.append(job)
                continue
            try:
                resp = self.fetch(url)
                html = resp.text
                soup = BeautifulSoup(html, "html.parser")
                sub_source = (job.get("source") or "").split(":")[-1]

                # ── RALP: 4-strategy description extraction ──
                desc = self._extract_description_ralp(soup, html, sub_source)
                if desc:
                    job["description"] = desc[:5000]
                    desc_found += 1

                # ── Extract structured fields from Korean table/detail layout ──
                _extract_korean_fields(soup, job)

                # ── Institute fallback ──
                if not job.get("institute"):
                    job["institute"] = _extract_institute_from_page(
                        soup, sub_source
                    )

                # ── Title normalization ──
                job["title"] = _normalize_korean_title(
                    job.get("title", ""),
                    job.get("institute"),
                    job.get("description"),
                )

            except Exception:
                self.logger.debug("Detail fetch failed: %s", url)

            enriched.append(job)

        # Add remaining jobs without enrichment
        enriched.extend(jobs[DETAIL_LIMIT:])
        self.logger.info(
            "Enriched %d/%d jobs, descriptions found: %d",
            min(len(jobs), DETAIL_LIMIT), len(jobs), desc_found,
        )
        return enriched

    def _extract_description_ralp(
        self, soup: BeautifulSoup, html: str, sub_source: str,
        min_len: int = 100,
    ) -> str | None:
        """RALP: try 4 strategies to extract description."""

        # Strategy 1: Per-site CSS hints
        for sel in _SITE_HINTS.get(sub_source, []):
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) >= min_len:
                return el.get_text(separator="\n", strip=True)[:5000]

        # Strategy 2: Generic Korean board selectors
        for sel in _KOREAN_BOARD_SELECTORS:
            el = soup.select_one(sel)
            if el and len(el.get_text(strip=True)) >= min_len:
                return el.get_text(separator="\n", strip=True)[:5000]

        # Strategy 3: Table body (Korean sites use tables for layout)
        for table in soup.find_all("table"):
            text = table.get_text(separator="\n", strip=True)
            if len(text) >= min_len * 2:  # higher threshold for table noise
                return text[:5000]

        # Strategy 4: Base class fallback (largest text block)
        return self._extract_description_fallback(html, min_length=min_len)


# ── Module-level helpers ──────────────────────────────────────────────────


def _extract_onclick_href(onclick: str) -> str | None:
    """Extract URL from onclick handlers like ``location.href='/path'`` or ``href='/path'``."""
    if not onclick:
        return None
    # location.href = '/web/board/13/23862'  or  href='/prog/jobOffer/.../view.do?nttId=155'
    m = re.search(r"""(?:location\.)?href\s*=\s*['"]([^'"]+)['"]""", onclick)
    return m.group(1) if m else None


# ── Title normalization ──────────────────────────────────────────────────

_TITLE_IS_INST_RE = re.compile(
    r"^[\[\(]?(?:.*대학교|.*대학|.*연구원|.*연구소|.*병원|"
    r"KAIST|POSTECH|DGIST|UNIST|GIST|.*University)[\]\)]?\s*$", re.I)

_POSITION_EXTRACT = [
    (re.compile(r"박사\s*후|포스닥|postdoc", re.I), "박사후연구원"),
    (re.compile(r"석사\s*후"), "석사후연구원"),
    (re.compile(r"연구\s*교수"), "연구교수"),
    (re.compile(r"전임\s*교원|교수\s*초빙|교원\s*초빙"), "전임교원"),
    (re.compile(r"연구원\s*모집|연구원\s*채용"), "연구원"),
]


def _normalize_korean_title(
    title: str, institute: str | None, description: str | None,
) -> str:
    """Supplement incomplete titles that only contain institute names."""
    # Already has position info — keep as-is
    if any(kw in title for kw in [
        "채용", "모집", "공고", "researcher", "fellow", "postdoc",
    ]):
        return title

    # Title is just an institute name — try to extract position from description
    if _TITLE_IS_INST_RE.match(title.strip()):
        desc = (description or "")[:500]
        for pat, label in _POSITION_EXTRACT:
            if pat.search(desc):
                return f"{title} {label} 모집"

    return title


def _extract_institute_from_page(
    soup: BeautifulSoup, sub_source: str,
) -> str | None:
    """Extract institute name from page metadata or source defaults."""
    # og:site_name meta tag
    meta = soup.select_one('meta[property="og:site_name"]')
    if meta and meta.get("content"):
        name = meta["content"].strip()
        if len(name) > 3 and not _looks_like_date(name):
            return name
    # Source-specific default institute name
    return _DEFAULT_INSTITUTES.get(sub_source)


# ── Korean title / content validation ─────────────────────────────────────

# Navigation/menu items, page elements, and non-job content
_KOREAN_GARBAGE_TITLES = {
    # Navigation / site elements
    "상품소개", "채용정보", "채용안내", "오늘마감", "유형별채용정보",
    "설립이념", "학교현황", "조직안내", "비전2031", "비전",
    "정보공개", "연구개발", "안전정보", "소통마당", "공지사항",
    "인재채용", "오시는길", "사이트맵", "이용약관", "개인정보",
    "주요사업", "기관소개", "연혁", "인사말", "미션", "윤리경영",
    "찾아오시는길", "관련사이트", "고객센터", "자주묻는질문",
    "이메일무단수집거부", "뉴스레터", "공개채용", "채용절차",
    "분류별채용정보", "연구원의길", "교수의길", "임용상담실",
    "임용후기", "등록/수정일순", "연구비정보", "평가심사",
    "장학금·공모전", "전문교육", "학술행사", "병역특례",
    "글로벌 가치창출 선도대학", "과학기술 혁신의 현장",
    "뷰페이지로 이동",
    # English navigation
    "support systems for international researchers",
    "annual r&d report", "research and entrepreneurship",
    "educational opportunities",
}

# Patterns in titles that indicate non-job content
_KOREAN_GARBAGE_PATTERNS = [
    r"^전체\d+건$",           # "전체709건" — count label
    r"^\d+$",                 # bare numbers
    r"전형\s*결과",           # "전형 결과" — selection results
    r"합격자\s*안내",         # "합격자 안내" — acceptance notice
    r"필기전형\s*결과",       # exam results
    r"서류전형\s*결과",       # document screening results
    r"최종\s*결과\s*안내",    # final results
    r"전형\s*일정\s*안내",    # schedule notice
    r"친인척\s*채용",         # nepotism disclosure
    r"인터뷰$",              # interview articles ("브만사 인터뷰")
    r"^분류별",              # category nav items
    r"의\s*길$",             # "연구원의길", "교수의길"
    r"시설관리직",           # facility management
    r"시간제상담사",         # part-time counselor
    r"^@|@kaist|@snu",      # email addresses as titles
]

# Positive indicators — titles that ARE job postings
_KOREAN_JOB_INDICATORS = [
    "채용", "공고", "모집", "연구원", "박사", "포스닥", "postdoc",
    "researcher", "fellow", "scientist", "professor", "교수",
    "recruit", "position", "opening", "vacancy",
]


_KOREAN_BIO_SIGNALS = [
    # Postdoc positions (always research-relevant)
    "박사후", "박사 후", "석사후", "석사 후",
    "postdoc", "post-doc", "postdoctoral",
    # Bio / medical / science fields (Korean)
    "바이오", "생명", "생물", "약학", "의과", "의학", "의생명",
    "뇌과학", "뇌공학", "유전", "분자", "단백질", "효소", "미생물",
    "세포", "게놈", "면역", "약리", "생화학", "발효", "보건연구",
    "생명공학", "합성생물", "대사공학", "식품공학",
    # English bio terms
    "biotech", "crispr", "genomic", "protein", "biology",
    "molecular", "biochem", "bioinformat", "microbio",
    "synthetic bio", "enzyme", "ferment",
]


def _is_bio_relevant_korean(title: str) -> bool:
    """Return True if a Korean job title is relevant to bio/research fields.

    Used to filter out non-bio positions from general Korean job boards
    (HiBrainNet, etc.) that list ALL types of professional positions.
    """
    t = title.lower()
    return any(sig in t for sig in _KOREAN_BIO_SIGNALS)


def _is_valid_korean_job_title(title: str) -> bool:
    """Return True if the title looks like an actual job posting."""
    t = title.strip()
    if not t:
        return False

    t_lower = t.lower()

    # Reject exact garbage matches
    if t_lower in _KOREAN_GARBAGE_TITLES:
        return False

    # Reject garbage patterns
    for pat in _KOREAN_GARBAGE_PATTERNS:
        if re.search(pat, t):
            return False

    # Very short titles (< 5 chars for Korean, < 8 for English-only)
    has_korean = bool(re.search(r"[\uac00-\ud7a3]", t))
    if has_korean and len(t) < 5:
        return False
    if not has_korean and len(t) < 8:
        return False

    # If title has a positive indicator, accept it
    if any(kw in t_lower for kw in _KOREAN_JOB_INDICATORS):
        return True

    # Reject very short Korean-only titles without job indicators (likely nav items)
    if has_korean and len(t) <= 10:
        return False

    return True


def _looks_like_date(text: str) -> bool:
    """Return True if text looks like a date string, not an institute name."""
    text = text.strip().rstrip(".")
    # "2026. 2. 27" or "2026.02.27" or "2026-02-27"
    if re.match(r"^\d{4}\s*[.\-/]\s*\d{1,2}\s*[.\-/]\s*\d{1,2}\.?$", text):
        return True
    # "2026년 3월 15일"
    if re.search(r"\d{4}\s*년", text):
        return True
    return False


def _extract_korean_fields(soup: BeautifulSoup, job: dict[str, Any]) -> None:
    """Extract structured fields from Korean detail pages.

    Korean job boards typically use <table> layouts with <th>/<td> pairs
    or <dt>/<dd> pairs.  This function scans for Korean header labels
    and fills in missing job dict fields.
    """
    # Build a header→value map from all <th>+<td> and <dt>+<dd> pairs
    kv: list[tuple[str, str]] = []
    for th in soup.select("th"):
        td = th.find_next_sibling("td")
        if td:
            kv.append((th.get_text(strip=True), td.get_text(separator=" ", strip=True)))
    for dt in soup.select("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            kv.append((dt.get_text(strip=True), dd.get_text(separator=" ", strip=True)))

    for header, value in kv:
        if not value or len(value) < 2:
            continue
        h = header.strip()

        # ── Institute / 기관 ──
        if not job.get("institute") and re.search(
            r"기관|소속|대학|연구소|회사|institution|organization", h, re.IGNORECASE
        ):
            job["institute"] = value[:200]

        # ── Department / 부서 ──
        elif not job.get("department") and re.search(
            r"부서|학과|학부|전공|분야|센터|연구단|소속\s*부서|department|division|center",
            h, re.IGNORECASE,
        ):
            job["department"] = value[:200]

        # ── Field / 연구분야 ──
        elif not job.get("field") and re.search(
            r"연구\s*분야|전문\s*분야|연구\s*영역|research\s*field|research\s*area",
            h, re.IGNORECASE,
        ):
            job["field"] = value[:300]

        # ── PI name / 연구책임자 ──
        elif not job.get("pi_name") and re.search(
            r"책임자|지도\s*교수|연구\s*책임|PI|supervisor|principal\s*investigator"
            r"|담당\s*교수|지도\s*위원",
            h, re.IGNORECASE,
        ):
            # Clean: strip phone/email from PI value
            pi = re.split(r"[,;/]|\s*\(", value)[0].strip()
            if 2 <= len(pi) <= 40:
                job["pi_name"] = pi

        # ── Deadline / 마감일 ──
        elif not job.get("deadline") and re.search(
            r"마감|접수\s*기간|모집\s*기간|지원\s*기한|deadline|closing|접수\s*마감",
            h, re.IGNORECASE,
        ):
            parsed = _parse_korean_date(value)
            if parsed:
                job["deadline"] = parsed

        # ── Salary / 급여 ──
        elif not job.get("conditions") and re.search(
            r"급여|보수|연봉|처우|salary|compensation|pay|대우",
            h, re.IGNORECASE,
        ):
            job.setdefault("_salary", value[:300])

        # ── Duration / 기간 ──
        elif re.search(
            r"계약\s*기간|임용\s*기간|근무\s*기간|기간|duration|term|period",
            h, re.IGNORECASE,
        ):
            job.setdefault("_duration", value[:200])

        # ── Contract type / 근무형태 ──
        elif re.search(
            r"근무\s*형태|고용\s*형태|계약\s*형태|직종|employment\s*type|contract\s*type",
            h, re.IGNORECASE,
        ):
            job.setdefault("_contract_type", value[:200])

        # ── Requirements / 자격요건 ──
        elif not job.get("requirements") and re.search(
            r"자격|요건|조건|우대|필요\s*역량|지원\s*자격|requirement|qualification",
            h, re.IGNORECASE,
        ):
            job["requirements"] = value[:2000]

        # ── Application materials / 제출서류 ──
        elif not job.get("application_materials") and re.search(
            r"제출\s*서류|지원\s*서류|접수\s*방법|지원\s*방법|구비\s*서류"
            r"|application|how\s*to\s*apply|documents",
            h, re.IGNORECASE,
        ):
            job["application_materials"] = value[:500]

        # ── Contact / 문의 ──
        elif not job.get("contact_email") and re.search(
            r"문의|연락처|담당자|contact|email|이메일", h, re.IGNORECASE
        ):
            # Try to extract email from value
            email_m = re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", value)
            if email_m:
                job["contact_email"] = email_m.group(0)

    # ── Build conditions from salary/duration/contract_type ──
    if not job.get("conditions"):
        parts = []
        if job.get("_salary"):
            parts.append(f"급여: {job.pop('_salary')}")
        if job.get("_duration"):
            parts.append(f"기간: {job.pop('_duration')}")
        if job.get("_contract_type"):
            parts.append(f"근무형태: {job.pop('_contract_type')}")
        if parts:
            job["conditions"] = " | ".join(parts)
    else:
        # Clean up temp keys
        job.pop("_salary", None)
        job.pop("_duration", None)
        job.pop("_contract_type", None)

    # ── Fallback: extract keywords from description ──
    desc = job.get("description", "")
    if not job.get("keywords") and desc:
        kws = _extract_korean_keywords(desc)
        if kws:
            job["keywords"] = ", ".join(kws)

    # ── Fallback: extract PI from description patterns ──
    if not job.get("pi_name") and desc:
        pi = _extract_korean_pi(desc)
        if pi:
            job["pi_name"] = pi

    # ── Fallback: extract field from title ──
    if not job.get("field") and job.get("title"):
        field = _infer_korean_field(job["title"], desc)
        if field:
            job["field"] = field

    # ── Fallback: parse institute/department from Korean title ──
    title = job.get("title") or ""
    if not job.get("institute") and title:
        inst, dept = _parse_institute_from_title(title)
        if inst:
            job["institute"] = inst
        if dept and not job.get("department"):
            job["department"] = dept

    # ── Fallback: extract deadline from description text ──
    if not job.get("deadline") and desc:
        for pat in [
            r"(?:접수\s*기간|모집\s*기간)\s*[:\s]*\d{4}[.\-/]\s*\d{1,2}[.\-/]\s*\d{1,2}"
            r"[^~]*~\s*(\d{4})[.\-/]\s*(\d{1,2})[.\-/]\s*(\d{1,2})",
            r"(?:마감|지원\s*기한|접수\s*마감)\s*[:\s]*(\d{4})[.\s\-/]*(\d{1,2})[.\s\-/]*(\d{1,2})",
            r"(?:deadline|closing\s*date)\s*[:\s]*(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})",
        ]:
            m = re.search(pat, desc, re.IGNORECASE)
            if m:
                try:
                    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
                    job["deadline"] = f"{y:04d}-{mo:02d}-{d:02d}"
                    break
                except (ValueError, IndexError):
                    pass


# Patterns to extract institute name from beginning of Korean job titles
_TITLE_INSTITUTE_PATTERNS = [
    # "한국○○연구원" or "○○연구원/연구소/연구센터"
    r"^((?:한국)?[\w가-힣]+(?:연구원|연구소|연구센터))\s",
    # "○○대학교" or "○○대학"
    r"^([\w가-힣]+(?:대학교|대학))\s",
    # Known English acronyms at start
    r"^(KAIST|POSTECH|SNU|GIST|DGIST|UNIST|IBS)\s",
    # "국립○○원" / "○○진흥원" / "○○재단" etc.
    r"^((?:국립)?[\w가-힣]+(?:원|재단|공단|기구))\s",
]

_TITLE_DEPT_PATTERNS = [
    r"([\w가-힣]+(?:학과|공학과|과학과|학부|대학원|센터|연구단|사업단))\s",
]


def _parse_institute_from_title(title: str) -> tuple[str | None, str | None]:
    """Extract institute and optionally department from Korean title."""
    for pat in _TITLE_INSTITUTE_PATTERNS:
        m = re.search(pat, title)
        if m:
            inst = m.group(1).strip()
            # Try to find department in remaining text
            rest = title[m.end():]
            dept = None
            for dpat in _TITLE_DEPT_PATTERNS:
                dm = re.search(dpat, rest)
                if dm:
                    dept = dm.group(1).strip()
                    break
            return inst, dept
    return None, None


def _extract_korean_keywords(text: str) -> list[str]:
    """Extract research keywords from Korean text."""
    found = []
    keywords_kr = [
        "합성생물학", "유전체", "단백질공학", "CRISPR", "유전자편집",
        "대사공학", "미생물", "효소공학", "바이오", "분자생물학",
        "시스템생물학", "생물정보학", "마이크로바이옴", "세포공학",
        "극한미생물", "고세균", "발효", "나노바이오", "세포치료",
        "유전공학", "NGS", "오믹스", "생화학", "약학", "생명과학",
        "생명공학", "화학생물학", "구조생물학",
    ]
    text_lower = text.lower()
    for kw in keywords_kr:
        if kw.lower() in text_lower:
            found.append(kw)
    # Also detect English keywords commonly appearing in Korean postings
    en_keywords = [
        "synthetic biology", "CRISPR", "Cas9", "protein engineering",
        "directed evolution", "metabolic engineering", "genome engineering",
        "bioinformatics", "systems biology", "molecular biology",
        "microbiology", "enzyme engineering", "microbiome",
        "high-throughput screening", "cell-free", "NGS",
    ]
    for kw in en_keywords:
        if kw.lower() in text_lower and kw not in found:
            found.append(kw)
    return found


def _extract_korean_pi(text: str) -> str | None:
    """Extract PI name from Korean text patterns."""
    patterns = [
        # "연구책임자: 홍길동" or "지도교수 : 김철수"
        r"(?:연구\s*책임자|지도\s*교수|담당\s*교수|PI)\s*[:\s]\s*([가-힣]{2,4})",
        # "홍길동 교수" (name + title) in context
        r"([가-힣]{2,4})\s*(?:교수|박사|연구원|선생)(?:님)?(?:의?\s*(?:연구실|랩|Lab))",
        # English PI patterns in Korean text
        r"(?:PI|Supervisor|Prof\.?|Dr\.?)\s*[:\s]\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})",
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            name = m.group(1).strip()
            if len(name) >= 2:
                return name
    return None


def _infer_korean_field(title: str, desc: str) -> str | None:
    """Infer research field from Korean title/description."""
    blob = f"{title} {desc[:500]}".lower()
    field_map = [
        (r"합성\s*생물|synthetic\s*bio", "Synthetic Biology"),
        (r"단백질\s*공학|protein\s*eng", "Protein Engineering"),
        (r"유전체|genome|genomics", "Genomics"),
        (r"CRISPR|유전자\s*편집|gene\s*edit", "Gene Editing"),
        (r"대사\s*공학|metabolic\s*eng", "Metabolic Engineering"),
        (r"미생물|microbio", "Microbiology"),
        (r"효소\s*공학|enzyme\s*eng", "Enzyme Engineering"),
        (r"생물\s*정보|bioinformatics", "Bioinformatics"),
        (r"시스템\s*생물|systems\s*bio", "Systems Biology"),
        (r"분자\s*생물|molecular\s*bio", "Molecular Biology"),
        (r"세포\s*생물|cell\s*bio", "Cell Biology"),
        (r"구조\s*생물|structural\s*bio", "Structural Biology"),
        (r"화학\s*생물|chemical\s*bio", "Chemical Biology"),
        (r"생명\s*공학|biotechnology", "Biotechnology"),
        (r"생명\s*과학|life\s*science", "Life Sciences"),
        (r"약학|pharm", "Pharmacology"),
        (r"면역|immun", "Immunology"),
        (r"신경|neuro", "Neuroscience"),
        (r"발효|ferment", "Fermentation"),
        (r"바이오|bio", "Biology"),
    ]
    for pat, field in field_map:
        if re.search(pat, blob, re.IGNORECASE):
            return field
    return None


def _parse_korean_date(text: str | None) -> str | None:
    """Parse Korean-style dates: '2026.03.15', '2026-03-15', '2026년 3월 15일'.

    For date ranges like '2026.02.26 ~ 2026.03.08', returns the END date
    (the deadline), not the start date.
    """
    if not text:
        return None
    text = text.strip()

    # Date range: "2026.02.26 ~ 2026.03.08" → use the LAST date (deadline)
    all_matches = re.findall(r"(\d{4})[.\-/]\s*(\d{1,2})[.\-/]\s*(\d{1,2})", text)
    if all_matches:
        # Use the last date in the string (= deadline / end date)
        y, m, d = all_matches[-1]
        try:
            dt = datetime(int(y), int(m), int(d))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # 2026년 3월 15일
    all_kr = re.findall(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
    if all_kr:
        y, m, d = all_kr[-1]
        try:
            dt = datetime(int(y), int(m), int(d))
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None
