"""Nature Careers scraper for postdoc positions (RSS + HTML fallback)."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin
from xml.etree import ElementTree

from bs4 import BeautifulSoup

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.nature.com"
SEARCH_URL = f"{BASE_URL}/naturecareers/jobs/search"

# RSS feeds (may 403 due to Nature blocking scrapers)
RSS_FEEDS = [
    f"{SEARCH_URL}?type=postdoctoral&format=rss",
    f"{SEARCH_URL}?type=postdoctoral&discipline=biological-sciences&format=rss",
    f"{SEARCH_URL}?type=postdoctoral&discipline=chemistry&format=rss",
]

# HTML search fallback URLs
HTML_SEARCHES = [
    {"type": "postdoctoral", "discipline": "biological-sciences"},
    {"type": "postdoctoral", "discipline": "chemistry"},
    {"type": "postdoctoral", "discipline": "life-science"},
]

# Namespace used in Nature's RSS
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
}

MAX_PAGES = 3


def _parse_date(raw: str | None) -> str | None:
    if not raw:
        return None
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d",
        "%d %B %Y",
        "%B %d, %Y",
    ):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"\d{4}-\d{2}-\d{2}", raw or "")
    return m.group(0) if m else None


def _matches_keywords(text: str) -> bool:
    lower = text.lower()
    if "postdoc" in lower or "postdoctoral" in lower:
        return True
    return any(kw.lower() in lower for kw in CV_KEYWORDS)


class NatureCareersScraper(BaseScraper):
    """Scrape postdoc positions from Nature Careers (RSS + HTML fallback)."""

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "nature_careers"

    # ── RSS parsing ───────────────────────────────────────────────────────

    def _parse_rss_items(self, xml_text: str) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            self.logger.warning("Failed to parse RSS XML")
            return jobs

        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date = item.findtext("pubDate")
            creator = item.findtext("dc:creator", namespaces=_NS)
            category = item.findtext("category") or ""

            institute, country = self._extract_institute_country(
                title, description, creator
            )

            blob = f"{title} {description} {category}"
            if not _matches_keywords(blob):
                continue

            jobs.append({
                "title": title,
                "institute": institute,
                "country": country,
                "posted_date": _parse_date(pub_date),
                "url": link,
                "description": description[:2000],
                "source": self.name,
                "field": category or None,
            })

        return jobs

    # ── HTML parsing (fallback when RSS returns 403) ──────────────────────

    def _parse_html_page(self, html: str) -> list[dict[str, Any]]:
        """Parse Nature Careers HTML search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Nature Careers renders job cards in various containers
        cards = soup.select(
            "div.card, div.job-card, article.job-listing, "
            "li.pb-4, div[data-test='job-card'], "
            "div[class*='JobCard'], a[class*='JobCard']"
        )

        if not cards:
            # Fallback: find links to job detail pages
            for link in soup.select("a[href*='/job/'], a[href*='/naturecareers/job/']"):
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
            job = self._parse_html_card(card)
            if job:
                jobs.append(job)

        return jobs

    def _parse_html_card(self, card) -> dict[str, Any] | None:
        link = card if card.name == "a" else card.select_one(
            "a[href*='/job/'], a[href*='/naturecareers/'], h2 a, h3 a"
        )
        if not link:
            return None

        title_el = card.select_one("h2, h3, span.title, strong")
        title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        inst_el = card.select_one("span.employer, span.institution, div.employer, p.company")
        institute = inst_el.get_text(strip=True) if inst_el else None

        loc_el = card.select_one("span.location, div.location, span.country")
        location = loc_el.get_text(strip=True) if loc_el else None
        country = self._extract_country_from_location(location)

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "source": self.name,
        }

    # ── Detail page enrichment ────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Full description (keep up to 15000 chars)
            found_desc = False
            for sel in (
                "div.job-description", "div.content-body",
                "div[class*='Description']", "article", "main",
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

            # Institute
            if not job.get("institute"):
                for sel in ("span.employer", "a.employer", "div.employer-name"):
                    el = soup.select_one(sel)
                    if el:
                        job["institute"] = el.get_text(strip=True)
                        break

            # Country
            if not job.get("country"):
                for sel in ("span.location", "div.location", "span[itemprop='addressCountry']"):
                    el = soup.select_one(sel)
                    if el:
                        job["country"] = self._extract_country_from_location(
                            el.get_text(strip=True)
                        )
                        break

            # Department
            if not job.get("department"):
                for sel in ("span.department", "div.department"):
                    el = soup.select_one(sel)
                    if el:
                        job["department"] = el.get_text(strip=True)
                        break

            # Deadline
            if not job.get("deadline"):
                for sel in ("span.deadline", "time.deadline", "div.closing-date"):
                    el = soup.select_one(sel)
                    if el:
                        raw = el.get("datetime") or el.get_text(strip=True)
                        job["deadline"] = _parse_date(raw)
                        break

        except Exception:
            self.logger.debug("Could not enrich detail: %s", url)

        return job

    # ── Main scrape logic ─────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []
        rss_worked = False

        # 1. Try RSS feeds first
        for feed_url in RSS_FEEDS:
            try:
                resp = self.fetch(
                    feed_url,
                    headers={"Accept": "application/rss+xml, application/xml, text/xml"},
                )
                items = self._parse_rss_items(resp.text)
                self.logger.info("RSS %s: %d matching items", feed_url, len(items))
                all_jobs.extend(items)
                if items:
                    rss_worked = True
            except Exception:
                self.logger.warning("RSS feed failed: %s", feed_url)

        # 2. If RSS failed, fall back to HTML scraping
        if not rss_worked:
            self.logger.info("RSS feeds failed, trying HTML scraping")
            html_failed = 0
            for search_params in HTML_SEARCHES:
                try:
                    params = dict(search_params)
                    resp = self.fetch(SEARCH_URL, params=params)
                    page_jobs = self._parse_html_page(resp.text)
                    if page_jobs:
                        self.logger.info(
                            "HTML %s: %d results",
                            search_params.get("discipline", "all"),
                            len(page_jobs),
                        )
                        all_jobs.extend(page_jobs)
                except Exception:
                    html_failed += 1
                    self.logger.debug(
                        "HTML scrape failed: %s",
                        search_params.get("discipline", "all"),
                    )

            if html_failed == len(HTML_SEARCHES) and not all_jobs:
                # Try Playwright as last resort
                self.logger.info(
                    "Nature Careers HTTP blocked on all %d searches; trying Playwright...",
                    html_failed,
                )
                try:
                    from src.scrapers.browser import fetch_page
                    import time as _time
                    for search_params in HTML_SEARCHES:
                        from urllib.parse import urlencode as _urlencode
                        pw_url = f"{SEARCH_URL}?{_urlencode(search_params)}"
                        html = fetch_page(
                            pw_url,
                            wait_selector="div.card, div.job-card, a[href*='/job/']",
                            wait_ms=5000,
                        )
                        if html:
                            page_jobs = self._parse_html_page(html)
                            if page_jobs:
                                self.logger.info(
                                    "Playwright %s: %d results",
                                    search_params.get("discipline", "all"),
                                    len(page_jobs),
                                )
                                all_jobs.extend(page_jobs)
                        _time.sleep(3.0)
                except ImportError:
                    self.logger.warning("Playwright not installed")
                except Exception:
                    self.logger.debug("Playwright fallback also failed for Nature Careers")

        # 3. Keyword filter
        filtered = [
            j for j in all_jobs
            if _matches_keywords(
                f"{j.get('title', '')} {j.get('description', '')} {j.get('field', '')}"
            )
        ]

        # 4. Enrich top results
        enriched: list[dict[str, Any]] = []
        for job in filtered[:30]:
            job = self._enrich_from_detail(job)
            enriched.append(job)
        enriched.extend(filtered[30:])

        self.logger.info("Total Nature Careers jobs: %d", len(enriched))
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _extract_institute_country(
        title: str, description: str, creator: str | None
    ) -> tuple[str | None, str | None]:
        institute = creator.strip() if creator else None
        country = None
        loc_match = re.search(
            r"(?:Location|Country|Place)\s*:\s*(.+?)(?:<|$)",
            description,
            re.IGNORECASE,
        )
        if loc_match:
            raw_loc = loc_match.group(1).strip()
            parts = [p.strip() for p in raw_loc.split(",")]
            country = parts[-1] if parts else None
        else:
            from src.config import COUNTRY_TO_REGION
            lower = description.lower()
            for country_name in COUNTRY_TO_REGION:
                if country_name.lower() in lower:
                    country = country_name
                    break
        return institute, country

    @staticmethod
    def _extract_country_from_location(location: str | None) -> str | None:
        if not location:
            return None
        parts = [p.strip() for p in location.split(",")]
        return parts[-1] if parts else None
