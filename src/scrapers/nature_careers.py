"""Nature Careers RSS feed scraper for postdoc positions."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from xml.etree import ElementTree

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

RSS_URL = "https://www.nature.com/naturecareers/jobs/search"
# Multiple feeds for broader coverage
RSS_FEEDS = [
    f"{RSS_URL}?type=postdoc&format=rss",
    f"{RSS_URL}?type=postdoc&discipline=biological-sciences&format=rss",
    f"{RSS_URL}?type=postdoc&discipline=chemistry&format=rss",
]

# Namespace used in Nature's RSS
_NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "dc": "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
}


def _parse_date(raw: str | None) -> str | None:
    """Try to parse multiple date formats Nature might use."""
    if not raw:
        return None
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",   # RFC 822
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",         # ISO 8601
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw.strip()[:10]  # fallback: first 10 chars


def _matches_keywords(text: str) -> bool:
    """Check whether *text* contains at least one CV_KEYWORD (case-insensitive)."""
    lower = text.lower()
    return any(kw.lower() in lower for kw in CV_KEYWORDS)


def _extract_country(location: str | None) -> str | None:
    """Best-effort extraction of country from a location string like
    'Cambridge, United Kingdom' or 'Boston, MA, United States'.
    """
    if not location:
        return None
    parts = [p.strip() for p in location.split(",")]
    if parts:
        return parts[-1]
    return None


class NatureCareersScraper(BaseScraper):
    """Scrape postdoc positions from Nature Careers RSS feed."""

    rate_limit: float = 2.0  # be polite to Nature

    @property
    def name(self) -> str:
        return "nature_careers"

    # ── RSS parsing ───────────────────────────────────────────────────────

    def _parse_rss_items(self, xml_text: str) -> list[dict[str, Any]]:
        """Parse <item> elements from an RSS 2.0 feed."""
        jobs: list[dict[str, Any]] = []
        try:
            root = ElementTree.fromstring(xml_text)
        except ElementTree.ParseError:
            self.logger.warning("Failed to parse RSS XML")
            return jobs

        # RSS 2.0: /rss/channel/item
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            description = (item.findtext("description") or "").strip()
            pub_date = item.findtext("pubDate")
            # Nature sometimes uses dc:creator for employer / institute
            creator = item.findtext("dc:creator", namespaces=_NS)
            # Nature sometimes embeds location in custom tags or in the description
            category = item.findtext("category") or ""

            # Try to extract institute and location from the description or
            # dedicated fields.  Nature's RSS often puts the employer in the
            # title or description.
            institute, location = self._extract_institute_location(
                title, description, creator
            )
            country = _extract_country(location)

            # Combine title + description for keyword matching
            blob = f"{title} {description} {category}"
            if not _matches_keywords(blob):
                continue

            jobs.append(
                {
                    "title": title,
                    "institute": institute,
                    "country": country,
                    "posted_date": _parse_date(pub_date),
                    "url": link,
                    "description": description[:2000],
                    "source": self.name,
                    "field": category or None,
                }
            )

        return jobs

    @staticmethod
    def _extract_institute_location(
        title: str, description: str, creator: str | None
    ) -> tuple[str | None, str | None]:
        """Heuristic extraction of institute and location.

        Nature Careers RSS items typically have the employer in
        the <dc:creator> element or in the description body.
        Location is often in the description as well.
        """
        institute = creator.strip() if creator else None

        # Try to grab location from description – commonly the last line
        # or after keywords like "Location:" / "Country:"
        location = None
        loc_match = re.search(
            r"(?:Location|Country|Place)\s*:\s*(.+?)(?:<|$)",
            description,
            re.IGNORECASE,
        )
        if loc_match:
            location = loc_match.group(1).strip()
        else:
            # Fallback: look for known country names in the description
            for country_name in (
                "United States",
                "United Kingdom",
                "Germany",
                "Switzerland",
                "Canada",
                "France",
                "Netherlands",
                "Sweden",
                "Japan",
                "South Korea",
                "Singapore",
                "Australia",
                "China",
                "Denmark",
                "Norway",
                "Austria",
                "Belgium",
                "Italy",
                "Spain",
                "Israel",
                "Ireland",
            ):
                if country_name.lower() in description.lower():
                    location = country_name
                    break

        return institute, location

    # ── Main scrape logic ─────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []

        for feed_url in RSS_FEEDS:
            try:
                resp = self.fetch(
                    feed_url,
                    headers={"Accept": "application/rss+xml, application/xml, text/xml"},
                )
                items = self._parse_rss_items(resp.text)
                self.logger.info(
                    "Feed %s returned %d matching items", feed_url, len(items)
                )
                all_jobs.extend(items)
            except Exception:
                self.logger.exception("Failed to fetch feed: %s", feed_url)

        self.logger.info("Total Nature Careers jobs collected: %d", len(all_jobs))
        return all_jobs
