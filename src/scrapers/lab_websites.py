"""PI lab website scraper – detects new hiring content on watched lab pages."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from typing import Any

from bs4 import BeautifulSoup

from src.config import LAB_HIRING_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Sub-paths commonly used for job/position pages on lab websites
_COMMON_SUBPATHS = [
    "",                 # root lab page
    "/positions",
    "/join",
    "/join-us",
    "/openings",
    "/opportunities",
    "/jobs",
    "/hiring",
    "/careers",
    "/people",          # some labs list openings on their people page
]


def _content_hash(text: str) -> str:
    """SHA-256 hash of normalised text for change detection."""
    normalised = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalised.encode()).hexdigest()


def _extract_hiring_snippets(text: str) -> list[str]:
    """Return sentences or paragraphs that mention hiring keywords."""
    snippets: list[str] = []
    # Split on sentence-ish boundaries
    sentences = re.split(r"(?<=[.!?])\s+|\n{2,}", text)
    for sent in sentences:
        lower = sent.lower()
        if any(kw.lower() in lower for kw in LAB_HIRING_KEYWORDS):
            cleaned = sent.strip()
            if len(cleaned) > 20:
                snippets.append(cleaned[:500])
    return snippets


class LabWebsiteScraper(BaseScraper):
    """Check PI lab websites for new hiring-related content.

    Workflow
    --------
    1. Load watchlist from the ``watchlist`` table (populated by the
       discovery pipeline with PIs' ``lab_url``).
    2. For each lab URL, fetch the page (and common sub-paths).
    3. Hash the page content; compare with ``last_content_hash``.
    4. If content changed and hiring keywords are found, create a job entry.
    5. Update ``last_content_hash`` and ``last_checked`` in the watchlist.
    """

    rate_limit: float = 2.0

    @property
    def name(self) -> str:
        return "lab_websites"

    # ── Fetch & detect ────────────────────────────────────────────────────

    def _fetch_page_text(self, url: str) -> str | None:
        """Fetch *url* and return visible text, or None on failure."""
        try:
            resp = self.fetch(url, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove scripts, styles, navs
            for tag in soup(["script", "style", "nav", "footer", "header", "noscript"]):
                tag.decompose()

            return soup.get_text(separator=" ", strip=True)
        except Exception:
            self.logger.debug("Could not fetch %s", url)
            return None

    def _check_lab(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """Check a single watchlist entry for new hiring content.

        Returns a job dict if new hiring content is detected, else None.
        """
        lab_url = (entry.get("lab_url") or "").rstrip("/")
        if not lab_url:
            return None

        pi_name = entry.get("pi_name", "Unknown PI")
        institute = entry.get("institute", "")
        old_hash = entry.get("last_content_hash") or ""

        # Aggregate text from the lab root and common sub-paths
        combined_text = ""
        for subpath in _COMMON_SUBPATHS:
            full_url = lab_url + subpath
            page_text = self._fetch_page_text(full_url)
            if page_text:
                combined_text += f"\n{page_text}"

        if not combined_text.strip():
            self.logger.debug("No content retrieved for %s", lab_url)
            self._update_watchlist_checked(entry["id"])
            return None

        new_hash = _content_hash(combined_text)

        # Check for hiring keywords
        snippets = _extract_hiring_snippets(combined_text)

        if not snippets:
            # No hiring content detected
            self._update_watchlist(entry["id"], new_hash)
            return None

        # Content changed?
        content_changed = new_hash != old_hash

        if not content_changed and old_hash:
            # Same content as before, already processed
            self._update_watchlist_checked(entry["id"])
            return None

        # New or changed hiring content found!
        self.logger.info(
            "Hiring content detected on %s (%s) – %d snippets",
            pi_name,
            lab_url,
            len(snippets),
        )

        description = " | ".join(snippets[:5])

        # Try to guess country from lab URL or institute
        country = self._guess_country(lab_url, institute)

        job = {
            "title": f"Potential opening – {pi_name} lab",
            "pi_name": pi_name,
            "institute": institute or None,
            "country": country,
            "url": lab_url,
            "lab_url": lab_url,
            "description": description[:2000],
            "source": self.name,
            "posted_date": datetime.utcnow().strftime("%Y-%m-%d"),
        }

        # Update the watchlist hash
        self._update_watchlist(entry["id"], new_hash)

        return job

    # ── DB helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _update_watchlist(watchlist_id: int, new_hash: str) -> None:
        """Update last_checked and last_content_hash for a watchlist entry."""
        from src.db import get_connection

        with get_connection() as conn:
            conn.execute(
                "UPDATE watchlist SET last_checked = datetime('now'), "
                "last_content_hash = ? WHERE id = ?",
                (new_hash, watchlist_id),
            )

    @staticmethod
    def _update_watchlist_checked(watchlist_id: int) -> None:
        """Update only last_checked timestamp."""
        from src.db import get_connection

        with get_connection() as conn:
            conn.execute(
                "UPDATE watchlist SET last_checked = datetime('now') WHERE id = ?",
                (watchlist_id,),
            )

    @staticmethod
    def _guess_country(lab_url: str, institute: str) -> str | None:
        """Best-effort country detection from URL TLD or institute name."""
        from src.config import COUNTRY_TO_REGION

        url_lower = lab_url.lower()

        # TLD-based guesses
        tld_map = {
            ".edu": "United States",
            ".ac.uk": "United Kingdom",
            ".ac.jp": "Japan",
            ".ac.kr": "South Korea",
            ".edu.au": "Australia",
            ".de": "Germany",
            ".ch": "Switzerland",
            ".fr": "France",
            ".nl": "Netherlands",
            ".se": "Sweden",
            ".dk": "Denmark",
            ".no": "Norway",
            ".fi": "Finland",
            ".sg": "Singapore",
            ".edu.cn": "China",
            ".ac.il": "Israel",
            ".ie": "Ireland",
            ".be": "Belgium",
            ".at": "Austria",
            ".it": "Italy",
            ".es": "Spain",
        }

        for tld, country in tld_map.items():
            if tld in url_lower:
                return country

        # Check institute name
        inst_lower = institute.lower() if institute else ""
        for country in COUNTRY_TO_REGION:
            if country.lower() in inst_lower:
                return country

        return None

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        from src.db import get_watchlist

        watchlist = get_watchlist()
        if not watchlist:
            self.logger.info("Watchlist is empty; nothing to check")
            return []

        self.logger.info("Checking %d lab websites from watchlist", len(watchlist))
        jobs: list[dict[str, Any]] = []

        for entry in watchlist:
            try:
                job = self._check_lab(entry)
                if job:
                    jobs.append(job)
            except Exception:
                self.logger.exception(
                    "Error checking lab: %s (%s)",
                    entry.get("pi_name"),
                    entry.get("lab_url"),
                )

        self.logger.info("Lab website scraper found %d new openings", len(jobs))
        return jobs
