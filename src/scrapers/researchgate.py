"""ResearchGate jobs scraper for postdoc positions."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS, SEARCH_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://www.researchgate.net"
JOBS_URL = f"{BASE_URL}/jobs"

# We build search queries from a curated subset of SEARCH_KEYWORDS
# (ResearchGate search is simpler than full-text job boards)
SEARCH_TERMS = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc metabolic engineering",
    "postdoc microbiology",
    "postdoctoral researcher biology",
    "postdoc directed evolution",
    "postdoc genome engineering",
    "postdoc biochemistry",
    "postdoc extremophiles",
    "postdoc systems biology",
    "postdoc bioengineering",
]

MAX_PAGES = 3


class ResearchGateScraper(BaseScraper):
    """Scrape ResearchGate job listings for postdoc positions in the life sciences."""

    rate_limit: float = 3.0  # ResearchGate is aggressive about bots

    @property
    def name(self) -> str:
        return "researchgate"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a ResearchGate jobs search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Try multiple selectors (ResearchGate updates their CSS frequently)
        cards = soup.select(
            "div.nova-legacy-v-entity-item, "
            "div.nova-v-entity-item, "
            "div[class*='entity-item'], "
            "div.search-result-item, "
            "li.result-item"
        )

        if not cards:
            # Fallback: try to find any job links
            for link in soup.select("a[href*='job/'], a[href*='/jobs/']"):
                href = link.get("href", "")
                title = link.get_text(strip=True)
                if title and len(title) > 5 and href:
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
        # Title link — try multiple selector patterns (nova-legacy and nova-v)
        link = None
        for sel in ("div.nova-legacy-v-entity-item__title a",
                     "div.nova-v-entity-item__title a",
                     "div[class*='entity-item__title'] a",
                     "h3 a", "h2 a"):
            link = card.select_one(sel)
            if link:
                break

        if not link:
            # Fallback: find first link with text
            for a in card.select("a[href*='job/'], a[href*='/jobs/']"):
                if a.get_text(strip=True):
                    link = a
                    break

        if not link:
            return None

        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            return None
        url = urljoin(BASE_URL, href)

        # Institute and location from info list items (try both nova-legacy and nova-v)
        info_items = card.select(
            "li.nova-legacy-e-list__item span, "
            "li.nova-e-list__item span, "
            "li[class*='list__item'] span"
        )
        institute = info_items[0].get_text(strip=True) if info_items else None
        location = info_items[1].get_text(strip=True) if len(info_items) > 1 else None
        country = self._extract_country(location)

        # Date
        date_el = card.select_one("span.date, time, span.posted-date")
        posted_date = None
        if date_el:
            raw = date_el.get("datetime") or date_el.get_text(strip=True)
            posted_date = self._parse_date(raw)

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "posted_date": posted_date,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    @staticmethod
    def _strip_html(raw: str) -> str:
        """Strip HTML tags from a string, returning plain text."""
        if "<" not in raw:
            return raw
        desc_soup = BeautifulSoup(raw, "html.parser")
        return desc_soup.get_text(separator="\n", strip=True)

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the full job detail page via Playwright (primary) with HTTP fallback.

        ResearchGate uses Cloudflare bot protection which blocks plain HTTP
        requests, so Playwright is used as the primary fetch method.
        """
        url = job.get("url")
        if not url:
            return job

        html_text = None

        # Playwright first (ResearchGate blocks HTTP with Cloudflare 403)
        try:
            from src.scrapers.browser import fetch_page
            html_text = fetch_page(
                url,
                wait_selector="div.job-description, div.job-details-nova, script[type='application/ld+json']",
                wait_ms=5000,
                timeout=40000,
            )
        except Exception:
            self.logger.debug("Playwright detail fetch failed for %s", url)

        # HTTP fallback (rarely succeeds due to Cloudflare, but worth trying)
        if not html_text:
            try:
                resp = self.fetch(url)
                # Check that we got a real page, not a Cloudflare challenge
                if resp.status_code == 200 and "JobPosting" in resp.text:
                    html_text = resp.text
            except Exception:
                self.logger.debug("HTTP detail fetch also failed for %s", url)

        if not html_text:
            return job

        try:
            soup = BeautifulSoup(html_text, "html.parser")

            # Try JSON-LD structured data first (most reliable)
            import json as _json
            for script in soup.select("script[type='application/ld+json']"):
                try:
                    ld = _json.loads(script.string or "")
                    if isinstance(ld, dict) and ld.get("@type") == "JobPosting":
                        if ld.get("description") and len(ld["description"]) > 50:
                            # JSON-LD description contains HTML tags; strip them
                            clean = self._strip_html(ld["description"])
                            if len(clean) > 50:
                                job["description"] = clean[:5000]
                        org = ld.get("hiringOrganization", {})
                        if isinstance(org, dict) and org.get("name"):
                            job["institute"] = org["name"]
                        if isinstance(org, dict) and org.get("department"):
                            job.setdefault("department", org["department"])
                        loc = ld.get("jobLocation", {})
                        if isinstance(loc, dict):
                            addr = loc.get("address", {})
                            if isinstance(addr, dict) and addr.get("addressCountry"):
                                job["country"] = addr["addressCountry"]
                        # Posted date from JSON-LD
                        if ld.get("datePosted") and not job.get("posted_date"):
                            job["posted_date"] = self._parse_date(ld["datePosted"])
                        # Deadline from JSON-LD
                        if ld.get("validThrough") and not job.get("deadline"):
                            job["deadline"] = self._parse_date(ld["validThrough"])
                except (ValueError, TypeError):
                    pass

            # Full description from CSS selectors if JSON-LD didn't provide one
            found_desc = bool(job.get("description") and len(job["description"]) > 50)
            if not found_desc:
                for sel in (
                    "div.job-description",
                    "div.job-description.c-cms-output",
                    "div.c-cms-output",
                    "div.job-details-nova",
                    "div[class*='job-description']",
                    "div.nova-legacy-o-stack",
                    "div.nova-o-stack",
                    "div[itemprop='description']",
                    "div.research-detail-middle-section",
                    "div[class*='description']",
                ):
                    desc_el = soup.select_one(sel)
                    if desc_el:
                        text = desc_el.get_text(separator="\n", strip=True)
                        if len(text) > 50:
                            job["description"] = text[:5000]
                            found_desc = True
                            break

            if not found_desc:
                fallback = self._extract_description_fallback(html_text)
                if fallback:
                    job["description"] = fallback

            # Institute (if not found via JSON-LD)
            if not job.get("institute"):
                inst_el = soup.select_one(
                    "a[href*='/institution/'], "
                    "span[itemprop='hiringOrganization']"
                )
                if inst_el:
                    job["institute"] = inst_el.get_text(strip=True)

            # Country
            if not job.get("country"):
                loc_el = soup.select_one(
                    "span[itemprop='addressCountry'], "
                    "span.location, div.location"
                )
                if loc_el:
                    job["country"] = self._extract_country(
                        loc_el.get_text(strip=True)
                    )

            # Deadline
            dl_el = soup.select_one(
                "span.deadline, div.application-deadline"
            )
            if dl_el:
                job["deadline"] = self._parse_date(dl_el.get_text(strip=True))

            # Department
            dept_el = soup.select_one(
                "span[itemprop='department'], span.department, "
                "div.department"
            )
            if dept_el and not job.get("department"):
                job["department"] = dept_el.get_text(strip=True)

            # Extract PI name, field, and deadline from description
            desc = job.get("description") or ""
            if desc:
                from src.matching.job_parser import (
                    extract_deadline,
                    extract_pi_name,
                    infer_field,
                )

                if not job.get("pi_name"):
                    pi = extract_pi_name(desc)
                    if pi:
                        job["pi_name"] = pi

                if not job.get("field"):
                    field = infer_field(desc)
                    if field:
                        job["field"] = field

                if not job.get("deadline"):
                    dl = extract_deadline(desc)
                    if dl:
                        job["deadline"] = dl

        except Exception:
            self.logger.debug("Could not parse detail for %s", url, exc_info=True)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        import requests as _requests

        all_jobs: list[dict[str, Any]] = []
        http_blocked = False

        for term in SEARCH_TERMS:
            if http_blocked:
                break  # Skip remaining HTTP attempts, go straight to Playwright

            self.logger.info("ResearchGate search: %s", term)

            for page in range(MAX_PAGES):
                try:
                    params = {
                        "query": term,
                        "page": str(page + 1),
                    }
                    resp = self.fetch(JOBS_URL, params=params)

                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break

                    self.logger.info(
                        "'%s' page %d: %d results", term, page + 1, len(page_jobs)
                    )
                    all_jobs.extend(page_jobs)

                except _requests.exceptions.HTTPError as e:
                    if e.response is not None and e.response.status_code == 403:
                        self.logger.info("ResearchGate 403, switching to Playwright")
                        http_blocked = True
                    else:
                        self.logger.warning(
                            "ResearchGate HTTP %s for '%s' page %d",
                            e.response.status_code if e.response else "?",
                            term,
                            page + 1,
                        )
                    break
                except Exception:
                    self.logger.exception(
                        "ResearchGate search failed: '%s' page %d",
                        term,
                        page + 1,
                    )
                    break

        # Playwright fallback if HTTP was blocked
        if http_blocked and not all_jobs:
            self.logger.info("Trying Playwright browser for ResearchGate...")
            try:
                from src.scrapers.browser import fetch_page
                import time as _time
                for term in SEARCH_TERMS:
                    url = f"{JOBS_URL}?query={term.replace(' ', '+')}&page=1"
                    html = fetch_page(url, wait_selector="div[class*='entity-item'], div.search-result-item", wait_ms=5000)
                    if html:
                        page_jobs = self._parse_listing_page(html)
                        if page_jobs:
                            self.logger.info("Playwright '%s': %d results", term, len(page_jobs))
                            all_jobs.extend(page_jobs)
                    _time.sleep(3.0)
            except ImportError:
                self.logger.warning("Playwright not installed")

        # Keyword filter (pre-enrichment, based on title only)
        candidates: list[dict[str, Any]] = []
        for job in all_jobs:
            blob = f"{job.get('title', '')} {job.get('description', '')}"
            if self._keyword_match(blob):
                candidates.append(job)

        # Parallel detail-page enrichment (skip already-in-DB)
        filtered = self._parallel_enrich(
            candidates, self._enrich_from_detail, max_workers=3,
        )

        self.logger.info(
            "ResearchGate: %d total, %d after filter", len(all_jobs), len(filtered)
        )
        return filtered

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        # Always match if "postdoc" is in the title
        if "postdoc" in lower:
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _extract_country(location: str | None) -> str | None:
        if not location:
            return None
        parts = [p.strip() for p in location.split(",")]
        if not parts:
            return None
        # ResearchGate format is "Country, City" — take first part
        # But if first part looks like a city (no known country match), try last part
        first = parts[0]
        from src.config import COUNTRY_TO_REGION
        if first in COUNTRY_TO_REGION or first.lower() in {
            k.lower() for k in COUNTRY_TO_REGION
        }:
            return first
        # Fallback: last part (traditional "City, Country" format)
        return parts[-1] if len(parts) > 1 else first

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None
