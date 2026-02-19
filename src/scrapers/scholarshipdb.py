"""ScholarshipDB scraper for postdoc opportunities.

ScholarshipDB aggregates postdoc positions from Nature Careers, university
job boards, and other sources.  The HTML structure uses:

Listing page (``/scholarships?q=...``):
    ``ul.list-unstyled > li`` items, each containing:
    - ``h4 > a`` — title + detail URL (``/jobs-in-{Country}/{Slug}={id}.html``)
    - 2nd ``div`` — institute link (``a[href*='/scholarships-at-']``),
      country link (``a[href*='/scholarships-in-']``), location span, time ago
    - 3rd ``div > p`` — snippet

Detail page (``/jobs-in-{Country}/{Slug}.html``):
    ``div.position-details`` containing:
    - ``h1`` — title
    - ``h2`` — institute + country
    - ``div.summary > span.col-sm-2`` labels (Updated, Location, Job Type)
    - remaining ``div`` children — full description
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from src.config import CV_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

BASE_URL = "https://scholarshipdb.net"

SEARCH_URL = f"{BASE_URL}/scholarships"
SEARCH_QUERIES = [
    "postdoc synthetic biology",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc microbiology",
    "postdoc metabolic engineering",
    "postdoc biotechnology",
    "postdoc biochemistry",
]

MAX_PAGES = 3

# Sources that are aggregators — real institute lives in the description
_AGGREGATOR_SOURCES = {
    "nature careers",
    "academic europe",
    "euraxess",
    "academic positions",
    "science careers",
    "times higher education",
    "inside higher ed",
    "higheredjobs",
    "chronicle of higher education",
}


class ScholarshipDBScraper(BaseScraper):
    """Scrape ScholarshipDB for postdoc fellowship and position opportunities."""

    rate_limit: float = 3.5  # ScholarshipDB frequently returns 520; be gentle

    @property
    def name(self) -> str:
        return "scholarshipdb"

    # ── Listing page ──────────────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a ScholarshipDB search results page.

        Results are ``<li>`` items inside ``<ul class="list-unstyled">``
        that contain an ``<h4>`` with the job title link.
        """
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # Only select li items that have an h4 (= actual result entries)
        result_items = [li for li in soup.select("li") if li.select_one("h4")]

        for li in result_items:
            job = self._parse_result_item(li)
            if job:
                jobs.append(job)

        return jobs

    def _parse_result_item(self, li: Tag) -> dict[str, Any] | None:
        """Parse a single ``<li>`` result item from the listing page."""
        # Title + link from h4 > a
        h4_a = li.select_one("h4 a")
        if not h4_a:
            return None

        href = h4_a.get("href", "")
        if not href:
            return None

        # get_text(separator=" ") prevents word concatenation from <b> tags
        title = h4_a.get_text(separator=" ", strip=True)
        title = re.sub(r"\s+", " ", title).strip()
        if not title or len(title) < 5:
            return None

        url = urljoin(BASE_URL, href)

        # All divs inside this li
        divs = li.select("div")

        # Second div: institute, country, location, time
        institute = ""
        country = ""
        location = ""
        posted_date = ""

        if len(divs) > 1:
            meta_div = divs[1]

            inst_link = meta_div.select_one("a[href*='/scholarships-at-']")
            if inst_link:
                institute = inst_link.get_text(strip=True)

            country_link = meta_div.select_one("a[href*='/scholarships-in-']")
            if country_link:
                country = country_link.get_text(strip=True)

            # Location is a span.text-success that is NOT a link
            for span in meta_div.select("span.text-success"):
                if span.name == "span" and not span.find("a"):
                    location = span.get_text(strip=True)
                    break

            time_span = meta_div.select_one("span.text-muted")
            if time_span:
                posted_date = self._relative_to_date(time_span.get_text(strip=True))

        # Third div: description snippet
        description = ""
        if len(divs) > 2:
            p = divs[2].select_one("p")
            if p:
                description = p.get_text(separator=" ", strip=True)

        return {
            "title": title,
            "institute": institute or None,
            "country": country or None,
            "location": location or None,
            "url": url,
            "posted_date": posted_date or None,
            "description": description or None,
            "source": self.name,
        }

    # ── Detail page ───────────────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page and extract full description + metadata."""
        url = job.get("url")
        if not url:
            return job

        try:
            resp = self.fetch(url)
            if resp.status_code != 200:
                self.logger.debug("Detail page %d: %s", resp.status_code, url)
                return job

            soup = BeautifulSoup(resp.text, "html.parser")

            # === Full description from div.position-details ===
            pos_detail = soup.select_one("div.position-details")
            if pos_detail:
                # The description is in the div children after summary
                desc_parts: list[str] = []
                skip_classes = {"summary", "mh-100", "space10", "row"}
                for child in pos_detail.children:
                    if not hasattr(child, "name") or not child.name:
                        continue
                    classes = set(child.get("class", []))
                    # Skip title, institute header, summary, and spacer divs
                    if child.name in ("h1", "h2", "h3"):
                        continue
                    if classes & skip_classes:
                        continue
                    text = child.get_text(separator="\n", strip=True)
                    if len(text) > 20:
                        desc_parts.append(text)

                if desc_parts:
                    full_desc = "\n\n".join(desc_parts)
                    if len(full_desc) > len(job.get("description") or ""):
                        job["description"] = full_desc[:5000]

                # === Metadata from div.summary ===
                summary_div = pos_detail.select_one("div.summary")
                if summary_div:
                    labels = summary_div.select("span.col-sm-2")
                    for label_span in labels:
                        label = label_span.get_text(strip=True).rstrip(":")
                        value_span = label_span.find_next_sibling("span")
                        if not value_span:
                            continue
                        value = value_span.get_text(strip=True)

                        if label == "Location" and value:
                            job["location"] = value
                        elif label == "Job Type" and value:
                            job["job_type"] = value
                        elif label == "Deadline" and value:
                            job["deadline"] = self._parse_date(value)

                # === Institute from h2 (overrides aggregator) ===
                h2 = pos_detail.select_one("h2")
                if h2:
                    inst_link = h2.select_one("a[href*='/scholarships-at-']")
                    if inst_link:
                        listed_source = inst_link.get_text(strip=True)
                        # If listed source is an aggregator, try to find real institute
                        if listed_source.lower() in _AGGREGATOR_SOURCES:
                            real_inst = self._infer_institute_from_desc(
                                job.get("description", "")
                            )
                            # Also try h2 full text (may contain real inst + aggregator)
                            if not real_inst:
                                real_inst = self._infer_institute_from_h2(h2, listed_source)
                            # Also try from other page elements
                            if not real_inst:
                                real_inst = self._infer_institute_from_page(pos_detail)
                            if real_inst:
                                job["institute"] = real_inst
                        elif not job.get("institute"):
                            job["institute"] = listed_source

                # === Fallback: infer institute from description if still empty ===
                if not job.get("institute"):
                    inferred = self._infer_institute_from_desc(
                        job.get("description", "")
                    )
                    if inferred:
                        job["institute"] = inferred

                # === Fallback: extract institute from URL slug ===
                if not job.get("institute"):
                    inferred = self._infer_institute_from_url(job.get("url", ""))
                    if inferred:
                        job["institute"] = inferred

            else:
                # Fallback: try generic content extraction
                fallback = self._extract_description_fallback(resp.text)
                if fallback and len(fallback) > len(job.get("description") or ""):
                    job["description"] = fallback

            # === Parse structured fields from description ===
            desc = job.get("description") or ""
            if desc:
                self._parse_fields_from_desc(job, desc)

            # === Country from description if still missing ===
            if not job.get("country") and desc:
                job["country"] = self._guess_country_from_text(desc)

        except Exception:
            self.logger.debug("Could not enrich detail: %s", url, exc_info=True)

        return job

    # ── Main scrape ───────────────────────────────────────────────────────

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for query in SEARCH_QUERIES:
            self.logger.info("ScholarshipDB search: %s", query)

            for page in range(MAX_PAGES):
                try:
                    params = {"q": query, "page": str(page + 1)}
                    resp = self.fetch(SEARCH_URL, params=params)
                    page_jobs = self._parse_listing_page(resp.text)
                    if not page_jobs:
                        break

                    # Deduplicate by URL across queries
                    new_jobs = []
                    for j in page_jobs:
                        if j["url"] not in seen_urls:
                            seen_urls.add(j["url"])
                            new_jobs.append(j)

                    self.logger.info(
                        "'%s' page %d: %d results (%d new)",
                        query, page + 1, len(page_jobs), len(new_jobs),
                    )
                    all_jobs.extend(new_jobs)

                except Exception:
                    self.logger.exception(
                        "ScholarshipDB failed: '%s' page %d", query, page + 1
                    )
                    break

        # Filter for relevance and remove fake listing pages
        filtered: list[dict[str, Any]] = []
        for job in all_jobs:
            # Skip fake category/listing pages (title is just "Postdoctoral"
            # and description starts with result counts like "813 postdoc...")
            title = (job.get("title") or "").strip()
            desc = (job.get("description") or "").strip()
            if title.lower() in ("postdoctoral", "postdoc", "post-doc"):
                if re.match(r'^\d', desc) or "Postdoctoral positions\nFilters" in desc:
                    continue
            blob = f"{title} {desc}"
            if self._keyword_match(blob):
                filtered.append(job)

        self.logger.info(
            "ScholarshipDB: %d total, %d after relevance filter",
            len(all_jobs), len(filtered),
        )

        # Enrich from detail pages (all jobs, not just first N)
        enriched = self._parallel_enrich(
            filtered, self._enrich_from_detail, max_workers=2,
        )

        # Remove fields not in the DB schema
        for job in enriched:
            job.pop("location", None)
            job.pop("job_type", None)

        self.logger.info(
            "ScholarshipDB: %d enriched jobs returned", len(enriched)
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        if "postdoc" in lower or "postdoctoral" in lower or "post-doc" in lower:
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        for fmt in (
            "%Y-%m-%d",
            "%B %d, %Y",
            "%d %B %Y",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y-%m-%dT%H:%M:%S",
        ):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        m = re.search(r"\d{4}-\d{2}-\d{2}", raw)
        return m.group(0) if m else None

    @staticmethod
    def _relative_to_date(text: str) -> str | None:
        """Convert '3 days ago' to ISO date string."""
        m = re.match(r"(\d+)\s+(day|hour|minute|week|month)s?\s+ago", text, re.IGNORECASE)
        if not m:
            return None
        n = int(m.group(1))
        unit = m.group(2).lower()
        if unit == "hour" or unit == "minute":
            delta = timedelta(hours=n if unit == "hour" else 0)
        elif unit == "day":
            delta = timedelta(days=n)
        elif unit == "week":
            delta = timedelta(weeks=n)
        elif unit == "month":
            delta = timedelta(days=n * 30)
        else:
            return None
        return (datetime.now() - delta).strftime("%Y-%m-%d")

    @staticmethod
    def _guess_country_from_text(text: str) -> str | None:
        from src.config import COUNTRY_TO_REGION

        lower = text.lower()
        for country in COUNTRY_TO_REGION:
            if country.lower() in lower:
                return country
        return None

    @staticmethod
    def _infer_institute_from_h2(h2: Tag, aggregator_name: str) -> str | None:
        """Extract real institute from h2 text when it contains aggregator + real name.

        h2 might contain: "Real Institute — Nature Careers" or
        "Some text Nature Careers at Real University"
        """
        full_text = h2.get_text(separator=" ", strip=True)
        # Remove the aggregator name
        cleaned = re.sub(re.escape(aggregator_name), "", full_text, flags=re.IGNORECASE).strip()
        cleaned = cleaned.strip("—–- ,|")
        if not cleaned or len(cleaned) < 5:
            return None

        # Check for university/institute pattern in remaining text
        m = re.search(
            r"((?:[A-Z][\w\'-]+\s+){0,4}"
            r"(?:University|Institute|College|Hospital|Centre|Center|Polytechnic)"
            r"(?:\s+(?:of|for|de)\s+(?:the\s+)?[A-Z][\w]+(?:\s+[A-Za-z]+){0,3})?)",
            cleaned,
        )
        if m and len(m.group(1).strip()) >= 10:
            return m.group(1).strip()
        return None

    @staticmethod
    def _infer_institute_from_page(pos_detail: Tag) -> str | None:
        """Try to extract institute from other page elements (links, metadata)."""
        # Check for employer/institution links elsewhere on the page
        for link in pos_detail.select("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)
            # Skip aggregator and generic links
            if any(agg in text.lower() for agg in ("nature", "academic", "euraxess", "science careers")):
                continue
            if "scholarships-at-" in href and len(text) >= 5:
                return text
        return None

    @staticmethod
    def _infer_institute_from_url(url: str) -> str | None:
        """Extract institute name from ScholarshipDB URL slug.

        URLs follow the pattern:
        ``/jobs-in-{Country}/{Title}-{Institute}={id}.html``
        e.g. ``/Postdoc-In-Ethology-Link-ping-University=98frk0wL8RG...``
        """
        if not url or "scholarshipdb.net" not in url:
            return None

        # Extract the slug part before the =id
        m = re.search(r"/([^/]+)=[^/]+\.html$", url)
        if not m:
            return None

        slug = m.group(1)
        # Look for university/institute keywords at the end of the slug
        inst_pattern = re.search(
            r"((?:[A-Z][a-z-]+[-]){0,4}"
            r"(?:University|Universit[aey]t[a-z]*|Institut[eo]?[a-z]*|"
            r"College|Hospital|Polytechnic|Academy|"
            r"Helmholtz|Max-Planck|Leibniz|"
            r"Research-(?:Center|Centre|Network|Institute))"
            r"(?:[-][A-Za-z-]+){0,5})"
            r"$",
            slug,
        )
        if not inst_pattern:
            return None

        raw = inst_pattern.group(1)
        # Convert URL slug to proper name: hyphens → spaces
        name = raw.replace("-", " ").strip()
        if len(name) < 5:
            return None
        # Skip if it matches an aggregator name
        if name.lower() in _AGGREGATOR_SOURCES:
            return None
        return name

    @staticmethod
    def _infer_institute_from_desc(description: str) -> str | None:
        """Extract real institute name from a job description.

        Used when the listed source is an aggregator (Nature Careers etc.).
        Also extracts from EURAXESS-style "Organisation/Company" fields.
        """
        if not description:
            return None

        # Pattern 0: EURAXESS-style "Organisation/Company\nSome Institute Name"
        org_match = re.search(
            r'Organisation/Company\s*\n?\s*(.+?)(?:\n|Research Field|Department|$)',
            description,
        )
        if org_match:
            org_name = org_match.group(1).strip()
            if len(org_name) >= 3 and org_name.lower() not in (
                "n/a", "unknown", "various", "multiple",
            ):
                return org_name

        # Pattern 1: "The University of X" or "X University"
        patterns = [
            # "University of Luxembourg", "Stanford University"
            r"(?:The\s+)?((?:[A-Z][a-z]+\s+){0,3}University"
            r"\s+of\s+(?:the\s+)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})",
            # "Stanford University", "Aarhus University" (Name + University)
            r"(?:The\s+)?((?:[A-Z][a-z]+\s+){1,3}University)",
            # "Massachusetts Institute of Technology"
            r"((?:[A-Z][a-z]+\s+){1,3}Institute"
            r"(?:\s+of\s+[A-Z][a-z]+(?:\s+[A-Za-z]+){0,2})?)",
            # "Max Planck Institute", "German Cancer Research Center"
            r"((?:[A-Z][a-z]+\s+){1,3}"
            r"(?:Research\s+)?(?:Center|Centre|Institut|Laboratory))",
        ]

        noise = {"about", "location", "position", "description", "overview"}

        for pattern in patterns:
            matches = re.findall(pattern, description)
            for candidate in matches:
                candidate = candidate.strip().rstrip(",. ")
                if len(candidate) < 10:
                    continue
                first_word = candidate.split()[0].lower()
                if first_word in noise:
                    continue
                return candidate

        return None

    @staticmethod
    def _parse_fields_from_desc(job: dict, desc: str) -> None:
        """Extract PI name, deadline, and field from description text."""
        from src.matching.job_parser import (
            extract_pi_name,
            extract_deadline,
            infer_field,
        )

        if not job.get("pi_name"):
            pi = extract_pi_name(desc)
            if pi:
                job["pi_name"] = pi

        if not job.get("deadline"):
            dl = extract_deadline(desc)
            if dl:
                job["deadline"] = dl

        if not job.get("field"):
            field = infer_field(desc)
            if field:
                job["field"] = field
