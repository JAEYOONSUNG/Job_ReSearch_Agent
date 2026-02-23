"""EURAXESS scraper – European postdoc positions in Life Sciences.

EURAXESS uses the ECL (Europa Component Library) design system:

Listing page (``/jobs/search?keywords=...``):
    ``article.ecl-content-item`` cards, each containing:
    - ``h3.ecl-content-block__title > a`` — title + detail URL
    - ``li.ecl-content-block__primary-meta-item > a`` — organisation
    - ``div.id-Application-Deadline`` — deadline
    - ``div.id-Work-Locations`` — country/city
    - ``div.id-Research-Field`` — research field

Detail page (``/jobs/{id}``):
    ``dl.ecl-description-list`` with dt/dd pairs under section headings:
    - ``h2#job-information`` — Organisation, Department, Research Field,
      Application Deadline (``<time datetime="...">``) , Country, Contract Type
    - ``h2#offer-description`` — free-text description
    - ``h2#requirements`` — education, skills, specific requirements
    - ``h2#additional-information`` — benefits, eligibility, selection
    - ``h2#work-locations`` — Company/Institute, Country, City
"""

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

BASE_URL = "https://euraxess.ec.europa.eu"
SEARCH_URL = f"{BASE_URL}/jobs/search"

# Sources that are aggregators — real institute should come from detail page
_AGGREGATOR_SOURCES = {
    "academictransfer",
    "eurosciencejobs",
    "jobrxiv",
    "jobrxiv.org",
    "times higher education",
    "academic positions",
    "nature careers",
    "science careers",
    "talentech",
}

SEARCH_KEYWORDS = [
    # Field-specific
    "postdoc biology",
    "postdoctoral life sciences",
    "postdoc CRISPR",
    "postdoc protein engineering",
    "postdoc synthetic biology",
    "postdoctoral microbiology",
    "postdoc chemistry biology",
    # Institution-specific (premier EU/UK research institutes)
    "EMBL postdoc",
    "Max Planck biology postdoc",
    "Sanger Institute",
    "Pasteur Institute postdoc",
    "Francis Crick postdoc",
]

MAX_PAGES = 3


class EuraxessScraper(BaseScraper):
    """Scrape EURAXESS for postdoc positions in life sciences across Europe."""

    rate_limit: float = 5.0

    @property
    def name(self) -> str:
        return "euraxess"

    # ── Listing page parsing ──────────────────────────────────────────────

    def _parse_listing_page(self, html: str) -> list[dict[str, Any]]:
        """Parse a EURAXESS search results page."""
        soup = BeautifulSoup(html, "html.parser")
        jobs: list[dict[str, Any]] = []

        # ECL design: each result is article.ecl-content-item
        articles = soup.select("article.ecl-content-item")

        if not articles:
            # Fallback: look for any links to job detail pages
            for link in soup.select("a[href*='/jobs/']"):
                href = link.get("href", "")
                if not href or "/jobs/search" in href:
                    continue
                if re.search(r"/jobs/\d+", href):
                    full_url = urljoin(BASE_URL, href)
                    title = link.get_text(strip=True)
                    if title and len(title) > 5:
                        jobs.append({"title": title, "url": full_url, "source": self.name})
            return jobs

        for article in articles:
            job = self._parse_listing_card(article)
            if job:
                jobs.append(job)

        return jobs

    def _parse_listing_card(self, article: Tag) -> dict[str, Any] | None:
        """Extract job info from a single ECL content-item article."""
        # Title + link
        title_el = article.select_one(
            "h3.ecl-content-block__title a, "
            "h2.ecl-content-block__title a, "
            "a[href*='/jobs/']"
        )
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        href = title_el.get("href", "")
        if not href or not title:
            return None
        url = urljoin(BASE_URL, href)

        # Organisation from primary-meta-item
        institute = None
        meta_items = article.select("li.ecl-content-block__primary-meta-item")
        for item in meta_items:
            link = item.select_one("a")
            if link and "/organisations/" in link.get("href", ""):
                institute = link.get_text(strip=True)
                break
            # Sometimes org is in text without a link
            text = item.get_text(strip=True)
            if text and not text.startswith("Posted on"):
                institute = text
                break

        # Application Deadline
        deadline = None
        dl_div = article.select_one("div.id-Application-Deadline")
        if dl_div:
            dl_text = dl_div.get_text(strip=True)
            deadline = self._parse_date_text(dl_text)

        # Work Locations → country
        country = None
        loc_div = article.select_one("div.id-Work-Locations")
        if loc_div:
            loc_text = loc_div.get_text(strip=True)
            country = self._extract_country_from_locations(loc_text)

        # Research Field
        field = None
        field_div = article.select_one("div.id-Research-Field")
        if field_div:
            # Use separator to prevent word concatenation from sibling elements
            field = field_div.get_text(separator="|", strip=True)
            # Clean: remove "Research Field" label
            field = re.sub(r"^Research\s*Field:?\s*\|?\s*", "", field, flags=re.IGNORECASE).strip()
            # Multiple fields concatenated with » — take the most specific
            field = self._clean_research_field(field)

        # Description snippet
        desc_el = article.select_one("div.ecl-content-block__description")
        description = desc_el.get_text(strip=True) if desc_el else None

        return {
            "title": title,
            "institute": institute,
            "country": country,
            "url": url,
            "deadline": deadline,
            "description": description,
            "field": field,
            "source": self.name,
        }

    # ── Detail page enrichment ─────────────────────────────────────────────

    def _enrich_from_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch the detail page and extract structured fields from dl/dt/dd."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url)
            if resp.status_code != 200:
                self.logger.debug("EURAXESS detail %d: %s", resp.status_code, url)
                return job

            soup = BeautifulSoup(resp.text, "html.parser")

            # === Extract dl/dt/dd fields from h2#job-information ===
            job_info = self._extract_dl_fields(soup, "job-information")

            # === Extract PI from structured fields ===
            if not job.get("pi_name") and job_info:
                for pi_key in ("Contact Person", "Supervisor",
                               "Principal Investigator", "Group Leader",
                               "Lab Head"):
                    pi_candidate = job_info.get(pi_key, "")
                    if pi_candidate:
                        from src.matching.job_parser import _is_valid_name
                        # Strip email addresses and phone numbers
                        clean = re.sub(r'\S+@\S+', '', pi_candidate).strip()
                        clean = re.sub(r'[\d\+\(\)]{5,}', '', clean).strip()
                        if _is_valid_name(clean):
                            job["pi_name"] = clean
                            break

            if job_info:
                detail_inst = job_info.get("Organisation/Company", "")
                if detail_inst:
                    current_inst = (job.get("institute") or "").lower()
                    # Override if no institute or current is an aggregator
                    if not current_inst or current_inst in _AGGREGATOR_SOURCES:
                        job["institute"] = detail_inst
                if not job.get("department") and job_info.get("Department"):
                    job["department"] = job_info["Department"]
                if not job.get("country") and job_info.get("Country"):
                    job["country"] = job_info["Country"]
                if not job.get("field") and job_info.get("Research Field"):
                    job["field"] = self._clean_research_field(job_info["Research Field"])

                # Deadline from <time datetime="..."> element
                if not job.get("deadline"):
                    dl_term = self._find_dt(soup, "job-information", "Application Deadline")
                    if dl_term:
                        dd = dl_term.find_next_sibling("dd")
                        if dd:
                            time_el = dd.select_one("time[datetime]")
                            if time_el:
                                job["deadline"] = self._parse_date_text(
                                    time_el["datetime"]
                                )
                            else:
                                job["deadline"] = self._parse_date_text(
                                    dd.get_text(strip=True)
                                )

                # Start date
                start_term = self._find_dt(soup, "job-information", "Offer Starting Date")
                if start_term:
                    dd = start_term.find_next_sibling("dd")
                    if dd:
                        time_el = dd.select_one("time[datetime]")
                        start_str = time_el["datetime"] if time_el else dd.get_text(strip=True)
                        start_date = self._parse_date_text(start_str)
                        if start_date:
                            existing = job.get("conditions") or ""
                            job["conditions"] = f"{existing} | Start: {start_date}".strip(" |")

                # Contract type + job status → conditions
                cond_parts = []
                for key in ("Type of Contract", "Job Status", "Hours Per Week"):
                    if job_info.get(key):
                        cond_parts.append(f"{key}: {job_info[key]}")
                if job_info.get("Researcher Profile"):
                    # Profile values may be concatenated without separator
                    profile = re.sub(
                        r"(Researcher \(R\d\))", r"\1, ",
                        job_info["Researcher Profile"],
                    ).rstrip(", ")
                    cond_parts.append(f"Profile: {profile}")
                if cond_parts:
                    existing = job.get("conditions") or ""
                    new_cond = " | ".join(cond_parts)
                    job["conditions"] = f"{existing} | {new_cond}".strip(" |")

            # === Full description ===
            # Regular jobs use h2#offer-description; hosting offers use h2#description
            desc_section = (
                soup.find("h2", id="offer-description")
                or soup.find("h2", id="description")
            )
            if desc_section:
                desc_parts = []
                for sibling in desc_section.find_next_siblings():
                    # Stop at next section heading (h2, h3, etc.)
                    if sibling.name and sibling.name in ("h2", "h3", "h4"):
                        break
                    text = sibling.get_text(separator="\n", strip=True)
                    if text:
                        desc_parts.append(text)
                # Also check div.ecl children within parent
                if not desc_parts and desc_section.parent:
                    for div in desc_section.parent.select("div.ecl"):
                        text = div.get_text(separator="\n", strip=True)
                        if len(text) > 30:
                            desc_parts.append(text)

                full_desc = "\n\n".join(desc_parts)
                if len(full_desc) > len(job.get("description") or ""):
                    job["description"] = full_desc[:15000]

            # Fallback description: also retry if description is truncated ('...')
            desc_val = job.get("description") or ""
            if not desc_val or len(desc_val) < 50 or desc_val.rstrip().endswith("..."):
                fallback = self._extract_description_fallback(resp.text)
                if fallback and len(fallback) > len(desc_val):
                    job["description"] = fallback

            # === Requirements from h2#requirements ===
            req_fields = self._extract_dl_fields(soup, "requirements")
            req_section = soup.find("h2", id="requirements")
            if req_section:
                req_parts = []
                parent = req_section.parent
                if parent:
                    # Get bold labels + content
                    for label_div in parent.select("div.ecl-u-type-bold"):
                        label = label_div.get_text(strip=True)
                        content_div = label_div.find_next_sibling("div")
                        if content_div:
                            content = content_div.get_text(separator="\n", strip=True)
                            if content:
                                req_parts.append(f"{label}: {content}")
                    # Also add Education Level from dl
                    if req_fields and req_fields.get("Education Level"):
                        req_parts.insert(0, f"Education: {req_fields['Education Level']}")
                if req_parts:
                    job["requirements"] = "\n".join(req_parts)[:2000]

            # === Work locations — get institute if still missing ===
            work_loc = self._extract_dl_fields(soup, "work-locations")
            if work_loc:
                wl_inst = work_loc.get("Company/Institute", "")
                if wl_inst:
                    current_inst = (job.get("institute") or "").lower()
                    if not current_inst or current_inst in _AGGREGATOR_SOURCES:
                        job["institute"] = wl_inst
                if not job.get("country") and work_loc.get("Country"):
                    job["country"] = work_loc["Country"]

            # === Additional info → benefits/conditions ===
            addl_section = soup.find("h2", id="additional-information")
            if addl_section:
                parent = addl_section.parent
                if parent:
                    for label_div in parent.select("div.ecl-u-type-bold"):
                        label = label_div.get_text(strip=True)
                        if label.lower() in ("benefits", "eligibility criteria"):
                            content_div = label_div.find_next_sibling("div")
                            if content_div:
                                content = content_div.get_text(separator=" ", strip=True)
                                if content and len(content) > 10:
                                    existing = job.get("conditions") or ""
                                    job["conditions"] = f"{existing} | {label}: {content[:300]}".strip(" |")

            # === Parse PI from description ===
            desc = job.get("description") or ""
            if desc and not job.get("pi_name"):
                from src.matching.job_parser import extract_pi_name
                pi = extract_pi_name(desc)
                if pi:
                    job["pi_name"] = pi

            # === Infer field from description if still missing ===
            if desc and not job.get("field"):
                from src.matching.job_parser import infer_field
                field = infer_field(desc)
                if field:
                    job["field"] = field

        except Exception:
            self.logger.debug("Could not enrich EURAXESS detail: %s", url, exc_info=True)

        return job

    # ── Main scrape logic ─────────────────────────────────────────────────

    def _get_search_url(self, keyword: str) -> str | None:
        """POST the search form to get a keyword-filtered URL.

        EURAXESS requires a POST with form tokens; the server redirects
        to ``/jobs/search?f[0]=keywords:<kw>`` which can then be paginated.
        """
        try:
            resp = self.fetch(SEARCH_URL)
            soup = BeautifulSoup(resp.text, "html.parser")

            form = soup.select_one('form[action="/jobs/search"]')
            if not form:
                return None
            fbi = form.select_one('input[name="form_build_id"]')
            fi = form.select_one('input[name="form_id"]')
            if not fbi or not fi:
                return None

            resp2 = self.fetch(
                SEARCH_URL,
                method="POST",
                data={
                    "keywords": keyword,
                    "form_build_id": fbi["value"],
                    "form_id": fi["value"],
                },
            )
            # The POST redirects to a URL like /jobs/search?f[0]=keywords:...
            return resp2.url  # type: ignore[return-value]
        except Exception:
            self.logger.debug("Failed to get search URL for '%s'", keyword, exc_info=True)
            return None

    def scrape(self) -> list[dict[str, Any]]:
        all_jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("EURAXESS search: %s", keyword)

            search_url = self._get_search_url(keyword)
            if not search_url:
                self.logger.warning("Could not get EURAXESS search URL for '%s'", keyword)
                continue

            for page in range(MAX_PAGES):
                try:
                    page_url = f"{search_url}&page={page}" if "?" in search_url else f"{search_url}?page={page}"
                    resp = self.fetch(page_url)
                except Exception:
                    self.logger.exception(
                        "Failed to fetch EURAXESS '%s' page %d", keyword, page
                    )
                    break

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
                    keyword, page, len(page_jobs), len(new_jobs),
                )
                all_jobs.extend(new_jobs)

        # Enrich from detail pages
        all_enriched = self._parallel_enrich(
            all_jobs, self._enrich_from_detail, max_workers=3,
        )

        # Keyword filter on enriched results
        enriched: list[dict[str, Any]] = []
        for job in all_enriched:
            blob = f"{job.get('title', '')} {(job.get('description') or '')} {job.get('field', '')}"
            if self._keyword_match(blob):
                enriched.append(job)

        self.logger.info(
            "EURAXESS: %d total, %d after keyword filter", len(all_jobs), len(enriched)
        )
        return enriched

    # ── Helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def _extract_dl_fields(soup: BeautifulSoup, section_id: str) -> dict[str, str]:
        """Extract dt/dd pairs from a dl list under h2#section_id."""
        h2 = soup.find("h2", id=section_id)
        if not h2:
            return {}

        parent = h2.parent
        if not parent:
            return {}

        dl = parent.find("dl", class_=re.compile(r"ecl-description-list"))
        if not dl:
            return {}

        fields: dict[str, str] = {}
        dts = dl.find_all("dt", class_=re.compile(r"ecl-description-list__term"))
        dds = dl.find_all("dd", class_=re.compile(r"ecl-description-list__definition"))
        for dt, dd in zip(dts, dds):
            key = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if key and value:
                fields[key] = value

        return fields

    @staticmethod
    def _find_dt(soup: BeautifulSoup, section_id: str, term: str) -> Tag | None:
        """Find a specific dt element by term text under a section."""
        h2 = soup.find("h2", id=section_id)
        if not h2 or not h2.parent:
            return None
        dl = h2.parent.find("dl", class_=re.compile(r"ecl-description-list"))
        if not dl:
            return None
        for dt in dl.find_all("dt"):
            if term.lower() in dt.get_text(strip=True).lower():
                return dt
        return None

    @staticmethod
    def _extract_country_from_locations(text: str) -> str | None:
        """Extract country name from work location text like 'Number of offers: 1, France, LILLE'."""
        from src.config import COUNTRY_TO_REGION

        # Split by comma and check each part against known countries
        parts = [p.strip() for p in text.split(",")]
        for part in parts:
            # Clean up common prefixes
            clean = re.sub(r"Number of offers:\s*\d+", "", part).strip()
            if not clean:
                continue
            for country in COUNTRY_TO_REGION:
                if clean.lower() == country.lower():
                    return country
        return None

    @staticmethod
    def _clean_research_field(raw: str) -> str:
        """Clean concatenated research field values like 'Biological sciences»Biology...'."""
        if not raw:
            return raw
        # Split on », ›, or | separators
        parts = re.split(r"[»›|]", raw)
        # Flatten and deduplicate, prefer specific sub-fields
        seen: set[str] = set()
        cleaned: list[str] = []
        for part in parts:
            part = part.strip().strip(",. ")
            if not part or part.lower() in seen:
                continue
            seen.add(part.lower())
            cleaned.append(part)
        # Group into parent»child pairs — keep child (more specific)
        specific: list[str] = []
        for i, c in enumerate(cleaned):
            lc = c.lower()
            # If another entry starts with this one, this is the parent — skip
            if any(
                other.lower().startswith(lc) and other != c
                for other in cleaned
            ):
                continue
            specific.append(c)
        if not specific:
            specific = cleaned
        return ", ".join(specific[:3])

    @staticmethod
    def _keyword_match(text: str) -> bool:
        lower = text.lower()
        if any(term in lower for term in ("postdoc", "postdoctoral", "post-doc")):
            return True
        return any(kw.lower() in lower for kw in CV_KEYWORDS)

    @staticmethod
    def _parse_date_text(raw: str) -> str | None:
        """Parse European and ISO date formats."""
        if not raw:
            return None
        raw = raw.strip()
        # ISO datetime: "2026-03-16T17:00:00+00:00"
        m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
        if m:
            return m.group(1)
        for fmt in (
            "%d/%m/%Y",
            "%d %B %Y",
            "%d %b %Y",
            "%B %d, %Y",
            "%d %b %Y - %H:%M (UTC)",
            "%d %B %Y - %H:%M (UTC)",
        ):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try extracting "16 Mar 2026" from longer text
        dm = re.search(r"(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})", raw)
        if dm:
            for fmt in ("%d %b %Y",):
                try:
                    return datetime.strptime(f"{dm.group(1)} {dm.group(2)} {dm.group(3)}", fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
        return None
