"""Institutional portal scrapers — direct scraping of major research institute career pages.

Covers 6 portals that are server-side rendered and scrapable:
- Max Planck Society (MPG) RSS job feed
- Max Planck PostDoc Program listing
- Institut Pasteur job board
- ETH Zurich job board
- Weizmann Institute open positions
- Broad Institute careers

Portals that are JS SPAs or block HTTP (Helmholtz, Salk, Technion, Leibniz,
EMBL, Francis Crick, EPFL) are covered by aggregator scrapers + search keywords.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Bio/life science keywords for filtering general feeds
_BIO_KEYWORDS = {
    "biology", "biological", "biochem", "biotech", "bioinformatics",
    "molecular", "cell", "genetic", "genomic", "microbio", "protein",
    "crispr", "synthetic", "metabol", "enzyme", "immunol", "neuro",
    "evolution", "ecology", "chemistry", "chemical", "life science",
}

# Topics that are clearly outside bio/life sciences
_EXCLUDE_TOPICS = {
    "astrophysics", "astronomy", "quantum physics", "condensed matter",
    "particle physics", "high energy physics", "plasma physics",
    "law", "political science", "linguistics", "architecture",
    "mechanical engineering", "aerospace engineering", "electrical engineering",
    "art history", "musicology", "theology", "accounting",
    "materials science", "meteorology", "geophysics", "seismology",
    "nuclear physics", "cosmology", "dark matter", "string theory",
    "fluid dynamics", "optics", "photonics", "semiconductor",
}


def _is_bio_related(text: str) -> bool:
    """Check if text mentions biology/life-science topics.

    Requires at least 1 bio keyword hit AND rejects if >=2 exclude topic hits.
    """
    lower = text.lower()
    if not any(kw in lower for kw in _BIO_KEYWORDS):
        return False
    exclude_hits = sum(1 for kw in _EXCLUDE_TOPICS if kw in lower)
    if exclude_hits >= 2:
        return False
    return True


def _parse_date(raw: str | None) -> str | None:
    """Try to parse various date formats into YYYY-MM-DD."""
    if not raw:
        return None
    raw = raw.strip()
    # ISO date
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%d %b %Y",
        "%d %B %Y",
        "%B %d, %Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S%z",
    ):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class InstitutionalPortalScraper(BaseScraper):
    """Scrape career portals of major research institutes directly."""

    rate_limit: float = 3.0

    @property
    def name(self) -> str:
        return "institutional"

    def scrape(self) -> list[dict[str, Any]]:
        jobs: list[dict[str, Any]] = []
        for method_name, label in [
            (self._scrape_mpg_rss, "MPG RSS"),
            (self._scrape_mpg_postdoc_program, "MPG PostDoc Program"),
            (self._scrape_pasteur, "Pasteur"),
            (self._scrape_ethz, "ETH Zurich"),
            (self._scrape_weizmann, "Weizmann"),
            (self._scrape_broad, "Broad Institute"),
            (self._scrape_wyss, "Wyss Institute"),
        ]:
            try:
                result = method_name()
                self.logger.info("%s: %d jobs", label, len(result))
                jobs.extend(result)
            except Exception:
                self.logger.exception("Failed to scrape %s", label)
        return jobs

    # ── Max Planck RSS ─────────────────────────────────────────────────

    def _scrape_mpg_rss(self) -> list[dict[str, Any]]:
        """Parse the MPG jobs RSS feed, filtering for bio/life-science postdocs."""
        try:
            import feedparser
        except ImportError:
            self.logger.warning("feedparser not installed; skipping MPG RSS")
            return []

        url = "https://www.mpg.de/feeds/jobs.rss"
        jobs: list[dict[str, Any]] = []
        try:
            resp = self.fetch(url, timeout=30)
            feed = feedparser.parse(resp.text)
        except Exception:
            self.logger.exception("Failed to fetch MPG RSS")
            return []

        for entry in feed.entries[:50]:
            title = entry.get("title", "")
            description = entry.get("summary", "") or entry.get("description", "")
            link = entry.get("link", "")

            # Filter: only bio/life science and postdoc-related
            blob = f"{title} {description}"
            if not _is_bio_related(blob):
                continue

            # Extract institute from title (e.g. "MPI of Biochemistry - Postdoc")
            institute = "Max Planck Society"
            mpi_match = re.search(
                r"((?:Max Planck|MPI)\s+(?:Institute\s+)?(?:of|for|für)\s+[\w\s&-]+)",
                title,
                re.IGNORECASE,
            )
            if mpi_match:
                institute = mpi_match.group(1).strip()

            posted = _parse_date(entry.get("published", ""))

            jobs.append({
                "title": title,
                "url": link,
                "description": description[:5000],
                "institute": institute,
                "country": "Germany",
                "posted_date": posted,
                "source": self.name,
            })

        # Enrich with full detail pages
        if jobs:
            jobs = self._parallel_enrich(
                jobs, self._enrich_mpg_detail, max_workers=3, limit=30,
            )
        return jobs

    # ── Max Planck PostDoc Program ─────────────────────────────────────

    def _scrape_mpg_postdoc_program(self) -> list[dict[str, Any]]:
        """Scrape the MPG PostDoc Program table of open positions."""
        url = "https://postdocprogram.mpg.de/all-postdoc-positions"
        jobs: list[dict[str, Any]] = []
        try:
            resp = self.fetch(url, timeout=30)
        except Exception:
            self.logger.exception("Failed to fetch MPG PostDoc Program")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.select_one("table.views-table")
        if not table:
            self.logger.warning("MPG PostDoc Program: table not found")
            return []

        for row in table.select("tbody tr"):
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            # Project title + link
            title_cell = row.select_one("td.views-field-ocm-pp-project-title")
            if not title_cell:
                continue
            link = title_cell.select_one("a")
            title = link.get_text(strip=True) if link else title_cell.get_text(strip=True)
            detail_url = urljoin(url, link["href"]) if link and link.get("href") else ""

            # Institute
            inst_cell = row.select_one("td.views-field-ocm-pp-mpi-container")
            institute = inst_cell.get_text(strip=True) if inst_cell else "Max Planck Society"

            # PI name
            pi_cell = row.select_one("td.views-field-ocm-pp-principal-investigator")
            pi_name = pi_cell.get_text(strip=True) if pi_cell else None

            # Field
            section_cell = row.select_one("td.views-field-ocm-pp-section")
            keywords_cell = row.select_one("td.views-field-ocm-pp-key-words")
            field_parts = []
            if section_cell:
                field_parts.append(section_cell.get_text(strip=True))
            if keywords_cell:
                field_parts.append(keywords_cell.get_text(strip=True))
            field = ", ".join(filter(None, field_parts)) or None

            # City
            city_cell = row.select_one("td.views-field-ocm-pp-city")
            city = city_cell.get_text(strip=True) if city_cell else None

            job = {
                "title": title,
                "url": detail_url,
                "institute": institute,
                "pi_name": pi_name,
                "field": field,
                "country": "Germany",
                "source": self.name,
            }
            if city:
                job["conditions"] = f"City: {city}"
            jobs.append(job)

        # Enrich with full detail pages
        if jobs:
            jobs = self._parallel_enrich(
                jobs, self._enrich_mpg_detail, max_workers=3, limit=30,
            )
        return jobs

    # ── Institut Pasteur ───────────────────────────────────────────────

    def _scrape_pasteur(self) -> list[dict[str, Any]]:
        """Scrape Institut Pasteur postdoc job listings."""
        url = "https://research.pasteur.fr/en/jobs/?type=post-doc"
        jobs: list[dict[str, Any]] = []
        try:
            resp = self.fetch(url, timeout=30)
        except Exception:
            self.logger.exception("Failed to fetch Pasteur")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.select("article.card.medium")
        if not articles:
            # Fallback: any article with card class
            articles = soup.select("article.card")

        for article in articles:
            link = article.select_one("a[href]")
            if not link:
                continue
            detail_url = link.get("href", "")
            if not detail_url.startswith("http"):
                detail_url = urljoin(url, detail_url)

            title_el = article.select_one("h3")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                title = link.get_text(strip=True)

            # Lab/team info
            lab_el = article.select_one("span.lab")
            lab = lab_el.get_text(strip=True) if lab_el else None

            # Position type
            label_el = article.select_one("div.label")
            pos_type = label_el.get_text(strip=True) if label_el else None

            job: dict[str, Any] = {
                "title": title,
                "url": detail_url,
                "institute": "Institut Pasteur",
                "country": "France",
                "source": self.name,
            }
            if lab:
                job["department"] = lab
            if pos_type:
                job["conditions"] = pos_type

            jobs.append(job)

        # Enrich with detail pages
        enriched = self._parallel_enrich(
            jobs, self._enrich_pasteur_detail, max_workers=3, limit=30,
        )
        return enriched

    def _enrich_pasteur_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch Pasteur detail page for full description."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")
            # Try main content area
            main = soup.select_one("div.entry-content, article, main")
            if main:
                desc = main.get_text(separator="\n", strip=True)
                if len(desc) > len(job.get("description") or ""):
                    job["description"] = desc[:15000]
            elif not job.get("description"):
                fallback = self._extract_description_fallback(resp.text)
                if fallback:
                    job["description"] = fallback

            # Extract deadline + application materials from enriched description
            enriched_desc = job.get("description") or ""
            if enriched_desc:
                from src.matching.job_parser import extract_deadline, extract_application_materials
                if not job.get("deadline"):
                    dl = extract_deadline(enriched_desc)
                    if dl:
                        job["deadline"] = dl
                if not job.get("application_materials"):
                    app_mat = extract_application_materials(enriched_desc)
                    if app_mat:
                        job["application_materials"] = app_mat
        except Exception:
            self.logger.debug("Failed to enrich Pasteur detail: %s", url)
        return job

    # ── ETH Zurich ─────────────────────────────────────────────────────

    def _scrape_ethz(self) -> list[dict[str, Any]]:
        """Scrape ETH Zurich job board for postdoc positions."""
        url = "https://jobs.ethz.ch/"
        jobs: list[dict[str, Any]] = []
        try:
            resp = self.fetch(
                url,
                method="POST",
                data={"JobSearch[jobtype_id]": "2"},  # 2 = postdoc
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=30,
            )
        except Exception:
            self.logger.exception("Failed to fetch ETH Zurich jobs")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.job-ad__item__wrapper")
        if not items:
            # Fallback selectors
            items = soup.select("div.job-ad__item, li.job-item")

        for item in items:
            title_el = item.select_one("h3.job-ad__item__title, h3")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)

            link = item.select_one("a.job-ad__item__link, a[href]")
            detail_url = ""
            if link and link.get("href"):
                detail_url = urljoin(url, link["href"])

            # Details: workload, location
            details_el = item.select_one("div.job-ad__item__details")
            details = details_el.get_text(strip=True) if details_el else None

            # Department and date
            company_el = item.select_one("div.job-ad__item__company")
            department = company_el.get_text(strip=True) if company_el else None

            job: dict[str, Any] = {
                "title": title,
                "url": detail_url,
                "institute": "ETH Zurich",
                "country": "Switzerland",
                "source": self.name,
            }
            if department:
                job["department"] = department
            if details:
                job["conditions"] = details

            # Filter: only bio/chemistry/life-science related
            blob = f"{title} {department or ''} {details or ''}"
            if _is_bio_related(blob):
                jobs.append(job)

        return jobs

    # ── Weizmann Institute ─────────────────────────────────────────────

    def _scrape_weizmann(self) -> list[dict[str, Any]]:
        """Scrape Weizmann Institute open postdoc positions."""
        url = "https://www.weizmann.ac.il/wsos/positions/post"
        jobs: list[dict[str, Any]] = []
        try:
            resp = self.fetch(url, timeout=30)
        except Exception:
            self.logger.exception("Failed to fetch Weizmann positions")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.mix.position-wrapper")
        if not items:
            # Fallback
            items = soup.select("li.position-wrapper, div.position-wrapper")

        for item in items:
            # PI name + link
            pi_el = item.select_one("h2.sci-name-wrapper a, h2.sci-name-wrapper")
            pi_name = pi_el.get_text(strip=True) if pi_el else None
            pi_link = ""
            if pi_el and pi_el.name == "a":
                pi_link = urljoin(url, pi_el.get("href", ""))
            elif pi_el:
                inner_a = pi_el.select_one("a")
                if inner_a:
                    pi_name = inner_a.get_text(strip=True)
                    pi_link = urljoin(url, inner_a.get("href", ""))

            # Department
            dept_el = item.select_one("div.department-name-wrapper")
            department = dept_el.get_text(strip=True) if dept_el else None

            # Short description
            desc_el = item.select_one("div.position-short-wrapper")
            description = desc_el.get_text(strip=True) if desc_el else None

            # Contact email
            email_el = item.select_one("a.contact-email")
            contact_email = email_el.get_text(strip=True) if email_el else None

            title = f"Postdoc with {pi_name}" if pi_name else "Postdoctoral Position"
            if department:
                title += f" — {department}"

            job: dict[str, Any] = {
                "title": title,
                "url": pi_link or url,
                "institute": "Weizmann Institute of Science",
                "country": "Israel",
                "pi_name": pi_name,
                "source": self.name,
            }
            if department:
                job["department"] = department
            if description:
                job["description"] = description
            if contact_email:
                job["contact_email"] = contact_email

            jobs.append(job)

        return jobs

    # ── Broad Institute ────────────────────────────────────────────────

    def _scrape_broad(self) -> list[dict[str, Any]]:
        """Scrape Broad Institute careers page for research/postdoc positions.

        Uses Avature ATS at broadinstitute.avature.net. Fetches listing pages
        with pagination, then enriches each detail page with Broad-specific
        selectors.
        """
        base_url = "https://broadinstitute.avature.net/careers/SearchJobs"
        jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        # Avature caps at 6 results per page regardless of jobRecordsPerPage
        page_size = 6
        for offset in range(0, 300, page_size):
            try:
                resp = self.fetch(
                    base_url,
                    params={"jobRecordsPerPage": str(page_size),
                            "jobOffset": str(offset)},
                    timeout=30,
                )
            except Exception:
                self.logger.debug("Broad: failed at offset %d", offset)
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            # Filter to actual job title links (not share/social links)
            links = [
                a for a in soup.select('a[href*="JobDetail"]')
                if a.get("href", "").startswith("http")
                and "broadinstitute.avature.net" in a.get("href", "")
                and len(a.get_text(strip=True)) > 10
            ]
            if not links:
                break

            page_new = 0
            for link in links:
                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not title or not href:
                    continue
                detail_url = urljoin(base_url, href)
                if detail_url in seen_urls:
                    continue

                # Filter: only research/postdoc positions
                title_lower = title.lower()
                if not any(kw in title_lower for kw in (
                    "research", "postdoc", "scientist", "fellow",
                    "computational", "bioinformatics", "data science",
                    "associate",
                )):
                    continue

                seen_urls.add(detail_url)
                page_new += 1
                jobs.append({
                    "title": title,
                    "url": detail_url,
                    "institute": "Broad Institute of MIT and Harvard",
                    "country": "United States",
                    "source": self.name,
                })

            # If no new links found on this page, we're done
            if page_new == 0 and len(links) < page_size:
                break

        self.logger.info("Broad: found %d job links", len(jobs))

        # Enrich with detail pages using Broad-specific enrichment
        if jobs:
            jobs = self._parallel_enrich(
                jobs, self._enrich_detail_broad, max_workers=3, limit=30,
            )
        return jobs

    def _enrich_detail_broad(self, job: dict[str, Any]) -> dict[str, Any]:
        """Enrich a Broad Institute job from its Avature detail page.

        Page structure (article elements):
        1. article.article--details.regular-fields--cols-2Z → sidebar metadata
           (Location, Ref#, Job Family, Date published, Pay Range)
        2. article.article--details → "Description & Requirements" (main content)
        3. article.article--actions → Apply/Share buttons
        """
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url, timeout=25)
            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Extract main description ──
            # Best selector: div.job__detail__fix has the richest description text
            desc_el = soup.select_one("div.job__detail__fix")
            if not desc_el:
                # Fallback: the second article.article--details (without regular-fields)
                articles = soup.select("article.article--details")
                for art in articles:
                    if "regular-fields" not in " ".join(art.get("class", [])):
                        desc_el = art
                        break
            if not desc_el:
                # Final fallback: main
                desc_el = soup.select_one("main")

            if desc_el:
                desc = desc_el.get_text(separator="\n", strip=True)
                if len(desc) > len(job.get("description") or ""):
                    job["description"] = desc[:15000]

            # ── Extract structured metadata from sidebar ──
            metadata_art = soup.select_one(
                "article.regular-fields--cols-2Z"
            )
            if metadata_art:
                for field_div in metadata_art.select(
                    "div.article__content__view__field"
                ):
                    label_el = field_div.select_one(
                        "div.article__content__view__field__label"
                    )
                    value_el = field_div.select_one(
                        "div.article__content__view__field__value"
                    )
                    if not label_el or not value_el:
                        continue
                    label = label_el.get_text(strip=True).lower()
                    value = value_el.get_text(strip=True)

                    if "date published" in label and value:
                        # Broad uses US date format MM/DD/YYYY
                        parsed = None
                        for fmt in ("%m/%d/%Y", "%B %d, %Y", "%b %d, %Y",
                                    "%d-%b-%Y"):
                            try:
                                parsed = datetime.strptime(
                                    value.strip(), fmt
                                ).strftime("%Y-%m-%d")
                                break
                            except ValueError:
                                continue
                        if not parsed:
                            parsed = _parse_date(value)
                        if parsed and not job.get("posted_date"):
                            job["posted_date"] = parsed

                    elif "job family" in label and value:
                        if not job.get("department"):
                            job["department"] = value

                    elif "pay range" in label and value:
                        if not job.get("conditions"):
                            job["conditions"] = value

        except Exception:
            self.logger.debug("Failed to enrich Broad detail: %s", url)
        return job

    # ── MPG detail enrichment ─────────────────────────────────────────

    def _enrich_mpg_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Fetch MPG detail page and extract full description + metadata."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")

            # Extract full description from content area
            desc_el = (
                soup.select_one("div.job-detail__content")
                or soup.select_one("div.content-block__text")
                or soup.select_one("article.job-detail")
                or soup.select_one("div.entry-content")
                or soup.select_one("main")
            )
            if desc_el:
                desc = desc_el.get_text(separator="\n", strip=True)
                if len(desc) > len(job.get("description") or ""):
                    job["description"] = desc[:15000]
            elif not job.get("description"):
                fallback = self._extract_description_fallback(resp.text)
                if fallback:
                    job["description"] = fallback

            # Extract deadline from sidebar/metadata
            for sel in ("div.job-detail__sidebar", "aside", "div.meta-data"):
                sidebar = soup.select_one(sel)
                if not sidebar:
                    continue
                sidebar_text = sidebar.get_text(separator="\n", strip=True)
                dl_match = re.search(
                    r"(?:Bewerbungsfrist|Deadline|Application\s+deadline|Closing\s+date)"
                    r"\s*[:=]?\s*(.+)",
                    sidebar_text, re.IGNORECASE,
                )
                if dl_match and not job.get("deadline"):
                    parsed = _parse_date(dl_match.group(1).strip()[:30])
                    if parsed:
                        job["deadline"] = parsed
                break

            # Extract PI name from contact section
            for sel in ("div.contact", "div.job-detail__contact", "div.ansprechpartner"):
                contact = soup.select_one(sel)
                if contact and not job.get("pi_name"):
                    contact_text = contact.get_text(separator=" ", strip=True)
                    from src.matching.job_parser import extract_pi_name
                    pi = extract_pi_name(contact_text)
                    if pi:
                        job["pi_name"] = pi
                    break

            # Extract department from structured elements
            if not job.get("department"):
                for sel in ("div.department", "span.department", "div.organisational-unit"):
                    dept_el = soup.select_one(sel)
                    if dept_el:
                        dept = dept_el.get_text(strip=True)
                        if dept and len(dept) > 3:
                            job["department"] = dept
                        break

            # Extract application materials from full description
            enriched_desc = job.get("description") or ""
            if enriched_desc and not job.get("application_materials"):
                from src.matching.job_parser import extract_application_materials
                app_mat = extract_application_materials(enriched_desc)
                if app_mat:
                    job["application_materials"] = app_mat

        except Exception:
            self.logger.debug("Failed to enrich MPG detail: %s", url)
        return job

    # ── Generic detail enrichment ─────────────────────────────────────

    def _enrich_detail_generic(self, job: dict[str, Any]) -> dict[str, Any]:
        """Generic detail page enrichment for institutional scrapers."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url, timeout=20)
            soup = BeautifulSoup(resp.text, "html.parser")

            desc_el = (
                soup.select_one("div.entry-content")
                or soup.select_one("article")
                or soup.select_one("main")
                or soup.select_one("div.content")
            )
            if desc_el:
                desc = desc_el.get_text(separator="\n", strip=True)
                if len(desc) > len(job.get("description") or ""):
                    job["description"] = desc[:15000]
            elif not job.get("description"):
                fallback = self._extract_description_fallback(resp.text)
                if fallback:
                    job["description"] = fallback

        except Exception:
            self.logger.debug("Failed to enrich detail: %s", url)
        return job

    # ── Wyss Institute ────────────────────────────────────────────────

    def _scrape_wyss(self) -> list[dict[str, Any]]:
        """Scrape Wyss Institute for Biologically Inspired Engineering careers.

        The main /careers/ page is a landing page with links to sub-pages.
        Actual job listings are on /careers/academic/ and /careers/staff/.
        """
        jobs: list[dict[str, Any]] = []
        seen_urls: set[str] = set()

        for page_url in (
            "https://wyss.harvard.edu/careers/academic/",
            "https://wyss.harvard.edu/careers/staff/",
        ):
            try:
                resp = self.fetch(page_url, timeout=30)
            except Exception:
                self.logger.debug("Wyss: failed to fetch %s", page_url)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Job links are in <li> > <a> elements pointing to:
            # - academicpositions.harvard.edu/postings/XXXXX
            # - wyss.harvard.edu/job/...
            for link in soup.select("a[href]"):
                href = link.get("href", "").strip()
                text = link.get_text(strip=True)
                if not href or not text or len(text) < 10:
                    continue

                # Only accept actual job posting URLs
                is_harvard_posting = "academicpositions.harvard.edu/postings/" in href
                is_wyss_job = "/job/" in href and "wyss.harvard.edu" in href
                if not (is_harvard_posting or is_wyss_job):
                    continue

                detail_url = urljoin(page_url, href)
                if detail_url in seen_urls:
                    continue
                seen_urls.add(detail_url)

                jobs.append({
                    "title": text,
                    "url": detail_url,
                    "institute": "Wyss Institute at Harvard University",
                    "country": "United States",
                    "source": self.name,
                })

        self.logger.info("Wyss: found %d job links", len(jobs))

        # Enrich with detail pages
        if jobs:
            jobs = self._parallel_enrich(
                jobs, self._enrich_detail_wyss, max_workers=3, limit=30,
            )
        return jobs

    def _enrich_detail_wyss(self, job: dict[str, Any]) -> dict[str, Any]:
        """Enrich a Wyss job posting from Harvard Academic Positions or Wyss internal pages."""
        url = job.get("url")
        if not url:
            return job
        try:
            resp = self.fetch(url, timeout=25)
            soup = BeautifulSoup(resp.text, "html.parser")

            if "academicpositions.harvard.edu" in url:
                # Harvard Academic Positions portal
                # Description in main content area
                content = (
                    soup.select_one("#content_inner")
                    or soup.select_one("div.posting-details")
                    or soup.select_one("main")
                )
                if content:
                    desc = content.get_text(separator="\n", strip=True)
                    if len(desc) > len(job.get("description") or ""):
                        job["description"] = desc[:15000]

                # Extract from structured fields (dl/dt/dd pairs)
                for dt in soup.select("dt"):
                    label = dt.get_text(strip=True).lower()
                    dd = dt.find_next_sibling("dd")
                    if not dd:
                        continue
                    val = dd.get_text(strip=True)
                    if "department" in label and not job.get("department"):
                        job["department"] = val
                    elif "contact" in label and not job.get("pi_name"):
                        from src.matching.job_parser import extract_pi_name
                        pi = extract_pi_name(val)
                        if pi:
                            job["pi_name"] = pi

            else:
                # Wyss internal job pages (wyss.harvard.edu/job/...)
                # WordPress post content
                content = (
                    soup.select_one("div.entry-content")
                    or soup.select_one("article")
                    or soup.select_one("main")
                )
                if content:
                    desc = content.get_text(separator="\n", strip=True)
                    if len(desc) > len(job.get("description") or ""):
                        job["description"] = desc[:15000]

        except Exception:
            self.logger.debug("Failed to enrich Wyss detail: %s", url)
        return job
