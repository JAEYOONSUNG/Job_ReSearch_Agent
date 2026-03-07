"""JobSpy wrapper – searches Google Jobs, LinkedIn, Indeed, Glassdoor, ZipRecruiter."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from src.matching.job_parser import (
    extract_application_materials,
    extract_contact_email,
    extract_deadline,
    extract_department,
    extract_pi_name,
    infer_field,
)
from src.config import COUNTRY_TO_REGION, SEARCH_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Sites the jobspy library supports
JOBSPY_SITES = ["indeed", "linkedin", "google", "glassdoor", "zip_recruiter"]

# We limit results per query to avoid huge payloads
RESULTS_PER_QUERY = 25

# LinkedIn-specific CSS selectors for job descriptions (ordered by specificity)
_LINKEDIN_DESC_SELECTORS = [
    "div.show-more-less-html__markup",
    "div.description__text",
    "section.description",
    "div[class*='description']",
]

# Regex to extract LinkedIn job ID from URL
_LINKEDIN_JOB_ID_RE = re.compile(r"linkedin\.com/jobs/view/(\d+)")

# LinkedIn guest API endpoint (works without authentication)
_LINKEDIN_GUEST_API = "https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"


def _safe_str(val: Any) -> str | None:
    """Convert pandas/numpy value to plain str or None."""
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except (ImportError, TypeError, ValueError):
        pass
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "nat", "<na>", "n/a", ""):
        return None
    return s


class JobSpyScraper(BaseScraper):
    """Use the ``jobspy`` Python package to aggregate job results from
    multiple job boards in one go.

    ``pip install python-jobspy``
    """

    rate_limit: float = 3.0  # jobspy does its own internal pacing
    _LINKEDIN_ENRICH_LIMIT = 75
    _INDEED_ENRICH_LIMIT = 12

    @property
    def name(self) -> str:
        return "jobspy"

    @staticmethod
    def _is_linkedin_url(url: str) -> bool:
        """Check if a URL is a LinkedIn job posting."""
        return "linkedin.com/jobs" in url

    @staticmethod
    def _extract_linkedin_job_id(url: str) -> str | None:
        """Extract the numeric job ID from a LinkedIn job URL."""
        m = _LINKEDIN_JOB_ID_RE.search(url)
        return m.group(1) if m else None

    @staticmethod
    def _extract_linkedin_description(html: str) -> str | None:
        """Extract job description from LinkedIn HTML using known selectors.

        Tries LinkedIn-specific selectors first, then falls back to JSON-LD,
        which LinkedIn sometimes embeds in the page.
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")

        # Try LinkedIn-specific selectors
        for sel in _LINKEDIN_DESC_SELECTORS:
            el = soup.select_one(sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if len(text) >= 80:
                    return text[:15000]

        # Try JSON-LD structured data
        for script in soup.select("script[type='application/ld+json']"):
            try:
                ld = json.loads(script.string or "")
                if isinstance(ld, dict) and ld.get("description"):
                    desc = str(ld["description"]).strip()
                    if len(desc) >= 80:
                        return desc[:15000]
            except (json.JSONDecodeError, TypeError):
                pass

        return None

    def _fetch_linkedin_description(self, url: str) -> str | None:
        """Fetch a LinkedIn job description using the guest API or direct URL.

        Strategy:
        1. Try the lightweight guest API endpoint (smaller payload, more reliable)
        2. Fall back to the full job page URL
        Both approaches work without LinkedIn authentication.
        """
        job_id = self._extract_linkedin_job_id(url)

        # Strategy 1: Guest API (lighter payload, ~80KB vs ~330KB)
        if job_id:
            try:
                api_url = _LINKEDIN_GUEST_API.format(job_id=job_id)
                resp = self.fetch(api_url, timeout=15)
                desc = self._extract_linkedin_description(resp.text)
                if desc:
                    return desc
            except Exception:
                self.logger.debug("LinkedIn guest API failed for job %s", job_id)

        # Strategy 2: Direct page URL
        try:
            resp = self.fetch(url, timeout=15)
            desc = self._extract_linkedin_description(resp.text)
            if desc:
                return desc
        except Exception:
            self.logger.debug("LinkedIn direct fetch failed for %s", url)

        return None

    def _fetch_linkedin_html(self, url: str) -> str | None:
        """Fetch LinkedIn guest/detail HTML for metadata extraction."""
        job_id = self._extract_linkedin_job_id(url)
        if job_id:
            try:
                api_url = _LINKEDIN_GUEST_API.format(job_id=job_id)
                resp = self.fetch(api_url, timeout=15)
                if resp.text and len(resp.text) > 500:
                    return resp.text
            except Exception:
                self.logger.debug("LinkedIn guest HTML fetch failed for job %s", job_id)

        try:
            resp = self.fetch(url, timeout=15)
            if resp.text and len(resp.text) > 500:
                return resp.text
        except Exception:
            self.logger.debug("LinkedIn direct HTML fetch failed for %s", url)
        return None

    def _fetch_description_browser(self, url: str) -> str | None:
        """Use Playwright to fetch job description when HTTP fails.

        This is the last-resort fallback for pages that require JavaScript
        rendering or block plain HTTP requests.
        """
        try:
            from src.scrapers.browser import fetch_page

            html = fetch_page(
                url,
                wait_selector="div.description__text, div.show-more-less-html__markup, div[class*='description']",
                wait_ms=2500,
                timeout=15000,
            )
            if not html:
                return None

            # For LinkedIn, use the targeted extractor
            if self._is_linkedin_url(url):
                desc = self._extract_linkedin_description(html)
                if desc:
                    return desc

            # Generic fallback
            return self._extract_description_fallback(html)
        except Exception:
            self.logger.debug("Browser fetch failed for %s", url)
            return None

    def _fetch_description(self, url: str) -> str | None:
        """Fetch a job page and extract description when jobspy returned None.

        For LinkedIn URLs, uses a multi-strategy approach:
        1. LinkedIn guest API (lightweight, no auth needed)
        2. Direct HTTP to LinkedIn page
        3. Playwright browser fallback

        For other sites, uses standard HTTP with generic extraction,
        falling back to Playwright if needed.
        """
        if self._is_linkedin_url(url):
            # LinkedIn-specific path with targeted selectors
            desc = self._fetch_linkedin_description(url)
            if desc:
                return desc
            # Playwright fallback for LinkedIn
            desc = self._fetch_description_browser(url)
            if desc:
                return desc
            return None

        # Non-LinkedIn: standard HTTP fetch
        try:
            resp = self.fetch(url, timeout=15)
            desc = self._extract_description_fallback(resp.text)
            if desc:
                return desc
        except Exception:
            self.logger.debug("HTTP fetch failed for %s", url)

        # Playwright fallback for non-LinkedIn
        return self._fetch_description_browser(url)

    def _fetch_html_browser(
        self,
        url: str,
        wait_selector: str,
        wait_ms: int = 2500,
        timeout: int = 15000,
    ) -> str | None:
        """Fetch page HTML via Playwright when HTTP metadata extraction is blocked."""
        try:
            from src.scrapers.browser import fetch_page
            return fetch_page(url, wait_selector=wait_selector, wait_ms=wait_ms, timeout=timeout)
        except Exception:
            self.logger.debug("Browser HTML fetch failed for %s", url)
            return None

    @staticmethod
    def _merge_condition_text(existing: str | None, *parts: str | None) -> str:
        """Merge condition snippets without duplicates."""
        merged: list[str] = []
        seen: set[str] = set()
        for item in (existing, *parts):
            cleaned = (item or "").strip().strip("|")
            if not cleaned:
                continue
            key = cleaned.lower()
            if key in seen:
                continue
            seen.add(key)
            merged.append(cleaned)
        return " | ".join(merged)

    @staticmethod
    def _relative_posted_to_iso(raw: str | None, now: datetime | None = None) -> str | None:
        """Convert relative posted text like '1 week ago' to YYYY-MM-DD."""
        if not raw:
            return None
        text = raw.strip().lower()
        now = now or datetime.now()
        if "today" in text or "just now" in text:
            return now.strftime("%Y-%m-%d")
        match = re.search(r"(\d+)\s+(hour|day|week|month|year)s?\s+ago", text)
        if not match:
            return None
        value = int(match.group(1))
        unit = match.group(2)
        delta = {
            "hour": timedelta(hours=value),
            "day": timedelta(days=value),
            "week": timedelta(weeks=value),
            "month": timedelta(days=30 * value),
            "year": timedelta(days=365 * value),
        }[unit]
        return (now - delta).strftime("%Y-%m-%d")

    @staticmethod
    def _extract_labeled_lines(text: str, labels: set[str]) -> dict[str, str]:
        """Extract simple label/value pairs from flattened detail text."""
        lines = [re.sub(r"\s+", " ", line).strip(" :|") for line in text.splitlines() if line.strip()]
        fields: dict[str, str] = {}
        i = 0
        while i < len(lines):
            line = lines[i]
            label = ""
            value = ""
            if ":" in line:
                maybe_label, maybe_value = [part.strip() for part in line.split(":", 1)]
                if maybe_label in labels:
                    label = maybe_label
                    value = maybe_value
                    if not value and i + 1 < len(lines):
                        value = lines[i + 1]
                        i += 1
            elif line in labels and i + 1 < len(lines):
                label = line
                value = lines[i + 1]
                i += 1
            if label:
                fields[label] = value
            i += 1
        return fields

    def _enrich_linkedin_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Enrich LinkedIn results using the guest posting HTML."""
        from bs4 import BeautifulSoup

        html = self._fetch_linkedin_html(job["url"])
        if not html:
            return job

        soup = BeautifulSoup(html, "html.parser")

        desc = self._extract_linkedin_description(html)
        if not desc:
            desc_el = soup.select_one("div.show-more-less-html__markup, div.description__text")
            if desc_el:
                text = desc_el.get_text(separator="\n", strip=True)
                if len(text) >= 40:
                    desc = text[:15000]
        if desc and (not job.get("description") or len(desc) > len(job.get("description") or "")):
            job["description"] = desc

        org = soup.select_one("a.topcard__org-name-link, a.topcard__flavor--black-link")
        if org and not job.get("institute"):
            job["institute"] = org.get_text(" ", strip=True)

        posted = soup.select_one("span.posted-time-ago__text")
        if posted and not job.get("posted_date"):
            parsed = self._relative_posted_to_iso(posted.get_text(" ", strip=True))
            if parsed:
                job["posted_date"] = parsed

        loc = soup.select_one("span.topcard__flavor.topcard__flavor--bullet")
        if loc and not job.get("country"):
            country = self._guess_country(loc.get_text(" ", strip=True))
            if country:
                job["country"] = country

        cond_parts: list[str] = []
        for item in soup.select("li.description__job-criteria-item"):
            label_el = item.select_one("h3")
            value_el = item.select_one("span")
            if not label_el or not value_el:
                continue
            label = label_el.get_text(" ", strip=True)
            value = value_el.get_text(" ", strip=True)
            if not value:
                continue
            if label.lower() == "employment type":
                cond_parts.append(f"Employment type: {value}")
            elif label.lower() in {"seniority level", "job function", "industries"}:
                cond_parts.append(f"{label}: {value}")
        if cond_parts:
            job["conditions"] = self._merge_condition_text(job.get("conditions"), *cond_parts)

        desc = job.get("description") or ""
        if desc:
            if not job.get("department"):
                dept = extract_department(desc)
                if dept:
                    job["department"] = dept
            if not job.get("pi_name"):
                pi = extract_pi_name(desc)
                if pi:
                    job["pi_name"] = pi
            if not job.get("field"):
                field = infer_field(desc)
                if field:
                    job["field"] = field
            if not job.get("deadline"):
                deadline = extract_deadline(desc)
                if deadline:
                    job["deadline"] = deadline
            if not job.get("application_materials"):
                app_materials = extract_application_materials(desc)
                if app_materials:
                    job["application_materials"] = app_materials
            if not job.get("contact_email"):
                contact = extract_contact_email(desc)
                if contact:
                    job["contact_email"] = contact

        return job

    def _enrich_indeed_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Enrich Indeed rows using browser-rendered HTML when available."""
        from bs4 import BeautifulSoup

        html = self._fetch_html_browser(
            job["url"],
            wait_selector="#jobDescriptionText, div[data-testid='jobsearch-JobComponent-description'], div[data-testid='jobsearch-OtherJobDetailsContainer']",
            wait_ms=2000,
            timeout=15000,
        )
        if not html:
            return job

        soup = BeautifulSoup(html, "html.parser")

        for sel in (
            "#jobDescriptionText",
            "div[data-testid='jobsearch-JobComponent-description']",
            "div.jobsearch-JobComponent-description",
        ):
            desc_el = soup.select_one(sel)
            if desc_el:
                desc = desc_el.get_text(separator="\n", strip=True)
                if len(desc) > 100 and len(desc) > len(job.get("description") or ""):
                    job["description"] = desc[:15000]
                    break

        details_parts: list[str] = []
        for sel in (
            "#salaryInfoAndJobType",
            "div[data-testid='salaryInfoAndJobType']",
            "div[data-testid='jobsearch-OtherJobDetailsContainer']",
        ):
            detail_el = soup.select_one(sel)
            if not detail_el:
                continue
            detail_text = detail_el.get_text(separator="\n", strip=True)
            fields = self._extract_labeled_lines(
                detail_text,
                {"Pay", "Job type", "Shift and schedule", "Location", "Benefits"},
            )
            if fields.get("Pay"):
                details_parts.append(f"Salary: {fields['Pay']}")
            if fields.get("Job type"):
                details_parts.append(f"Type: {fields['Job type']}")
            if fields.get("Shift and schedule"):
                details_parts.append(f"Schedule: {fields['Shift and schedule']}")
            if fields.get("Location") and not job.get("country"):
                country = self._guess_country(fields["Location"])
                if country:
                    job["country"] = country
            if fields.get("Benefits"):
                details_parts.append(f"Benefits: {fields['Benefits']}")
            if details_parts:
                break
        if details_parts:
            job["conditions"] = self._merge_condition_text(job.get("conditions"), *details_parts)

        desc = job.get("description") or ""
        if desc:
            if not job.get("department"):
                dept = extract_department(desc)
                if dept:
                    job["department"] = dept
            if not job.get("pi_name"):
                pi = extract_pi_name(desc)
                if pi:
                    job["pi_name"] = pi
            if not job.get("field"):
                field = infer_field(desc)
                if field:
                    job["field"] = field
            if not job.get("deadline"):
                deadline = extract_deadline(desc)
                if deadline:
                    job["deadline"] = deadline
            if not job.get("application_materials"):
                app_materials = extract_application_materials(desc)
                if app_materials:
                    job["application_materials"] = app_materials
            if not job.get("contact_email"):
                contact = extract_contact_email(desc)
                if contact:
                    job["contact_email"] = contact

        return job

    def _enrich_detail(self, job: dict[str, Any]) -> dict[str, Any]:
        """Dispatch source-specific metadata enrichment."""
        source = (job.get("source") or "").lower()
        url = job.get("url") or ""
        if "linkedin" in source or self._is_linkedin_url(url):
            return self._enrich_linkedin_detail(job)
        if "indeed" in source or "indeed.com" in url:
            return self._enrich_indeed_detail(job)
        return job

    @staticmethod
    def _needs_linkedin_enrichment(job: dict[str, Any]) -> bool:
        """Return True if a LinkedIn row is still missing key metadata."""
        desc = job.get("description") or ""
        conditions = job.get("conditions") or ""
        return any(
            not job.get(key)
            for key in ("posted_date", "application_materials", "contact_email")
        ) or len(desc) < 500 or "Employment type:" not in conditions

    @staticmethod
    def _needs_indeed_enrichment(job: dict[str, Any]) -> bool:
        """Return True if an Indeed row is worth a browser enrichment pass."""
        desc = job.get("description") or ""
        missing_contact_bundle = not job.get("application_materials") and not job.get("contact_email")
        missing_description = len(desc) < 400
        missing_conditions = not job.get("conditions")
        return missing_description or missing_contact_bundle or (missing_conditions and len(desc) < 800)

    def scrape(self) -> list[dict[str, Any]]:
        try:
            from jobspy import scrape_jobs
        except ImportError:
            self.logger.error(
                "python-jobspy is not installed. "
                "Install it with: pip install python-jobspy"
            )
            return []

        all_jobs: list[dict[str, Any]] = []

        for keyword in SEARCH_KEYWORDS:
            self.logger.info("JobSpy search: %s", keyword)
            try:
                df = scrape_jobs(
                    site_name=JOBSPY_SITES,
                    search_term=keyword,
                    results_wanted=RESULTS_PER_QUERY,
                    country_indeed="USA",
                    # Limit to recent postings
                    hours_old=168,  # 7 days
                )

                if df is None or df.empty:
                    self.logger.info("No results for '%s'", keyword)
                    continue

                self.logger.info(
                    "JobSpy returned %d rows for '%s'", len(df), keyword
                )

                for _, row in df.iterrows():
                    job = self._map_row(row)
                    if job:
                        all_jobs.append(job)

            except Exception:
                self.logger.exception(
                    "JobSpy query failed for keyword: %s", keyword
                )

        # Back-fill descriptions for all jobs that have none
        empty_desc = [j for j in all_jobs if not j.get("description")]
        if empty_desc:
            self.logger.info(
                "Back-filling descriptions for %d/%d jobs",
                len(empty_desc),
                len(all_jobs),
            )
            filled = 0
            for i, job in enumerate(empty_desc, 1):
                desc = self._fetch_description(job["url"])
                if desc:
                    job["description"] = desc
                    filled += 1
                if i % 10 == 0:
                    self.logger.info(
                        "Back-fill progress: %d/%d checked, %d filled",
                        i,
                        len(empty_desc),
                        filled,
                    )
            self.logger.info(
                "Back-fill complete: %d/%d descriptions filled",
                filled,
                len(empty_desc),
            )

        # Add source-specific metadata only where it is likely to pay off.
        linkedin_targets = [
            job for job in all_jobs
            if "linkedin" in (job.get("source") or "").lower()
            and self._needs_linkedin_enrichment(job)
        ]
        if linkedin_targets:
            linkedin_targets = sorted(
                linkedin_targets,
                key=lambda job: (
                    bool(job.get("application_materials")),
                    bool(job.get("contact_email")),
                    len(job.get("description") or ""),
                ),
            )
            enriched = self._parallel_enrich(
                linkedin_targets,
                self._enrich_linkedin_detail,
                max_workers=6,
                limit=self._LINKEDIN_ENRICH_LIMIT,
            )
            by_url = {job.get("url"): job for job in enriched if job.get("url")}
            all_jobs = [by_url.get(job.get("url"), job) for job in all_jobs]

        indeed_targets = [
            job for job in all_jobs
            if "indeed" in (job.get("source") or "").lower()
            and self._needs_indeed_enrichment(job)
        ]
        if indeed_targets:
            indeed_targets = sorted(
                indeed_targets,
                key=lambda job: (
                    bool(job.get("application_materials")),
                    bool(job.get("contact_email")),
                    bool(job.get("conditions")),
                    len(job.get("description") or ""),
                ),
            )
            enriched = self._parallel_enrich(
                indeed_targets,
                self._enrich_indeed_detail,
                max_workers=1,
                limit=self._INDEED_ENRICH_LIMIT,
            )
            by_url = {job.get("url"): job for job in enriched if job.get("url")}
            all_jobs = [by_url.get(job.get("url"), job) for job in all_jobs]

        self.logger.info("Total JobSpy jobs collected: %d", len(all_jobs))
        return all_jobs

    # ── Row mapping ───────────────────────────────────────────────────────

    def _map_row(self, row: Any) -> dict[str, Any] | None:
        """Map a jobspy DataFrame row to our job schema."""
        title = _safe_str(getattr(row, "title", None))
        url = _safe_str(getattr(row, "job_url", None))
        if not title or not url:
            return None

        company = _safe_str(getattr(row, "company_name", None)) or _safe_str(
            getattr(row, "company", None)
        )
        location = _safe_str(getattr(row, "location", None))
        description = _safe_str(getattr(row, "description", None))
        date_posted = _safe_str(getattr(row, "date_posted", None))
        site = _safe_str(getattr(row, "site", None))

        # Attempt to extract country from location
        country = self._guess_country(location)

        # Extract additional fields from jobspy DataFrame
        salary_min = _safe_str(getattr(row, "min_amount", None))
        salary_max = _safe_str(getattr(row, "max_amount", None))
        # Strip trailing ".0" from salary values (e.g. "35000.0" -> "35000")
        if salary_min and salary_min.endswith(".0"):
            salary_min = salary_min[:-2]
        if salary_max and salary_max.endswith(".0"):
            salary_max = salary_max[:-2]
        interval = _safe_str(getattr(row, "interval", None))
        job_type = _safe_str(getattr(row, "job_type", None))

        conditions_parts = []
        if salary_min and salary_max:
            try:
                smin = f"{int(float(salary_min)):,}"
                smax = f"{int(float(salary_max)):,}"
            except (ValueError, OverflowError):
                smin, smax = salary_min, salary_max
            conditions_parts.append(f"Salary: ${smin}-${smax}")
            if interval:
                conditions_parts[-1] += f"/{interval}"
        elif salary_min:
            try:
                smin = f"{int(float(salary_min)):,}"
            except (ValueError, OverflowError):
                smin = salary_min
            conditions_parts.append(f"Salary: ${smin}+")
        if job_type:
            conditions_parts.append(f"Type: {job_type}")

        return {
            "title": title,
            "institute": company,
            "country": country,
            "description": (description or "")[:15000],
            "url": url,
            "posted_date": date_posted,
            "source": f"jobspy_{site}" if site else "jobspy",
            "field": None,
            "conditions": " | ".join(conditions_parts) if conditions_parts else None,
        }

    @staticmethod
    def _guess_country(location: str | None) -> str | None:
        """Very rough heuristic to extract a country from a location string."""
        if not location:
            return None
        loc_lower = location.lower()

        # US state abbreviations
        us_states = {
            "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
            "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
            "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
            "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
            "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
        }
        parts = [p.strip() for p in location.split(",")]
        if len(parts) >= 2:
            last = parts[-1].strip().lower()
            # If last part is a 2-letter US state abbreviation
            if last in us_states or last == "usa" or last == "united states":
                return "United States"

        # Check for explicit country names
        for country_name in COUNTRY_TO_REGION:
            if country_name.lower() in loc_lower:
                return country_name

        return None
