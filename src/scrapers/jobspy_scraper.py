"""JobSpy wrapper – searches Google Jobs, LinkedIn, Indeed, Glassdoor, ZipRecruiter."""

from __future__ import annotations

import logging
from typing import Any

from src.config import COUNTRY_TO_REGION, SEARCH_KEYWORDS
from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)

# Sites the jobspy library supports
JOBSPY_SITES = ["indeed", "linkedin", "google", "zip_recruiter"]

# We limit results per query to avoid huge payloads
RESULTS_PER_QUERY = 25


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

    @property
    def name(self) -> str:
        return "jobspy"

    def _fetch_description(self, url: str) -> str | None:
        """Fetch a job page and extract description when jobspy returned None."""
        try:
            resp = self.fetch(url, timeout=15)
            return self._extract_description_fallback(resp.text)
        except Exception:
            self.logger.debug("Could not fetch description from %s", url)
            return None

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

        # Back-fill descriptions for jobs that have none (up to 30)
        empty_desc = [j for j in all_jobs if not j.get("description")]
        for job in empty_desc[:30]:
            desc = self._fetch_description(job["url"])
            if desc:
                job["description"] = desc

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
            conditions_parts.append(f"Salary: ${salary_min}-${salary_max}")
            if interval:
                conditions_parts[-1] += f"/{interval}"
        elif salary_min:
            conditions_parts.append(f"Salary: ${salary_min}+")
        if job_type:
            conditions_parts.append(f"Type: {job_type}")

        return {
            "title": title,
            "institute": company,
            "country": country,
            "description": (description or "")[:3000],
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
