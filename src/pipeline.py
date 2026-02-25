"""Main pipeline orchestrator.

Uses asyncio for concurrent scraper execution when running in parallel mode.
Each scraper runs as an independent coroutine with per-scraper timeouts.
"""

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta

from src.config import LOG_DIR
from src.db import init_db, log_scrape

logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging to file and console."""
    level = logging.DEBUG if verbose else logging.INFO
    log_file = LOG_DIR / f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(str(log_file)),
            logging.StreamHandler(),
        ],
    )


def _build_scrapers() -> list:
    """Instantiate all available scrapers."""
    from src.scrapers.nature_careers import NatureCareersScraper
    from src.scrapers.jobspy_scraper import JobSpyScraper
    from src.scrapers.euraxess import EuraxessScraper
    from src.scrapers.academicpositions import AcademicPositionsScraper
    from src.scrapers.scholarshipdb import ScholarshipDBScraper
    from src.scrapers.researchgate import ResearchGateScraper
    from src.scrapers.lab_websites import LabWebsiteScraper
    from src.scrapers.institutional import InstitutionalPortalScraper

    scrapers = [
        NatureCareersScraper(),
        JobSpyScraper(),
        EuraxessScraper(),
        InstitutionalPortalScraper(),
        AcademicPositionsScraper(),
        ScholarshipDBScraper(),
        ResearchGateScraper(),
        LabWebsiteScraper(),
    ]

    # Conditionally add scrapers that may have import issues
    # Glassdoor disabled: consistently returns 0 results, blocks Playwright for ~3min
    for cls_path, label in [
        ("src.scrapers.jobs_ac_uk", "JobsAcUkScraper"),
        ("src.scrapers.jobs_ac_kr", "JobsAcKrScraper"),
        ("src.scrapers.wanted", "WantedScraper"),
    ]:
        try:
            import importlib
            mod = importlib.import_module(cls_path)
            cls = getattr(mod, label)
            scrapers.append(cls())
        except (ImportError, AttributeError):
            logger.warning("%s scraper unavailable", label)

    return scrapers


def _run_single_scraper(scraper) -> list[dict]:
    """Run a single scraper with error handling (sync, used for sequential mode)."""
    try:
        logger.info("Running scraper: %s", scraper.name)
        jobs = scraper.run()
        logger.info("%s: found %d jobs", scraper.name, len(jobs))
        return jobs
    except Exception as e:
        logger.error("%s failed: %s", scraper.name, e, exc_info=True)
        log_scrape(scraper.name, "error", error=str(e))
        return []


SCRAPER_TIMEOUT = 300  # 5 minutes per scraper
# Scrapers that need more time (many detail-page fetches with rate limiting)
_SLOW_SCRAPERS = {"scholarshipdb": 900, "jobs_ac_uk": 900, "euraxess": 900, "researchgate": 900, "institutional": 600}


async def _async_run_single_scraper(scraper) -> list[dict]:
    """Run a single scraper as an async coroutine with timeout."""
    timeout = _SLOW_SCRAPERS.get(scraper.name, SCRAPER_TIMEOUT)
    try:
        logger.info("Running async scraper: %s", scraper.name)
        jobs = await asyncio.wait_for(
            scraper.async_run(),
            timeout=timeout,
        )
        logger.info("%s: found %d jobs", scraper.name, len(jobs))
        return jobs
    except asyncio.TimeoutError:
        logger.warning("%s timed out after %ds, skipping", scraper.name, timeout)
        log_scrape(scraper.name, "error", error=f"Timed out after {timeout}s")
        return []
    except Exception as e:
        logger.error("%s failed: %s", scraper.name, e, exc_info=True)
        log_scrape(scraper.name, "error", error=str(e))
        return []


async def _async_run_all_scrapers(scrapers: list) -> list[dict]:
    """Run all scrapers concurrently using asyncio.gather."""
    tasks = [_async_run_single_scraper(s) for s in scrapers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_jobs: list[dict] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(
                "%s raised exception: %s",
                scrapers[i].name, result, exc_info=result,
            )
        elif isinstance(result, list):
            all_jobs.extend(result)

    # Close shared async browser + aiohttp session
    try:
        from src.scrapers.browser import async_close_browser
        await async_close_browser()
    except Exception:
        pass

    try:
        from src.scrapers.base import _AsyncSessionManager
        await _AsyncSessionManager.close()
    except Exception:
        pass

    # Also close sync browser if any scraper used it as fallback
    try:
        from src.scrapers.browser import close_browser
        close_browser()
    except Exception:
        pass

    return all_jobs


def run_scrapers(sequential: bool = False) -> list[dict]:
    """Run all scrapers and collect jobs.

    Parameters
    ----------
    sequential : bool
        If True, run scrapers one-by-one (useful for debugging).
        Default is parallel execution via asyncio.gather().
    """
    scrapers = _build_scrapers()
    all_jobs: list[dict] = []

    if sequential:
        for scraper in scrapers:
            all_jobs.extend(_run_single_scraper(scraper))
    else:
        all_jobs = asyncio.run(_async_run_all_scrapers(scrapers))

    # Close shared Playwright browser if it was used (sync fallback)
    try:
        from src.scrapers.browser import close_browser
        close_browser()
    except Exception:
        pass

    return all_jobs


def run_scoring(jobs: list[dict]) -> list[dict]:
    """Score and sort collected jobs."""
    from src.matching.cv_parser import load_cached_keywords
    from src.matching.scorer import score_and_sort_jobs
    from src.matching.dedup import deduplicate_jobs

    keywords = load_cached_keywords()
    jobs = deduplicate_jobs(jobs)

    # Persist alt_url from cross-source dedup merges into the DB
    _persist_alt_urls(jobs)

    jobs = score_and_sort_jobs(jobs, keywords)

    # Persist match_score to DB
    _persist_match_scores(jobs)

    return jobs


def _persist_match_scores(jobs: list[dict]) -> None:
    """Write match_score back to the DB for all scored jobs."""
    from src.db import get_connection

    updates = [
        (j["match_score"], j["url"])
        for j in jobs
        if j.get("url") and j.get("match_score", 0) > 0
    ]
    if not updates:
        return
    with get_connection() as conn:
        conn.executemany(
            "UPDATE jobs SET match_score = ? WHERE url = ?",
            updates,
        )
    logger.info("Persisted match_score for %d jobs", len(updates))


def _persist_alt_urls(jobs: list[dict]) -> None:
    """Write alt_url and dismiss duplicate rows in the database.

    Runs dedup on ALL DB jobs (not just the current batch) so that
    cross-source duplicates from previous runs are also detected.
    The "winner" (longest description) keeps its row and gets alt_url set.
    The "loser" (duplicate) rows are marked status='merged' so they
    don't appear in Excel export.
    """
    from src.db import get_connection, get_jobs
    from src.matching.dedup import deduplicate_jobs

    all_jobs = get_jobs(limit=10000)
    all_urls = {j.get("url") for j in all_jobs if j.get("url")}

    deduped = deduplicate_jobs(all_jobs)
    kept_urls = {j.get("url") for j in deduped if j.get("url")}

    # URLs that were in the original but not in the deduped list = losers
    loser_urls = all_urls - kept_urls

    # Collect alt_url updates from cross-source merges
    updates = []
    for job in deduped:
        alt_urls = job.get("alt_urls")
        if alt_urls and job.get("url"):
            updates.append((alt_urls[0]["url"], job.get("pi_name") or "", job["url"]))

    if not updates and not loser_urls:
        return
    with get_connection() as conn:
        if updates:
            conn.executemany(
                "UPDATE jobs SET alt_url = ? WHERE url = ? AND (alt_url IS NULL OR alt_url = '')",
                [(u[0], u[2]) for u in updates],
            )
            # Backfill pi_name from merged data
            conn.executemany(
                "UPDATE jobs SET pi_name = ? WHERE url = ? AND (pi_name IS NULL OR pi_name = '') AND ? != ''",
                [(u[1], u[2], u[1]) for u in updates],
            )
        # Dismiss all loser rows so they don't appear in exports
        if loser_urls:
            conn.executemany(
                "UPDATE jobs SET status = 'merged' WHERE url = ? AND status = 'new'",
                [(url,) for url in loser_urls],
            )
            logger.info("Dismissed %d duplicate rows (status='merged')", len(loser_urls))
    logger.info("Persisted alt_url for %d cross-source duplicates", len(updates))


def run_pi_enrichment(jobs: list[dict], max_workers: int = 2) -> list[dict]:
    """Batch PI URL lookup for jobs that have a pi_name but missing URLs.

    Runs after scraping/scoring so it doesn't block scrapers.
    Uses asyncio with a semaphore for controlled concurrency
    (Scholar rate-limits aggressively).
    """
    candidates = [
        j for j in jobs
        if j.get("pi_name") and not (j.get("scholar_url") and j.get("lab_url"))
    ]
    if not candidates:
        return jobs

    logger.info("PI enrichment: %d jobs need URL lookup", len(candidates))

    async def _async_pi_enrichment() -> None:
        sem = asyncio.Semaphore(max_workers)
        done_count = 0

        async def _lookup_one(job: dict) -> None:
            nonlocal done_count
            async with sem:
                loop = asyncio.get_running_loop()
                try:
                    def _do_lookup() -> dict:
                        from src.matching.pi_lookup import lookup_pi_urls
                        return lookup_pi_urls(
                            job["pi_name"], job.get("institute"), job.get("department")
                        )

                    urls = await loop.run_in_executor(None, _do_lookup)
                    for key in ("scholar_url", "lab_url", "dept_url", "h_index",
                                "citations", "recent_papers", "top_cited_papers"):
                        if urls.get(key) and not job.get(key):
                            job[key] = urls[key]
                except Exception:
                    logger.debug("PI lookup failed for %s", job.get("pi_name"))

                done_count += 1
                if done_count % 10 == 0:
                    logger.info("PI enrichment progress: %d/%d", done_count, len(candidates))

        await asyncio.gather(*[_lookup_one(j) for j in candidates])

    asyncio.run(_async_pi_enrichment())
    logger.info("PI enrichment complete")
    return jobs


_DEPT_CACHE_TABLE = """
CREATE TABLE IF NOT EXISTS dept_url_cache (
    institute TEXT NOT NULL,
    dept_hint TEXT NOT NULL DEFAULT '',
    dept_url TEXT,
    searched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (institute, dept_hint)
)
"""

MAX_DEPT_LOOKUPS_PER_RUN = 200  # effectively unlimited; circuit breaker handles abuse


def _init_dept_cache() -> None:
    from src.db import get_connection
    with get_connection() as conn:
        conn.executescript(_DEPT_CACHE_TABLE)


def _get_cached_dept(institute: str, dept_hint: str) -> tuple[bool, str | None]:
    """Check cache. Returns (found_in_cache, url_or_none)."""
    from src.db import get_connection
    with get_connection() as conn:
        row = conn.execute(
            "SELECT dept_url FROM dept_url_cache WHERE institute = ? AND dept_hint = ?",
            (institute.strip().lower(), dept_hint.strip().lower()),
        ).fetchone()
        if row:
            return True, row["dept_url"]
        return False, None


def _save_dept_cache(institute: str, dept_hint: str, url: str | None) -> None:
    from src.db import get_connection
    with get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO dept_url_cache (institute, dept_hint, dept_url) "
            "VALUES (?, ?, ?)",
            (institute.strip().lower(), dept_hint.strip().lower(), url),
        )


def run_dept_enrichment(jobs: list[dict]) -> list[dict]:
    """Batch department URL lookup for jobs that have an institute but no dept_url.

    Uses a persistent SQLite cache so each (institute, dept) pair is only
    searched once across all runs. Limits DDG requests per run to avoid 403s.
    """
    _init_dept_cache()

    candidates = [
        j for j in jobs
        if j.get("institute") and not j.get("dept_url")
    ]
    if not candidates:
        return jobs

    def _make_key(job: dict) -> tuple[str, str]:
        return (
            (job.get("institute") or "").strip().lower(),
            (job.get("department") or job.get("field") or "").strip().lower(),
        )

    def _lookup_dept(institute: str, dept_hint: str) -> str | None:
        """Search DDG directly for '{institute} {dept_hint} department' (no site: operator)."""
        import requests as _req
        import re as _re
        from src.discovery.lab_finder import _extract_ddg_url, _is_valid_lab_url, _institute_to_domain
        import time as _time

        domain = _institute_to_domain(institute)
        query = f"{institute} {dept_hint} department".strip() if dept_hint else f"{institute} research department"
        url = f"https://html.duckduckgo.com/html/?q={_req.utils.quote(query)}"
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        try:
            resp = _req.get(url, headers=headers, timeout=15)
            if resp.status_code == 403:
                logger.debug("DDG 403 for dept lookup: %s", institute)
                return None
            resp.raise_for_status()
            _time.sleep(2.5)

            urls = _re.findall(r'class="result__a"[^>]*href="([^"]+)"', resp.text)
            for candidate in urls[:5]:
                real = _extract_ddg_url(candidate)
                if not real or not _is_valid_lab_url(real):
                    continue
                # Prefer results on the institute's own domain
                if domain and domain in real:
                    return real
            # Fallback: return first valid result
            for candidate in urls[:3]:
                real = _extract_ddg_url(candidate)
                if real and _is_valid_lab_url(real):
                    return real
        except Exception:
            logger.debug("Dept DDG search failed for %s", institute)
        return None

    unique_keys = set(_make_key(j) for j in candidates)
    logger.info(
        "Dept URL enrichment: %d jobs, %d unique pairs", len(candidates), len(unique_keys),
    )

    # Phase 1: fill from cache
    key_to_url: dict[tuple, str | None] = {}
    uncached_keys: list[tuple[str, str]] = []
    for key in unique_keys:
        found, url = _get_cached_dept(key[0], key[1])
        if found:
            key_to_url[key] = url
        else:
            uncached_keys.append(key)

    logger.info(
        "Dept cache: %d hits, %d to search (max %d this run)",
        len(unique_keys) - len(uncached_keys),
        len(uncached_keys),
        MAX_DEPT_LOOKUPS_PER_RUN,
    )

    # Phase 2: DDG search for uncached (with circuit breaker)
    consecutive_failures = 0
    searched = 0
    # Sequential DDG lookups (DDG rate-limits parallel requests aggressively)
    consecutive_failures = 0
    for inst, dept in uncached_keys:
        if consecutive_failures >= 5:
            logger.warning("DDG circuit breaker after %d failures, deferring rest", consecutive_failures)
            break

        url = _lookup_dept(inst, dept)
        _save_dept_cache(inst, dept, url)
        key_to_url[(inst, dept)] = url
        searched += 1

        if url:
            consecutive_failures = 0
        else:
            consecutive_failures += 1

    # Phase 3: apply to jobs and persist to DB
    from src.db import get_connection
    filled = 0
    with get_connection() as conn:
        for job in candidates:
            key = _make_key(job)
            url = key_to_url.get(key)
            if url:
                job["dept_url"] = url
                filled += 1
                # Persist to DB
                job_url = job.get("url")
                if job_url:
                    conn.execute(
                        "UPDATE jobs SET dept_url = ? WHERE url = ? AND (dept_url IS NULL OR dept_url = '')",
                        (url, job_url),
                    )

    logger.info("Dept URL enrichment complete: %d/%d filled", filled, len(candidates))
    return jobs


def run_weekly_discovery() -> None:
    """Run the weekly PI discovery pipeline."""
    logger.info("Starting weekly PI discovery pipeline...")

    try:
        from src.discovery.seed_profiler import profile_seed_pis
        logger.info("Phase 1: Profiling seed PIs...")
        profile_seed_pis()
    except Exception as e:
        logger.error("Seed profiling failed: %s", e, exc_info=True)

    try:
        from src.discovery.coauthor_network import build_coauthor_network
        logger.info("Phase 2: Building coauthor network...")
        build_coauthor_network()
    except Exception as e:
        logger.error("Coauthor network failed: %s", e, exc_info=True)

    try:
        from src.discovery.citation_network import build_citation_network
        logger.info("Phase 3: Building citation network...")
        build_citation_network()
    except Exception as e:
        logger.error("Citation network failed: %s", e, exc_info=True)

    try:
        from src.discovery.topic_discovery import discover_by_topic
        logger.info("Phase 4: Topic-based discovery...")
        discover_by_topic()
    except Exception as e:
        logger.error("Topic discovery failed: %s", e, exc_info=True)

    try:
        from src.discovery.pi_recommender import score_all_pis
        logger.info("Phase 5: Scoring PI recommendations...")
        score_all_pis()
    except Exception as e:
        logger.error("PI scoring failed: %s", e, exc_info=True)

    try:
        from src.discovery.lab_finder import find_lab_urls
        logger.info("Phase 6: Finding lab URLs...")
        find_lab_urls()
    except Exception as e:
        logger.error("Lab finder failed: %s", e, exc_info=True)

    logger.info("Weekly PI discovery pipeline complete")


def run_report(send_email: bool = True) -> None:
    """Generate and optionally send the report."""
    from src.reporting.email_report import send_report
    from src.reporting.excel_export import export_to_excel

    since = (datetime.now() - timedelta(hours=24)).isoformat()

    # Export Excel
    try:
        excel_path = export_to_excel()
        logger.info("Excel exported to %s", excel_path)
    except Exception as e:
        logger.error("Excel export failed: %s", e, exc_info=True)

    # Send email
    if send_email:
        try:
            send_report(since)
        except Exception as e:
            logger.error("Email report failed: %s", e, exc_info=True)


def print_summary(jobs: list[dict]) -> None:
    """Print a text summary of results to console."""
    from src.db import get_new_jobs_since

    since = (datetime.now() - timedelta(hours=24)).isoformat()
    new_jobs = get_new_jobs_since(since)

    us = [j for j in new_jobs if j.get("region") == "US"]
    eu = [j for j in new_jobs if j.get("region") == "EU"]
    asia = [j for j in new_jobs if j.get("region") == "Asia"]
    other = [j for j in new_jobs if j.get("region") not in ("US", "EU", "Asia")]

    print(f"\n{'='*70}")
    print(f" Job Search Pipeline Results — {datetime.now().strftime('%b %d, %Y')}")
    print(f"{'='*70}")
    print(f" Total scraped: {len(jobs)}")
    print(f" New jobs (24h): {len(new_jobs)}")
    print(f"   US: {len(us)}  |  EU: {len(eu)}  |  Asia: {len(asia)}  |  Other: {len(other)}")
    print(f"{'='*70}")

    for region_name, region_jobs in [("US", us), ("EU", eu), ("Asia/Other", asia + other)]:
        if region_jobs:
            print(f"\n── {region_name} ({len(region_jobs)}) ──")
            for i, j in enumerate(region_jobs[:10], 1):
                inst = j.get("institute") or "-"
                tier = j.get("tier")
                tier_str = f"T{tier}" if tier else ""
                pi = j.get("pi_name") or ""
                country = j.get("country") or ""
                title = (j.get("title") or "-")[:50]
                source = j.get("source") or ""
                keywords = j.get("keywords") or ""
                conditions = j.get("conditions") or ""
                print(f"  {i}. {title}")
                print(f"     {inst} {tier_str} | {country} [{source}]")
                if pi:
                    print(f"     PI: {pi}")
                if keywords:
                    print(f"     Keywords: {keywords[:80]}")
                if conditions:
                    print(f"     Conditions: {conditions[:80]}")
            if len(region_jobs) > 10:
                print(f"  ... and {len(region_jobs) - 10} more")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Postdoc Job Search Pipeline")
    parser.add_argument("--email", action="store_true", help="Send email report")
    parser.add_argument("--no-email", action="store_true", help="Skip email")
    parser.add_argument("--weekly", action="store_true", help="Run weekly PI discovery")
    parser.add_argument("--summary", action="store_true", help="Print text summary")
    parser.add_argument("--export-only", action="store_true", help="Only export Excel")
    parser.add_argument("--backfill-pi", action="store_true", help="Backfill PI URLs for existing jobs")
    parser.add_argument("--sequential", action="store_true", help="Run scrapers sequentially (debug mode)")
    parser.add_argument("--skip-pi-lookup", action="store_true", help="Skip PI URL enrichment (faster)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    init_db()

    if args.backfill_pi:
        from src.matching.backfill_pi_urls import backfill
        logger.info("Running PI URL backfill...")
        result = backfill()
        logger.info("Backfill result: %s", result)

    if args.export_only:
        run_report(send_email=False)
        return

    if args.weekly:
        run_weekly_discovery()

    # Daily scrape + score
    jobs = run_scrapers(sequential=args.sequential)
    jobs = run_scoring(jobs)

    # Deep enrichment: resolve aggregators + single-name PIs
    try:
        from src.matching.job_enricher import enrich_jobs_deep
        jobs = enrich_jobs_deep(jobs)
    except Exception as e:
        logger.error("Deep enrichment failed: %s", e, exc_info=True)

    # PI URL enrichment (batch, after scoring)
    if not args.skip_pi_lookup:
        jobs = run_pi_enrichment(jobs)

    # Dept URL enrichment (always runs, uses persistent cache)
    jobs = run_dept_enrichment(jobs)

    # Export Excel (always)
    try:
        from src.reporting.excel_export import export_to_excel
        excel_path = export_to_excel()
        logger.info("Excel exported to %s", excel_path)
    except Exception as e:
        logger.error("Excel export failed: %s", e, exc_info=True)

    if args.summary:
        print_summary(jobs)

    if not args.no_email:
        run_report(send_email=True if args.email else False)


if __name__ == "__main__":
    main()
