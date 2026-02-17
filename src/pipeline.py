"""Main pipeline orchestrator."""

import argparse
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

    scrapers = [
        NatureCareersScraper(),
        JobSpyScraper(),
        EuraxessScraper(),
        AcademicPositionsScraper(),
        ScholarshipDBScraper(),
        ResearchGateScraper(),
        LabWebsiteScraper(),
    ]

    # Conditionally add scrapers that may have import issues
    for cls_path, label in [
        ("src.scrapers.jobs_ac_uk", "JobsAcUkScraper"),
        ("src.scrapers.jobs_ac_kr", "JobsAcKrScraper"),
        ("src.scrapers.wanted", "WantedScraper"),
        ("src.scrapers.glassdoor", "GlassdoorScraper"),
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
    """Run a single scraper with error handling (used by both sequential/parallel)."""
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


def run_scrapers(sequential: bool = False) -> list[dict]:
    """Run all scrapers and collect jobs.

    Parameters
    ----------
    sequential : bool
        If True, run scrapers one-by-one (useful for debugging).
        Default is parallel execution with up to 5 workers.
    """
    scrapers = _build_scrapers()
    all_jobs: list[dict] = []

    if sequential:
        for scraper in scrapers:
            all_jobs.extend(_run_single_scraper(scraper))
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_run_single_scraper, s): s for s in scrapers}
            for future in as_completed(futures, timeout=SCRAPER_TIMEOUT * 2):
                scraper = futures[future]
                try:
                    jobs = future.result(timeout=SCRAPER_TIMEOUT)
                    all_jobs.extend(jobs)
                except TimeoutError:
                    logger.warning(
                        "%s timed out after %ds, skipping", scraper.name, SCRAPER_TIMEOUT
                    )
                except Exception as e:
                    logger.error("%s future failed: %s", scraper.name, e, exc_info=True)

    # Close shared Playwright browser if it was used
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
    jobs = score_and_sort_jobs(jobs, keywords)
    return jobs


def run_pi_enrichment(jobs: list[dict], max_workers: int = 3) -> list[dict]:
    """Batch PI URL lookup for jobs that have a pi_name but missing URLs.

    Runs after scraping/scoring so it doesn't block scrapers.
    Uses a thread pool for moderate parallelism (Scholar rate-limits aggressively).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    candidates = [
        j for j in jobs
        if j.get("pi_name") and not (j.get("scholar_url") and j.get("lab_url"))
    ]
    if not candidates:
        return jobs

    logger.info("PI enrichment: %d jobs need URL lookup", len(candidates))

    def _lookup_one(job: dict) -> None:
        try:
            from src.matching.pi_lookup import lookup_pi_urls
            urls = lookup_pi_urls(
                job["pi_name"], job.get("institute"), job.get("department")
            )
            for key in ("scholar_url", "lab_url", "dept_url", "h_index", "citations"):
                if urls.get(key) and not job.get(key):
                    job[key] = urls[key]
        except Exception:
            logger.debug("PI lookup failed for %s", job.get("pi_name"))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_lookup_one, j): j for j in candidates}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 10 == 0:
                logger.info("PI enrichment progress: %d/%d", done, len(candidates))

    logger.info("PI enrichment complete")
    return jobs


def run_dept_enrichment(jobs: list[dict], max_workers: int = 4) -> list[dict]:
    """Batch department URL lookup for jobs that have an institute but no dept_url.

    Works independently of PI name — uses institute + department (or title)
    to find the department/faculty homepage via DuckDuckGo site-scoped search.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    candidates = [
        j for j in jobs
        if j.get("institute") and not j.get("dept_url")
    ]
    if not candidates:
        return jobs

    logger.info("Dept URL enrichment: %d jobs to look up", len(candidates))

    # Deduplicate by (institute, department) to avoid repeated searches
    seen_keys: dict[tuple, str | None] = {}

    def _make_key(job: dict) -> tuple:
        return (
            (job.get("institute") or "").strip().lower(),
            (job.get("department") or job.get("field") or "").strip().lower(),
        )

    def _lookup_dept(institute: str, dept_hint: str) -> str | None:
        from src.discovery.lab_finder import _institute_to_domain, _search_university_directory
        domain = _institute_to_domain(institute)
        if not domain:
            return None
        # Search for department/field page on the institute domain
        query = dept_hint if dept_hint else "research"
        return _search_university_directory(query, domain)

    def _process_job(job: dict) -> None:
        key = _make_key(job)
        if key in seen_keys:
            url = seen_keys[key]
        else:
            dept_hint = job.get("department") or job.get("field") or ""
            url = _lookup_dept(job["institute"], dept_hint)
            seen_keys[key] = url
        if url:
            job["dept_url"] = url

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_process_job, j): j for j in candidates}
        done = 0
        for future in as_completed(futures):
            done += 1
            if done % 20 == 0:
                logger.info("Dept enrichment progress: %d/%d", done, len(candidates))

    filled = sum(1 for j in candidates if j.get("dept_url"))
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

    # PI URL enrichment (batch, after scoring)
    if not args.skip_pi_lookup:
        jobs = run_pi_enrichment(jobs)
        jobs = run_dept_enrichment(jobs)

    if args.summary:
        print_summary(jobs)

    if args.email and not args.no_email:
        run_report(send_email=True)
    elif not args.no_email:
        run_report(send_email=False)


if __name__ == "__main__":
    main()
