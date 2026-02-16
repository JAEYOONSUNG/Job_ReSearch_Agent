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


def run_scrapers() -> list[dict]:
    """Run all scrapers and collect jobs."""
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
    try:
        from src.scrapers.jobs_ac_kr import JobsAcKrScraper
        scrapers.append(JobsAcKrScraper())
    except ImportError:
        logger.warning("jobs_ac_kr scraper unavailable")

    try:
        from src.scrapers.wanted import WantedScraper
        scrapers.append(WantedScraper())
    except ImportError:
        logger.warning("wanted scraper unavailable")

    all_jobs = []
    for scraper in scrapers:
        try:
            logger.info("Running scraper: %s", scraper.name)
            jobs = scraper.run()
            all_jobs.extend(jobs)
            log_scrape(scraper.name, "success", len(jobs), len(jobs))
            logger.info("%s: found %d jobs", scraper.name, len(jobs))
        except Exception as e:
            logger.error("%s failed: %s", scraper.name, e, exc_info=True)
            log_scrape(scraper.name, "error", error=str(e))

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
                country = j.get("country") or ""
                title = (j.get("title") or "-")[:50]
                source = j.get("source") or ""
                print(f"  {i}. {title}")
                print(f"     {inst} {tier_str} | {country} [{source}]")
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
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    setup_logging(args.verbose)
    init_db()

    if args.export_only:
        run_report(send_email=False)
        return

    if args.weekly:
        run_weekly_discovery()

    # Daily scrape + score
    jobs = run_scrapers()
    jobs = run_scoring(jobs)

    if args.summary:
        print_summary(jobs)

    if args.email and not args.no_email:
        run_report(send_email=True)
    elif not args.no_email:
        run_report(send_email=False)


if __name__ == "__main__":
    main()
