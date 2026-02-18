"""Backfill PI URLs for existing jobs that have pi_name but lack scholar/lab URLs.

Usage:
    python -m src.matching.backfill_pi_urls           # run backfill
    python -m src.matching.backfill_pi_urls --dry-run  # preview only
"""

import argparse
import logging
import sys

from src.db import get_connection, init_db, upsert_job
from src.matching.pi_lookup import lookup_pi_urls

logger = logging.getLogger(__name__)


def _get_backfill_candidates() -> list[dict]:
    """Return jobs that have pi_name but missing scholar_url or lab_url."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, title, pi_name, institute, department, scholar_url, lab_url, dept_url "
            "FROM jobs "
            "WHERE pi_name IS NOT NULL AND pi_name != '' "
            "AND (scholar_url IS NULL OR scholar_url = '' "
            "     OR lab_url IS NULL OR lab_url = '')"
        ).fetchall()
        return [dict(r) for r in rows]


def backfill(dry_run: bool = False) -> dict:
    """Backfill PI URLs for candidate jobs.

    Returns summary: {total, updated, failed, skipped}.
    """
    candidates = _get_backfill_candidates()
    if not candidates:
        logger.info("No jobs need PI URL backfill.")
        return {"total": 0, "updated": 0, "failed": 0, "skipped": 0}

    logger.info("Backfill candidates: %d jobs", len(candidates))

    if dry_run:
        print(f"\n{'='*70}")
        print(f" DRY RUN — {len(candidates)} jobs would be processed")
        print(f"{'='*70}\n")
        for j in candidates:
            scholar = "Y" if j.get("scholar_url") else "N"
            lab = "Y" if j.get("lab_url") else "N"
            dept = "Y" if j.get("dept_url") else "N"
            print(
                f"  [{j['id']}] {j.get('pi_name', '?'):25s} | "
                f"{(j.get('institute') or '?')[:30]:30s} | "
                f"Scholar={scholar} Lab={lab} Dept={dept}"
            )
        print()
        return {"total": len(candidates), "updated": 0, "failed": 0, "skipped": 0}

    updated = 0
    failed = 0
    skipped = 0

    for job in candidates:
        pi_name = job["pi_name"]
        institute = job.get("institute")
        department = job.get("department")
        job_id = job["id"]

        try:
            urls = lookup_pi_urls(pi_name, institute, department)

            update_fields: dict = {}
            for key in ("scholar_url", "lab_url", "dept_url", "h_index", "citations",
                        "recent_papers", "top_cited_papers"):
                if urls.get(key) and not job.get(key):
                    update_fields[key] = urls[key]

            if update_fields:
                with get_connection() as conn:
                    set_clause = ", ".join(f"{k} = ?" for k in update_fields)
                    values = list(update_fields.values()) + [job_id]
                    conn.execute(
                        f"UPDATE jobs SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
                        values,
                    )
                updated += 1
                logger.info(
                    "Updated job %d (%s): %s",
                    job_id,
                    pi_name,
                    list(update_fields.keys()),
                )
            else:
                skipped += 1
                logger.debug("No new URLs found for job %d (%s)", job_id, pi_name)

        except Exception:
            failed += 1
            logger.exception("Failed to backfill job %d (%s)", job_id, pi_name)

    summary = {
        "total": len(candidates),
        "updated": updated,
        "failed": failed,
        "skipped": skipped,
    }
    logger.info("Backfill complete: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill PI URLs for existing jobs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview candidates without making changes",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    init_db()
    result = backfill(dry_run=args.dry_run)

    if not args.dry_run:
        print(
            f"\nBackfill done: {result['updated']} updated, "
            f"{result['skipped']} skipped, {result['failed']} failed "
            f"(out of {result['total']})"
        )


if __name__ == "__main__":
    main()
