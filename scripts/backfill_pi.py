"""Re-apply PI extraction with improved patterns to existing DB records.

Finds jobs with pi_name IS NULL, attempts extraction from title and
description using the latest patterns, and updates the DB.

Usage:
    python scripts/backfill_pi.py [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_connection, init_db
from src.matching.job_parser import (
    extract_pi_from_title,
    extract_pi_name,
    expand_pi_last_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("backfill_pi")


def backfill(dry_run: bool = False) -> None:
    init_db()

    with get_connection() as conn:
        rows = conn.execute(
            "SELECT id, title, description, pi_name FROM jobs WHERE pi_name IS NULL"
        ).fetchall()

    logger.info("Found %d jobs without PI name", len(rows))

    updated = 0
    for row in rows:
        job_id = row["id"]
        title = row["title"] or ""
        desc = row["description"] or ""
        pi_name: str | None = None

        # 1. Try title Lab patterns (e.g. "Badran Lab")
        pi_from_title = extract_pi_from_title(title)
        if pi_from_title:
            pi_name = pi_from_title
            # Try to expand single last name to full name
            if " " not in pi_name and desc:
                full = expand_pi_last_name(pi_name, desc)
                if full:
                    pi_name = full

        # 2. Try description patterns
        if not pi_name and desc:
            pi_name = extract_pi_name(desc)

        if pi_name:
            if dry_run:
                logger.info("[DRY-RUN] id=%d  pi=%s  title=%s", job_id, pi_name, title[:80])
            else:
                with get_connection() as conn:
                    conn.execute(
                        "UPDATE jobs SET pi_name = ?, updated_at = datetime('now') WHERE id = ?",
                        (pi_name, job_id),
                    )
            updated += 1

    logger.info(
        "%s %d of %d jobs",
        "Would update" if dry_run else "Updated",
        updated,
        len(rows),
    )

    # Print per-source statistics
    with get_connection() as conn:
        stats = conn.execute(
            "SELECT source, COUNT(*) as total, "
            "SUM(CASE WHEN pi_name IS NOT NULL THEN 1 ELSE 0 END) as with_pi "
            "FROM jobs GROUP BY source"
        ).fetchall()

    logger.info("--- PI extraction stats by source ---")
    for s in stats:
        total = s["total"]
        with_pi = s["with_pi"]
        pct = 100.0 * with_pi / total if total else 0
        logger.info("  %-20s  %3d / %3d  (%.1f%%)", s["source"], with_pi, total, pct)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill PI names in existing DB records")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing to DB")
    args = parser.parse_args()
    backfill(dry_run=args.dry_run)
