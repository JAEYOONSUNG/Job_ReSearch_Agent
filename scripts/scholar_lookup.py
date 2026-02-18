"""Batch Google Scholar + paper lookup for PIs in the database.

Runs sequentially (Google Scholar blocks parallel requests) with
progress output and resume support (only processes PIs without scholar_url).

Usage:
    python scripts/scholar_lookup.py                  # full run
    python scripts/scholar_lookup.py --dry-run        # preview candidates
    python scripts/scholar_lookup.py --limit 10       # process 10 PIs only
    python scripts/scholar_lookup.py --skip-papers    # skip S2 paper fetch
    python scripts/scholar_lookup.py --export         # export Excel after
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db import get_connection, init_db
from src.discovery.scholar_scraper import search_scholar_author
from src.discovery.seed_profiler import (
    fetch_pi_papers,
    fetch_semantic_scholar_metadata,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
logger = logging.getLogger("scholar_lookup")


def _get_candidates() -> list[dict]:
    """Get jobs with pi_name but no scholar_url (resume support)."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT pi_name, institute, department"
            " FROM jobs"
            " WHERE pi_name IS NOT NULL"
            "   AND pi_name != ''"
            "   AND (scholar_url IS NULL OR scholar_url = '')"
            "   AND pi_name LIKE '% %'"  # skip single-name PIs
            " ORDER BY pi_name"
        ).fetchall()
    return [dict(r) for r in rows]


def _update_jobs(pi_name: str, institute: str | None, updates: dict) -> int:
    """Apply updates to all matching jobs. Returns count of updated rows."""
    if not updates:
        return 0

    # s2_author_id is a pis-only column, not on jobs
    _PI_ONLY_KEYS = {"s2_author_id"}

    set_parts = []
    values = []
    for k, v in updates.items():
        if v is not None and k not in _PI_ONLY_KEYS:
            set_parts.append(f"{k} = ?")
            values.append(v)

    if not set_parts:
        return 0

    set_parts.append("updated_at = datetime('now')")
    set_clause = ", ".join(set_parts)

    where = "pi_name = ?"
    values.append(pi_name)
    if institute:
        where += " AND institute = ?"
        values.append(institute)

    with get_connection() as conn:
        cursor = conn.execute(
            f"UPDATE jobs SET {set_clause} WHERE {where}",
            values,
        )
        return cursor.rowcount


def _update_pi_cache(pi_name: str, institute: str | None, updates: dict) -> None:
    """Upsert PI data into the pis cache table."""
    from src.db import upsert_pi
    from datetime import datetime

    record = {"name": pi_name, "last_scraped": datetime.now().isoformat()}
    if institute:
        record["institute"] = institute
    record.update({k: v for k, v in updates.items() if v is not None})
    upsert_pi(record)


def run(
    dry_run: bool = False,
    limit: int | None = None,
    skip_papers: bool = False,
    export: bool = False,
) -> None:
    init_db()

    candidates = _get_candidates()
    total = len(candidates)

    if limit:
        candidates = candidates[:limit]

    logger.info("Scholar lookup: %d candidates (of %d total PIs without scholar_url)", len(candidates), total)

    if dry_run:
        for i, c in enumerate(candidates, 1):
            pi = c["pi_name"]
            inst = c.get("institute") or "?"
            logger.info("[%d/%d] %s @ %s", i, len(candidates), pi, inst)
        logger.info("DRY RUN complete — %d PIs would be processed", len(candidates))
        return

    found_scholar = 0
    found_papers = 0

    for i, c in enumerate(candidates, 1):
        pi_name = c["pi_name"]
        institute = c.get("institute")
        department = c.get("department")
        pct = 100 * i // len(candidates)

        updates: dict = {}
        status_parts: list[str] = []

        # Step 1: Google Scholar direct scraping
        gs_data = search_scholar_author(pi_name, institute)
        if gs_data:
            updates["scholar_url"] = gs_data.get("scholar_url")
            if gs_data.get("cited_by"):
                updates["citations"] = gs_data["cited_by"]
            status_parts.append("scholar_url FOUND")
            found_scholar += 1
        else:
            status_parts.append("scholar_url -")

        # Step 2: Semantic Scholar metadata (for h_index + authorId)
        s2_author_id = None
        s2_meta = fetch_semantic_scholar_metadata(pi_name, institute)
        if s2_meta:
            if s2_meta.get("h_index") is not None:
                updates["h_index"] = s2_meta["h_index"]
            if s2_meta.get("citationCount") is not None and "citations" not in updates:
                updates["citations"] = s2_meta["citationCount"]
            s2_author_id = s2_meta.get("authorId")
            if s2_author_id:
                updates["s2_author_id"] = s2_author_id

        # Step 3: Paper fetch (unless --skip-papers)
        paper_counts = ""
        if not skip_papers and s2_author_id:
            paper_data = fetch_pi_papers(pi_name, institute, s2_author_id=s2_author_id)
            if paper_data:
                recent = paper_data["recent_papers"]
                top_cited = paper_data["top_cited_papers"]
                updates["recent_papers"] = json.dumps(recent)
                updates["top_cited_papers"] = json.dumps(top_cited)
                paper_counts = f"{len(recent)}+{len(top_cited)}"
                found_papers += 1
            else:
                paper_counts = "0"
        elif skip_papers:
            paper_counts = "skipped"
        else:
            paper_counts = "no-id"

        status_parts.append(f"papers: {paper_counts}")

        # Apply updates to DB
        rows_updated = _update_jobs(pi_name, institute, updates)
        _update_pi_cache(pi_name, institute, updates)

        logger.info(
            "[%d/%d] (%d%%) %s @ %s -> %s | rows=%d",
            i, len(candidates), pct,
            pi_name, institute or "?",
            " | ".join(status_parts),
            rows_updated,
        )

    logger.info(
        "Scholar lookup complete: %d/%d scholar_url found, %d/%d with papers",
        found_scholar, len(candidates),
        found_papers, len(candidates),
    )

    # Print DB stats
    with get_connection() as conn:
        total_jobs = conn.execute("SELECT COUNT(*) FROM jobs WHERE pi_name IS NOT NULL").fetchone()[0]
        with_scholar = conn.execute("SELECT COUNT(*) FROM jobs WHERE scholar_url IS NOT NULL AND scholar_url != ''").fetchone()[0]
        with_papers = conn.execute("SELECT COUNT(*) FROM jobs WHERE recent_papers IS NOT NULL AND recent_papers != ''").fetchone()[0]

    logger.info("--- DB Stats ---")
    logger.info("  Jobs with PI name:   %d", total_jobs)
    logger.info("  Jobs with scholar_url: %d (%.1f%%)", with_scholar, 100 * with_scholar / max(total_jobs, 1))
    logger.info("  Jobs with papers:      %d (%.1f%%)", with_papers, 100 * with_papers / max(total_jobs, 1))

    if export:
        logger.info("Exporting Excel...")
        from src.reporting.excel_export import export_to_excel
        path = export_to_excel()
        logger.info("Excel exported to %s", path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch Google Scholar + paper lookup for PIs"
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview candidates without making requests")
    parser.add_argument("--limit", type=int, default=None, help="Process only N PIs")
    parser.add_argument("--skip-papers", action="store_true", help="Skip Semantic Scholar paper fetch")
    parser.add_argument("--export", action="store_true", help="Export Excel after completion")
    args = parser.parse_args()

    run(
        dry_run=args.dry_run,
        limit=args.limit,
        skip_papers=args.skip_papers,
        export=args.export,
    )
