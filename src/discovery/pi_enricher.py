"""Enrich recommended PIs with missing metadata from Semantic Scholar.

Many PIs discovered via the coauthor network (coauthor_network.py) have
h_index and semantic_id but are missing institute, scholar_url, lab_url,
fields, and s2_author_id.  This module back-fills that data by searching
Semantic Scholar, matching by name + h_index (within +/-5) for disambiguation.

Usage:
    python -m src.discovery.pi_enricher               # enrich all
    python -m src.discovery.pi_enricher --limit 50    # first 50 only
    python -m src.discovery.pi_enricher --dry-run     # preview without writes
"""

import argparse
import logging
import time
from collections import Counter
from typing import Optional

import requests

from src import db
from src.config import SEMANTIC_SCHOLAR_API_KEY

logger = logging.getLogger(__name__)

_S2_API_BASE = "https://api.semanticscholar.org/graph/v1"
_S2_DELAY = 1.1  # ~1 req/s; conservative for unauthenticated, fast with key
_S2_RATE_LIMIT_DELAY = 30  # seconds to wait on 429
_H_INDEX_TOLERANCE = 5  # +/- tolerance for h-index matching
_BATCH_LOG_INTERVAL = 25  # log progress every N PIs


# ---------------------------------------------------------------------------
# Semantic Scholar helpers
# ---------------------------------------------------------------------------

def _s2_headers() -> dict:
    """Return request headers, including API key if configured."""
    headers: dict[str, str] = {}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY
    return headers


def _search_author(
    name: str,
    expected_h_index: Optional[int] = None,
) -> Optional[dict]:
    """Search Semantic Scholar for an author by name, disambiguating via h_index.

    Returns a dict with keys: authorId, name, hIndex, citationCount,
    affiliations, papers (list with title + fieldsOfStudy).

    Matching logic:
    - Fetch up to 20 candidates from the S2 author search API
    - If expected_h_index is provided, filter to candidates within +/-5
    - Among remaining candidates, pick the one with the closest h_index
    - If no h_index filter applies, pick the highest h_index candidate

    Returns None if no suitable match is found.
    """
    try:
        params = {
            "query": name,
            "fields": (
                "authorId,name,hIndex,citationCount,affiliations,"
                "papers.title,papers.fieldsOfStudy"
            ),
            "limit": 20,
        }
        resp = requests.get(
            f"{_S2_API_BASE}/author/search",
            params=params,
            headers=_s2_headers(),
            timeout=15,
        )

        if resp.status_code == 429:
            logger.warning("S2 rate-limited searching for %s; sleeping %ds", name, _S2_RATE_LIMIT_DELAY)
            time.sleep(_S2_RATE_LIMIT_DELAY)
            # Retry once
            resp = requests.get(
                f"{_S2_API_BASE}/author/search",
                params=params,
                headers=_s2_headers(),
                timeout=15,
            )
            if resp.status_code == 429:
                logger.error("S2 still rate-limited for %s after retry", name)
                return None

        resp.raise_for_status()
        data = resp.json().get("data", [])

        if not data:
            logger.debug("No S2 results for %s", name)
            return None

        # Filter by h-index tolerance if we have an expected value
        if expected_h_index is not None:
            candidates = [
                c for c in data
                if c.get("hIndex") is not None
                and abs(c["hIndex"] - expected_h_index) <= _H_INDEX_TOLERANCE
            ]
            if not candidates:
                # Fallback: relax filter, pick closest h-index among all
                candidates = [c for c in data if c.get("hIndex") is not None]
                if not candidates:
                    logger.debug("No S2 candidates with h-index for %s", name)
                    return None

            # Pick the candidate with closest h-index to expected
            best = min(
                candidates,
                key=lambda c: abs((c.get("hIndex") or 0) - expected_h_index),
            )
        else:
            # No h-index to match on; pick highest h-index
            best = max(data, key=lambda c: c.get("hIndex") or 0)

        return best

    except requests.RequestException:
        logger.exception("S2 request error searching for %s", name)
        return None
    except Exception:
        logger.exception("Unexpected error searching S2 for %s", name)
        return None


def _extract_fields(papers: list[dict], top_n: int = 5) -> str:
    """Extract the top research fields from a list of S2 paper dicts.

    Each paper dict should have a ``fieldsOfStudy`` key (list of strings).
    Returns a comma-separated string of the most frequent fields.
    """
    counter: Counter = Counter()
    for paper in papers or []:
        for field in paper.get("fieldsOfStudy") or []:
            if field:
                counter[field] += 1

    if not counter:
        return ""

    top_fields = [field for field, _ in counter.most_common(top_n)]
    return ", ".join(top_fields)


# ---------------------------------------------------------------------------
# Core enrichment
# ---------------------------------------------------------------------------

def _get_pis_needing_enrichment(limit: Optional[int] = None) -> list[dict]:
    """Fetch recommended PIs that are missing fields, institute, or scholar_url.

    Skips PIs that already have ``fields`` populated (already enriched).
    """
    query = (
        "SELECT * FROM pis "
        "WHERE is_recommended = 1 "
        "  AND (fields IS NULL OR fields = '') "
        "ORDER BY h_index DESC"
    )
    params: list = []
    if limit:
        query += " LIMIT ?"
        params.append(limit)

    with db.get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def _update_pi_enrichment(
    pi_id: int,
    *,
    institute: Optional[str] = None,
    fields: Optional[str] = None,
    scholar_url: Optional[str] = None,
    s2_author_id: Optional[str] = None,
) -> None:
    """Update a PI record with enriched metadata.

    Only updates columns that have non-empty values provided.
    """
    updates: dict[str, str] = {}
    if institute:
        updates["institute"] = institute
    if fields:
        updates["fields"] = fields
    if scholar_url:
        updates["scholar_url"] = scholar_url
    if s2_author_id:
        updates["s2_author_id"] = s2_author_id

    if not updates:
        return

    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [pi_id]

    with db.get_connection() as conn:
        conn.execute(
            f"UPDATE pis SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
            values,
        )


def enrich_single_pi(pi: dict, dry_run: bool = False) -> dict:
    """Enrich a single PI record from Semantic Scholar.

    Parameters
    ----------
    pi : dict
        PI row from the database (must have at least ``id`` and ``name``).
    dry_run : bool
        If True, return enrichment data without writing to the database.

    Returns
    -------
    dict
        The enrichment data found (may be empty if no match).
    """
    pi_id = pi["id"]
    name = pi["name"]
    h_index = pi.get("h_index")
    semantic_id = pi.get("semantic_id")

    enrichment: dict = {}

    # If we already have a semantic_id (S2 author ID from coauthor network),
    # we can use it directly to construct the scholar_url and set s2_author_id
    if semantic_id:
        enrichment["s2_author_id"] = semantic_id
        enrichment["scholar_url"] = f"https://www.semanticscholar.org/author/{semantic_id}"

    # Search S2 to fill in remaining fields (institute, research fields)
    s2_result = _search_author(name, expected_h_index=h_index)
    time.sleep(_S2_DELAY)

    if s2_result:
        author_id = s2_result.get("authorId")
        if author_id:
            enrichment.setdefault("s2_author_id", author_id)
            enrichment.setdefault(
                "scholar_url",
                f"https://www.semanticscholar.org/author/{author_id}",
            )

        # Extract institute from affiliations
        affiliations = s2_result.get("affiliations") or []
        if affiliations and not pi.get("institute"):
            enrichment["institute"] = affiliations[0]

        # Extract research fields from papers
        papers = s2_result.get("papers") or []
        fields = _extract_fields(papers)
        if fields:
            enrichment["fields"] = fields
    else:
        logger.debug("No S2 match for %s (h=%s)", name, h_index)

    # Persist if not dry run
    if enrichment and not dry_run:
        _update_pi_enrichment(
            pi_id,
            institute=enrichment.get("institute"),
            fields=enrichment.get("fields"),
            scholar_url=enrichment.get("scholar_url"),
            s2_author_id=enrichment.get("s2_author_id"),
        )

    return enrichment


def enrich_recommended_pis(
    limit: Optional[int] = None,
    dry_run: bool = False,
) -> dict:
    """Enrich all recommended PIs missing critical metadata.

    For each PI, searches Semantic Scholar by name + h_index to find the
    correct author and fills in: institute, fields, scholar_url, s2_author_id.

    Parameters
    ----------
    limit : int, optional
        Maximum number of PIs to process (None = all).
    dry_run : bool
        If True, compute enrichments without writing to the database.

    Returns
    -------
    dict
        Summary statistics: total, enriched, skipped, failed.
    """
    pis = _get_pis_needing_enrichment(limit=limit)
    total = len(pis)

    if total == 0:
        logger.info("No PIs need enrichment.")
        return {"total": 0, "enriched": 0, "skipped": 0, "failed": 0}

    logger.info(
        "Enriching %d recommended PIs from Semantic Scholar%s",
        total,
        f" (limit={limit})" if limit else "",
    )

    enriched = 0
    skipped = 0
    failed = 0

    for i, pi in enumerate(pis, 1):
        name = pi["name"]

        try:
            result = enrich_single_pi(pi, dry_run=dry_run)

            if result:
                enriched += 1
                fields_str = result.get("fields", "")[:60]
                inst_str = result.get("institute", "")[:40]
                logger.info(
                    "[%d/%d] Enriched: %s — institute=%s, fields=%s",
                    i, total, name, inst_str or "(none)", fields_str or "(none)",
                )
            else:
                skipped += 1
                logger.debug("[%d/%d] Skipped (no S2 data): %s", i, total, name)

        except Exception:
            failed += 1
            logger.exception("[%d/%d] Error enriching %s", i, total, name)

        # Batch progress logging
        if i % _BATCH_LOG_INTERVAL == 0:
            logger.info(
                "Progress: %d/%d processed (enriched=%d, skipped=%d, failed=%d)",
                i, total, enriched, skipped, failed,
            )

    summary = {
        "total": total,
        "enriched": enriched,
        "skipped": skipped,
        "failed": failed,
    }
    action = "would be" if dry_run else "were"
    logger.info(
        "Enrichment complete: %d/%d PIs %s enriched (%d skipped, %d failed)",
        enriched, total, action, skipped, failed,
    )
    return summary


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for PI enrichment."""
    parser = argparse.ArgumentParser(
        description="Enrich recommended PIs with Semantic Scholar metadata"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of PIs to process (default: all)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview enrichment without writing to the database",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    db.init_db()
    summary = enrich_recommended_pis(limit=args.limit, dry_run=args.dry_run)

    print(f"\n{'='*60}")
    print(" PI Enrichment Summary")
    print(f"{'='*60}")
    print(f"  Total PIs processed:  {summary['total']}")
    print(f"  Successfully enriched: {summary['enriched']}")
    print(f"  Skipped (no match):    {summary['skipped']}")
    print(f"  Failed (errors):       {summary['failed']}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
