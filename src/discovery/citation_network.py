"""Citation network explorer.

Uses the Semantic Scholar API to trace who cites seed-PI papers (interested
researchers) and who seed PIs cite (research foundations), then extracts
corresponding / last authors and adds them to the database.

Supports batch paper fetching and parallel citation exploration via
``concurrent.futures`` while respecting S2 rate limits.
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

import requests as _req

from semanticscholar import SemanticScholar

from src import db
from src.config import CV_KEYWORDS, SEMANTIC_SCHOLAR_API_KEY

logger = logging.getLogger(__name__)

# Semantic Scholar free tier: 100 requests per 5 minutes
_S2_DELAY = 3.1  # seconds between requests (safe margin)
_RECENT_YEARS = 5
_MAX_WORKERS = 3  # parallel citation fetchers (rate-limit safe)
_BATCH_SIZE = 50  # S2 batch endpoint max

_s2_client: Optional[SemanticScholar] = None
_s2_lock = threading.Lock()


def _get_s2_client() -> SemanticScholar:
    global _s2_client
    if _s2_client is None:
        with _s2_lock:
            if _s2_client is None:
                kwargs = {}
                if SEMANTIC_SCHOLAR_API_KEY:
                    kwargs["api_key"] = SEMANTIC_SCHOLAR_API_KEY
                _s2_client = SemanticScholar(**kwargs)
    return _s2_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_field_relevant(title: str, abstract: str) -> bool:
    """Return True if *title* or *abstract* contains any CV keyword."""
    combined = f"{title} {abstract}".lower()
    for kw in CV_KEYWORDS:
        if kw.lower() in combined:
            return True
    return False


def _extract_corresponding_author(authors: list) -> Optional[dict]:
    """Extract the corresponding (last) author from a paper's author list.

    Returns a dict with ``name`` and ``semantic_id``, or None.
    """
    if not authors:
        return None
    # Convention: corresponding author is typically the last author
    last = authors[-1]
    name = getattr(last, "name", None) or (last.get("name") if isinstance(last, dict) else None)
    author_id = getattr(last, "authorId", None) or (last.get("authorId") if isinstance(last, dict) else None)
    if not name:
        return None
    return {"name": name, "semantic_id": author_id or ""}


def _add_discovered_pi(
    author_info: dict,
    source_pi_id: int,
    source_pi_name: str,
    direction: str,
) -> Optional[int]:
    """Add a discovered PI to the database.

    Parameters
    ----------
    author_info : dict
        Must contain ``name`` and optionally ``semantic_id``.
    source_pi_id : int
        The seed PI id this discovery originated from.
    source_pi_name : str
        Name of the seed PI (for connected_seeds).
    direction : str
        ``"citing"`` or ``"cited_by"`` -- for logging.

    Returns
    -------
    int or None
        The PI id if stored, else None.
    """
    name = author_info.get("name", "").strip()
    if not name:
        return None

    pi_record: dict = {
        "name": name,
        "institute": "",
        "is_recommended": 1,
        "connected_seeds": source_pi_name,
    }
    if author_info.get("semantic_id"):
        pi_record["semantic_id"] = author_info["semantic_id"]

    pi_id, is_new = db.upsert_pi(pi_record)
    if is_new:
        logger.info(
            "New PI via %s citation: %s (source: %s)", direction, name, source_pi_name
        )
    else:
        # Append connected seed
        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT connected_seeds FROM pis WHERE id = ?", (pi_id,)
            ).fetchone()
            existing = row["connected_seeds"] or "" if row else ""
            if source_pi_name not in existing:
                updated = f"{existing}, {source_pi_name}" if existing else source_pi_name
                conn.execute(
                    "UPDATE pis SET connected_seeds = ?, is_recommended = 1, "
                    "updated_at = datetime('now') WHERE id = ?",
                    (updated, pi_id),
                )

    # Record in citations table
    with db.get_connection() as conn:
        if direction == "citing":
            # The discovered PI cited a seed PI's paper
            existing_cite = conn.execute(
                "SELECT id, citation_count FROM citations "
                "WHERE citing_pi_id = ? AND cited_pi_id = ?",
                (pi_id, source_pi_id),
            ).fetchone()
        else:
            # The seed PI cited the discovered PI's paper
            existing_cite = conn.execute(
                "SELECT id, citation_count FROM citations "
                "WHERE citing_pi_id = ? AND cited_pi_id = ?",
                (source_pi_id, pi_id),
            ).fetchone()

        if existing_cite:
            conn.execute(
                "UPDATE citations SET citation_count = citation_count + 1 WHERE id = ?",
                (existing_cite["id"],),
            )
        else:
            if direction == "citing":
                conn.execute(
                    "INSERT INTO citations (citing_pi_id, cited_pi_id, citation_count) "
                    "VALUES (?, ?, 1)",
                    (pi_id, source_pi_id),
                )
            else:
                conn.execute(
                    "INSERT INTO citations (citing_pi_id, cited_pi_id, citation_count) "
                    "VALUES (?, ?, 1)",
                    (source_pi_id, pi_id),
                )

    return pi_id


# ---------------------------------------------------------------------------
# Core: Fetch citing / cited papers
# ---------------------------------------------------------------------------

def _get_seed_papers(semantic_id: str) -> list[dict]:
    """Fetch recent papers for a seed PI from Semantic Scholar.

    Returns a list of dicts with ``paperId``, ``title``, ``year``.
    """
    s2 = _get_s2_client()
    try:
        author = s2.get_author(
            semantic_id,
            fields=["papers", "papers.paperId", "papers.title", "papers.year"],
        )
        time.sleep(_S2_DELAY)
    except Exception:
        logger.exception("Error fetching papers for S2 author %s", semantic_id)
        return []

    if author is None or not author.papers:
        return []

    cutoff = datetime.now().year - _RECENT_YEARS
    results: list[dict] = []
    for p in author.papers:
        if p.year is not None and p.year >= cutoff:
            results.append(
                {"paperId": p.paperId, "title": p.title or "", "year": p.year}
            )
    return results


def _batch_get_papers(paper_ids: list[str], fields: list[str]) -> list[dict]:
    """Fetch multiple papers in a single batch POST to the S2 API.

    Uses the ``/graph/v1/paper/batch`` endpoint to reduce individual
    request count and stay within rate limits.

    Returns a list of paper dicts (may contain ``None`` entries for
    papers that could not be found).
    """
    if not paper_ids:
        return []

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if SEMANTIC_SCHOLAR_API_KEY:
        headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

    results: list[dict] = []
    for batch_start in range(0, len(paper_ids), _BATCH_SIZE):
        batch = paper_ids[batch_start : batch_start + _BATCH_SIZE]
        try:
            resp = _req.post(
                "https://api.semanticscholar.org/graph/v1/paper/batch",
                headers=headers,
                json={"ids": batch},
                params={"fields": ",".join(fields)},
                timeout=30,
            )
            time.sleep(_S2_DELAY)

            if resp.status_code == 429:
                logger.warning("S2 rate limited during batch fetch, sleeping 60s")
                time.sleep(60)
                resp = _req.post(
                    "https://api.semanticscholar.org/graph/v1/paper/batch",
                    headers=headers,
                    json={"ids": batch},
                    params={"fields": ",".join(fields)},
                    timeout=30,
                )
                time.sleep(_S2_DELAY)

            resp.raise_for_status()
            results.extend(resp.json())
        except Exception:
            logger.exception("Batch paper fetch failed for %d papers", len(batch))
            results.extend([None] * len(batch))

    return results


def _explore_citations_for_paper(
    paper_id: str,
    source_pi_id: int,
    source_pi_name: str,
    direction: str,
    max_results: int = 50,
) -> int:
    """Fetch citations (citing or references) for a paper and add relevant PIs.

    Parameters
    ----------
    direction : str
        ``"citing"`` -- who cited this paper (forward citations).
        ``"cited_by"`` -- papers this paper references (backward citations).

    Returns
    -------
    int
        Number of new PIs discovered.
    """
    s2 = _get_s2_client()
    discovered = 0

    try:
        paper = s2.get_paper(
            paper_id,
            fields=[
                "citations" if direction == "citing" else "references",
                "citations.paperId",
                "citations.title",
                "citations.abstract",
                "citations.authors",
                "references.paperId",
                "references.title",
                "references.abstract",
                "references.authors",
            ],
        )
        time.sleep(_S2_DELAY)
    except Exception:
        logger.exception("Error fetching %s for paper %s", direction, paper_id)
        return 0

    if paper is None:
        return 0

    related_papers = (
        paper.citations if direction == "citing" else paper.references
    )
    if not related_papers:
        return 0

    for rp in related_papers[:max_results]:
        title = rp.title or ""
        abstract = rp.abstract or ""
        if not _is_field_relevant(title, abstract):
            continue

        author_info = _extract_corresponding_author(rp.authors or [])
        if author_info is None:
            continue

        pi_id = _add_discovered_pi(author_info, source_pi_id, source_pi_name, direction)
        if pi_id is not None:
            discovered += 1

    return discovered


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _explore_paper_both_directions(
    paper_id: str,
    paper_title: str,
    pi_id: int,
    pi_name: str,
) -> tuple[int, int]:
    """Explore both citation directions for a single paper.

    Returns ``(forward_count, backward_count)``.
    """
    fwd = _explore_citations_for_paper(
        paper_id, pi_id, pi_name, direction="citing"
    )
    bwd = _explore_citations_for_paper(
        paper_id, pi_id, pi_name, direction="cited_by"
    )
    logger.debug(
        "Paper '%s': %d forward, %d backward discoveries",
        paper_title[:60],
        fwd,
        bwd,
    )
    return fwd, bwd


def build_citation_network() -> dict:
    """Trace citation networks from all seed PIs.

    For each seed PI with a Semantic Scholar ID:
    1. Fetch their recent papers.
    2. For each paper, find who cited it (forward) and who it cites (backward)
       using parallel workers.
    3. Extract corresponding/last authors and add relevant ones to the DB.

    Returns
    -------
    dict
        Summary: ``{"seed_pis_processed": int, "forward_discovered": int,
        "backward_discovered": int}``.
    """
    seed_pis = db.get_seed_pis()
    if not seed_pis:
        logger.info("No seed PIs in database.")
        return {"seed_pis_processed": 0, "forward_discovered": 0, "backward_discovered": 0}

    logger.info("Building citation network from %d seed PIs", len(seed_pis))

    total_forward = 0
    total_backward = 0
    processed = 0

    for pi in seed_pis:
        semantic_id = pi.get("semantic_id")
        if not semantic_id:
            logger.info(
                "Skipping %s -- no Semantic Scholar ID (run seed_profiler first)",
                pi["name"],
            )
            continue

        pi_name = pi["name"]
        pi_id = pi["id"]
        processed += 1

        papers = _get_seed_papers(semantic_id)
        logger.info("PI %s: %d recent papers to explore", pi_name, len(papers))

        # Parallel exploration of citation directions across papers
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    _explore_paper_both_directions,
                    paper["paperId"],
                    paper["title"],
                    pi_id,
                    pi_name,
                ): paper["paperId"]
                for paper in papers
            }

            for future in as_completed(futures):
                try:
                    fwd, bwd = future.result()
                    total_forward += fwd
                    total_backward += bwd
                except Exception:
                    logger.exception(
                        "Error exploring citations for paper %s",
                        futures[future],
                    )

    summary = {
        "seed_pis_processed": processed,
        "forward_discovered": total_forward,
        "backward_discovered": total_backward,
    }
    logger.info("Citation network complete: %s", summary)
    return summary
