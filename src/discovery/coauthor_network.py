"""Co-author network explorer.

Builds a multi-hop co-authorship graph starting from seed PIs, identifies
cross-connections, and adds field-relevant new PIs to the database as
recommendations.
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Optional

from scholarly import scholarly
from semanticscholar import SemanticScholar

from src import db
from src.config import CV_KEYWORDS, SEMANTIC_SCHOLAR_API_KEY

logger = logging.getLogger(__name__)

_SCHOLARLY_DELAY = 1.0
_S2_DELAY = 3.1  # ~100 req / 5 min free tier
_RECENT_YEARS = 5

_s2_client: Optional[SemanticScholar] = None


def _get_s2_client() -> SemanticScholar:
    global _s2_client
    if _s2_client is None:
        kwargs = {}
        if SEMANTIC_SCHOLAR_API_KEY:
            kwargs["api_key"] = SEMANTIC_SCHOLAR_API_KEY
        _s2_client = SemanticScholar(**kwargs)
    return _s2_client


# ---------------------------------------------------------------------------
# Keyword relevance helpers
# ---------------------------------------------------------------------------

_CV_PATTERN: Optional[re.Pattern] = None


def _cv_regex() -> re.Pattern:
    """Compile (once) a case-insensitive regex for CV_KEYWORDS."""
    global _CV_PATTERN
    if _CV_PATTERN is None:
        escaped = [re.escape(kw) for kw in CV_KEYWORDS]
        _CV_PATTERN = re.compile("|".join(escaped), re.IGNORECASE)
    return _CV_PATTERN


def _is_field_relevant(texts: list[str]) -> bool:
    """Return True if any text in *texts* matches a CV keyword."""
    pattern = _cv_regex()
    for text in texts:
        if text and pattern.search(text):
            return True
    return False


# ---------------------------------------------------------------------------
# Semantic Scholar: fetch recent coauthors
# ---------------------------------------------------------------------------

def _fetch_coauthors_s2(
    semantic_id: str,
) -> list[dict]:
    """Return coauthors from recent papers of the given S2 author.

    Each dict has keys: name, semantic_id, paper_titles, paper_abstracts.
    """
    s2 = _get_s2_client()
    try:
        author = s2.get_author(
            semantic_id,
            fields=[
                "authorId",
                "name",
                "papers",
                "papers.paperId",
                "papers.title",
                "papers.abstract",
                "papers.year",
                "papers.authors",
            ],
        )
        time.sleep(_S2_DELAY)
    except Exception:
        logger.exception("S2 error fetching author %s", semantic_id)
        return []

    if author is None or not author.papers:
        return []

    cutoff_year = datetime.now().year - _RECENT_YEARS
    coauthor_map: dict[str, dict] = {}  # keyed by S2 author ID

    for paper in author.papers:
        if paper.year is None or paper.year < cutoff_year:
            continue
        for a in paper.authors or []:
            if a.authorId and a.authorId != semantic_id:
                entry = coauthor_map.setdefault(
                    a.authorId,
                    {
                        "name": a.name or "",
                        "semantic_id": a.authorId,
                        "paper_titles": [],
                        "paper_abstracts": [],
                    },
                )
                entry["paper_titles"].append(paper.title or "")
                if paper.abstract:
                    entry["paper_abstracts"].append(paper.abstract)

    return list(coauthor_map.values())


def _fetch_coauthors_scholarly(name: str, institute: Optional[str] = None) -> list[str]:
    """Return coauthor names from the Google Scholar profile.

    This is a lightweight fallback when no Semantic Scholar ID is available.
    """
    try:
        query = f"{name} {institute}" if institute else name
        results = scholarly.search_author(query)
        author = next(results, None)
        if author is None:
            return []
        time.sleep(_SCHOLARLY_DELAY)
        author = scholarly.fill(author)
        return [
            ca["name"]
            for ca in author.get("coauthors", [])
            if ca.get("name")
        ]
    except Exception:
        logger.exception("Scholarly error fetching coauthors for %s", name)
        return []


# ---------------------------------------------------------------------------
# Core network builder
# ---------------------------------------------------------------------------

def _process_coauthor(
    coauthor: dict,
    source_pi_id: int,
    source_pi_name: str,
    hop: int,
) -> Optional[int]:
    """Evaluate a single coauthor and, if relevant, add to DB.

    Returns the PI id of the coauthor if stored, else ``None``.
    """
    name = coauthor.get("name", "").strip()
    if not name:
        return None

    # Field relevance check
    texts = coauthor.get("paper_titles", []) + coauthor.get("paper_abstracts", [])
    if not _is_field_relevant(texts):
        logger.debug("Skipping %s (not field-relevant)", name)
        return None

    # Upsert PI
    pi_record: dict = {
        "name": name,
        "institute": "",  # often unknown for coauthors
        "is_recommended": 1,
        "connected_seeds": source_pi_name,
    }
    if coauthor.get("semantic_id"):
        pi_record["semantic_id"] = coauthor["semantic_id"]

    pi_id, is_new = db.upsert_pi(pi_record)
    if is_new:
        logger.info(
            "New PI from coauthor network (hop %d): %s (source: %s)",
            hop,
            name,
            source_pi_name,
        )
    else:
        # Append connected seed if not already listed
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

    # Record coauthorship
    shared = len(coauthor.get("paper_titles", []))
    db.add_coauthorship(source_pi_id, pi_id, shared_papers=shared)

    return pi_id


def build_coauthor_network(max_hops: int = 2) -> dict:
    """Build the full coauthor network from seed PIs.

    Parameters
    ----------
    max_hops : int
        Number of hops to explore (1 = direct coauthors only,
        2 = coauthors of coauthors).

    Returns
    -------
    dict
        Summary statistics: ``{"seed_pis": int, "discovered": int,
        "cross_connections": int}``.
    """
    seed_pis = db.get_seed_pis()
    if not seed_pis:
        logger.info("No seed PIs in database.")
        return {"seed_pis": 0, "discovered": 0, "cross_connections": 0}

    logger.info(
        "Building coauthor network from %d seed PIs (max_hops=%d)",
        len(seed_pis),
        max_hops,
    )

    discovered_total = 0
    cross_connections = 0

    # Track which PI IDs we have seen at each hop so we don't re-explore
    explored_ids: set[int] = {pi["id"] for pi in seed_pis}
    frontier_ids: set[int] = set(explored_ids)

    for hop in range(1, max_hops + 1):
        logger.info("--- Hop %d (frontier size: %d) ---", hop, len(frontier_ids))
        next_frontier: set[int] = set()

        for pi_id in list(frontier_ids):
            # Fetch the PI record
            with db.get_connection() as conn:
                row = conn.execute(
                    "SELECT * FROM pis WHERE id = ?", (pi_id,)
                ).fetchone()
            if row is None:
                continue
            pi = dict(row)
            pi_name = pi["name"]

            # Prefer Semantic Scholar for coauthor extraction
            coauthors: list[dict] = []
            if pi.get("semantic_id"):
                coauthors = _fetch_coauthors_s2(pi["semantic_id"])
            else:
                # Fallback: scholarly (limited info)
                ca_names = _fetch_coauthors_scholarly(pi_name, pi.get("institute"))
                coauthors = [{"name": n, "paper_titles": [], "paper_abstracts": []} for n in ca_names]
                time.sleep(_SCHOLARLY_DELAY)

            for ca in coauthors:
                ca_pi_id = _process_coauthor(ca, pi_id, pi_name, hop)
                if ca_pi_id is not None:
                    discovered_total += 1
                    if ca_pi_id not in explored_ids:
                        next_frontier.add(ca_pi_id)

        explored_ids |= next_frontier
        frontier_ids = next_frontier

    # Cross-connection detection: coauthors shared by multiple seeds
    cross_connections = _detect_cross_connections(seed_pis)

    summary = {
        "seed_pis": len(seed_pis),
        "discovered": discovered_total,
        "cross_connections": cross_connections,
    }
    logger.info("Coauthor network complete: %s", summary)
    return summary


def _detect_cross_connections(seed_pis: list[dict]) -> int:
    """Find PIs that are coauthors of more than one seed PI.

    Updates the ``connected_seeds`` field for those PIs and returns the count.
    """
    seed_ids = {pi["id"] for pi in seed_pis}
    cross_count = 0

    with db.get_connection() as conn:
        # For each non-seed PI, count how many distinct seed PIs they
        # share a coauthorship with.
        rows = conn.execute(
            """
            SELECT pi_id,
                   GROUP_CONCAT(seed_id) AS seed_ids,
                   COUNT(DISTINCT seed_id) AS seed_count
            FROM (
                SELECT pi_id_2 AS pi_id, pi_id_1 AS seed_id
                FROM coauthorships
                WHERE pi_id_1 IN ({seeds})
                UNION
                SELECT pi_id_1 AS pi_id, pi_id_2 AS seed_id
                FROM coauthorships
                WHERE pi_id_2 IN ({seeds})
            )
            WHERE pi_id NOT IN ({seeds})
            GROUP BY pi_id
            HAVING seed_count > 1
            """.format(
                seeds=",".join(str(s) for s in seed_ids)
            )
        ).fetchall()

        for row in rows:
            cross_count += 1
            logger.info(
                "Cross-connection: PI %d linked to seeds %s",
                row["pi_id"],
                row["seed_ids"],
            )

    return cross_count
