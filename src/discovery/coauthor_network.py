"""Co-author network explorer (Semantic Scholar only).

Builds a multi-hop co-authorship graph starting from seed PIs, identifies
cross-connections, and adds field-relevant, PI-level researchers to the
database as recommendations.

Uses BFS with concurrent.futures for parallel coauthor fetching while
respecting Semantic Scholar rate limits (100 req / 5 min).

Filtering strategy (applied per coauthor candidate):
1. Field relevance — paper titles/abstracts must match CV_KEYWORDS
2. PI-level check — h-index >= 10, total papers >= 15, recent papers >= 3
3. Institution tier (optional boost) — Tier 1-3 institutions prioritised
"""

import logging
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

from semanticscholar import SemanticScholar

from src import db
from src.config import CV_KEYWORDS, SEMANTIC_SCHOLAR_API_KEY, load_rankings

logger = logging.getLogger(__name__)

_S2_DELAY = 3.1  # ~100 req / 5 min free tier
_RECENT_YEARS = 5
_MAX_WORKERS = 3  # concurrent S2 fetches (rate-limit safe: 3 * 3.1s ~ 1 req/s)

# PI quality thresholds — filters out students, postdocs, and inactive researchers
_MIN_H_INDEX = 10
_MIN_RECENT_PAPERS = 3
_MIN_TOTAL_PAPERS = 15

_s2_client: Optional[SemanticScholar] = None
_s2_lock = threading.Lock()

# Institution rankings cache
_rankings: Optional[dict] = None


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


def _get_rankings() -> dict:
    global _rankings
    if _rankings is None:
        _rankings = load_rankings()
    return _rankings


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


def _get_institution_tier(affiliations: list[str]) -> Optional[int]:
    """Return the best institution tier (1-4) from affiliations, or None.

    Also checks companies section (top_companies → 2, companies → 3).
    """
    rankings = _get_rankings()
    if not rankings or not affiliations:
        return None

    best: Optional[int] = None
    tiers = rankings.get("tiers", {})
    companies = rankings.get("companies", {})

    for aff in affiliations:
        aff_lower = aff.lower()
        for tier_str, tier_data in tiers.items():
            try:
                tier_num = int(tier_str)
            except (ValueError, TypeError):
                continue
            for inst in tier_data.get("institutions", []):
                if inst.lower() in aff_lower or aff_lower in inst.lower():
                    if best is None or tier_num < best:
                        best = tier_num
        # Check companies
        for inst in companies.get("top_companies", []):
            if inst.lower() in aff_lower or aff_lower in inst.lower():
                if best is None or 2 < best:
                    best = 2
        for inst in companies.get("companies", []):
            if inst.lower() in aff_lower or aff_lower in inst.lower():
                if best is None or 3 < best:
                    best = 3

    return best


# ---------------------------------------------------------------------------
# Semantic Scholar: fetch + validate coauthors
# ---------------------------------------------------------------------------

def _fetch_coauthors_s2(semantic_id: str) -> list[dict]:
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


def _is_pi_level(semantic_id: str) -> tuple[bool, dict]:
    """Check if an author qualifies as PI-level via Semantic Scholar metrics.

    Returns (passes_threshold, metadata_dict).
    The metadata_dict contains h_index, citations, paper_count, affiliations,
    and recent_paper_count for use in the PI record.
    """
    s2 = _get_s2_client()
    metadata: dict = {}
    try:
        author = s2.get_author(
            semantic_id,
            fields=[
                "authorId",
                "name",
                "hIndex",
                "citationCount",
                "paperCount",
                "affiliations",
                "papers",
                "papers.year",
            ],
        )
        time.sleep(_S2_DELAY)
    except Exception:
        logger.debug("S2 error checking PI level for %s", semantic_id)
        return False, metadata

    if author is None:
        return False, metadata

    h_index = author.hIndex or 0
    paper_count = author.paperCount or 0
    citations = author.citationCount or 0
    affiliations = author.affiliations or []

    cutoff_year = datetime.now().year - _RECENT_YEARS
    recent_papers = sum(
        1 for p in (author.papers or [])
        if p.year and p.year >= cutoff_year
    )

    metadata = {
        "h_index": h_index,
        "citations": citations,
        "paper_count": paper_count,
        "affiliations": affiliations,
        "recent_paper_count": recent_papers,
    }

    passes = (
        h_index >= _MIN_H_INDEX
        and paper_count >= _MIN_TOTAL_PAPERS
        and recent_papers >= _MIN_RECENT_PAPERS
    )

    if not passes:
        logger.debug(
            "Filtered out %s (h=%d, papers=%d, recent=%d)",
            author.name, h_index, paper_count, recent_papers,
        )

    return passes, metadata


# ---------------------------------------------------------------------------
# Core network builder
# ---------------------------------------------------------------------------

def _process_coauthor(
    coauthor: dict,
    source_pi_id: int,
    source_pi_name: str,
    hop: int,
) -> Optional[int]:
    """Evaluate a single coauthor and, if relevant + PI-level, add to DB.

    Returns the PI id of the coauthor if stored, else ``None``.
    """
    name = coauthor.get("name", "").strip()
    semantic_id = coauthor.get("semantic_id", "")
    if not name or not semantic_id:
        return None

    # 1) Field relevance check
    texts = coauthor.get("paper_titles", []) + coauthor.get("paper_abstracts", [])
    if not _is_field_relevant(texts):
        logger.debug("Skipping %s (not field-relevant)", name)
        return None

    # 2) PI-level check via S2 metrics
    passes, metadata = _is_pi_level(semantic_id)
    if not passes:
        return None

    # 3) Build PI record with enriched data
    institute = ""
    tier = None
    if metadata.get("affiliations"):
        institute = metadata["affiliations"][0]
        tier = _get_institution_tier(metadata["affiliations"])

    pi_record: dict = {
        "name": name,
        "institute": institute,
        "semantic_id": semantic_id,
        "s2_author_id": semantic_id,  # also store in s2_author_id for enrichment
        "scholar_url": f"https://www.semanticscholar.org/author/{semantic_id}",
        "is_recommended": 1,
        "connected_seeds": source_pi_name,
        "h_index": metadata.get("h_index", 0),
        "citations": metadata.get("citations", 0),
    }

    pi_id, is_new = db.upsert_pi(pi_record)
    if is_new:
        tier_label = f" [Tier {tier}]" if tier else ""
        logger.info(
            "New PI (hop %d): %s — h=%d, papers=%d, recent=%d%s (via %s)",
            hop,
            name,
            metadata.get("h_index", 0),
            metadata.get("paper_count", 0),
            metadata.get("recent_paper_count", 0),
            tier_label,
            source_pi_name,
        )
    else:
        # Append connected seed if not already listed; backfill s2_author_id
        with db.get_connection() as conn:
            row = conn.execute(
                "SELECT connected_seeds, s2_author_id, scholar_url FROM pis WHERE id = ?",
                (pi_id,),
            ).fetchone()
            existing = row["connected_seeds"] or "" if row else ""
            updates: list[str] = ["is_recommended = 1", "updated_at = datetime('now')"]
            params: list = []
            if source_pi_name not in existing:
                updated_seeds = f"{existing}, {source_pi_name}" if existing else source_pi_name
                updates.append("connected_seeds = ?")
                params.append(updated_seeds)
            if row and not row["s2_author_id"] and semantic_id:
                updates.append("s2_author_id = ?")
                params.append(semantic_id)
            if row and not row["scholar_url"] and semantic_id:
                updates.append("scholar_url = ?")
                params.append(f"https://www.semanticscholar.org/author/{semantic_id}")
            params.append(pi_id)
            conn.execute(
                f"UPDATE pis SET {', '.join(updates)} WHERE id = ?",
                params,
            )

    # Record coauthorship
    shared = len(coauthor.get("paper_titles", []))
    db.add_coauthorship(source_pi_id, pi_id, shared_papers=shared)

    return pi_id


def _fetch_coauthors_for_pi(pi_id: int) -> tuple[int, str, list[dict]]:
    """Fetch coauthors for a single PI via Semantic Scholar.

    Returns ``(pi_id, pi_name, coauthors)``.
    Skips PIs without a semantic_id.
    """
    with db.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM pis WHERE id = ?", (pi_id,)
        ).fetchone()
    if row is None:
        return pi_id, "", []

    pi = dict(row)
    pi_name = pi["name"]

    if not pi.get("semantic_id"):
        logger.debug("Skipping %s (no semantic_id)", pi_name)
        return pi_id, pi_name, []

    coauthors = _fetch_coauthors_s2(pi["semantic_id"])
    return pi_id, pi_name, coauthors


def build_coauthor_network(max_hops: int = 2) -> dict:
    """Build the full coauthor network from seed PIs using BFS with parallelism.

    Uses Semantic Scholar only. Each candidate coauthor is verified for:
    - Field relevance (CV_KEYWORDS match)
    - PI-level metrics (h-index, paper count, recent activity)

    Parameters
    ----------
    max_hops : int
        Number of hops to explore (1 = direct coauthors only,
        2 = coauthors of coauthors).

    Returns
    -------
    dict
        Summary statistics.
    """
    seed_pis = db.get_seed_pis()
    if not seed_pis:
        logger.info("No seed PIs in database.")
        return {"seed_pis": 0, "discovered": 0, "filtered": 0, "cross_connections": 0}

    logger.info(
        "Building coauthor network from %d seed PIs (max_hops=%d, "
        "min_h=%d, min_papers=%d, min_recent=%d)",
        len(seed_pis), max_hops, _MIN_H_INDEX, _MIN_TOTAL_PAPERS, _MIN_RECENT_PAPERS,
    )

    discovered_total = 0
    filtered_total = 0

    # BFS: queue entries are (pi_id, current_hop)
    explored_ids: set[int] = {pi["id"] for pi in seed_pis}
    bfs_queue: deque[tuple[int, int]] = deque()
    for pi in seed_pis:
        bfs_queue.append((pi["id"], 0))

    # Process hops level by level for controlled parallelism
    for hop in range(1, max_hops + 1):
        frontier_ids: list[int] = []
        while bfs_queue and bfs_queue[0][1] == hop - 1:
            pi_id, _ = bfs_queue.popleft()
            frontier_ids.append(pi_id)

        if not frontier_ids:
            break

        logger.info("--- Hop %d (frontier size: %d) ---", hop, len(frontier_ids))
        next_frontier: set[int] = set()

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            futures = {
                executor.submit(_fetch_coauthors_for_pi, pi_id): pi_id
                for pi_id in frontier_ids
            }

            for future in as_completed(futures):
                try:
                    pi_id, pi_name, coauthors = future.result()
                except Exception:
                    logger.exception(
                        "Error fetching coauthors for PI %d", futures[future]
                    )
                    continue

                if not pi_name:
                    continue

                for ca in coauthors:
                    ca_pi_id = _process_coauthor(ca, pi_id, pi_name, hop)
                    if ca_pi_id is not None:
                        discovered_total += 1
                        if ca_pi_id not in explored_ids:
                            next_frontier.add(ca_pi_id)
                    else:
                        filtered_total += 1

        # Enqueue next hop
        for nf_id in next_frontier:
            bfs_queue.append((nf_id, hop))
        explored_ids |= next_frontier

    # Cross-connection detection
    cross_connections = _detect_cross_connections(seed_pis)

    summary = {
        "seed_pis": len(seed_pis),
        "discovered": discovered_total,
        "filtered": filtered_total,
        "cross_connections": cross_connections,
    }
    logger.info("Coauthor network complete: %s", summary)
    return summary


def _detect_cross_connections(seed_pis: list[dict]) -> int:
    """Find PIs that are coauthors of more than one seed PI."""
    seed_ids = {pi["id"] for pi in seed_pis}
    cross_count = 0

    with db.get_connection() as conn:
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
