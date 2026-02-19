"""PI recommendation scoring engine.

Calculates a composite recommendation score for every discovered PI based on
field similarity, connection strength, institution ranking, h-index, and
recent publication activity.  Weights are read from
``config.RECOMMENDER_WEIGHTS``.
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from typing import Optional

import numpy as np
from semanticscholar import SemanticScholar

from src import db
from src.config import (
    RECOMMENDER_WEIGHTS,
    SEMANTIC_SCHOLAR_API_KEY,
    load_rankings,
)

logger = logging.getLogger(__name__)

_S2_DELAY = 3.1
_RECENT_YEARS = 2

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
# Sub-scores
# ---------------------------------------------------------------------------

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    va = np.array(a, dtype=float)
    vb = np.array(b, dtype=float)
    if va.shape != vb.shape:
        # Pad the shorter vector with zeros
        max_len = max(len(a), len(b))
        va = np.pad(va, (0, max_len - len(va)))
        vb = np.pad(vb, (0, max_len - len(vb)))
    norm_a = np.linalg.norm(va)
    norm_b = np.linalg.norm(vb)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(va, vb) / (norm_a * norm_b))


def _score_field_similarity(
    pi: dict,
    seed_vectors: list[list[float]],
) -> float:
    """Average cosine similarity between this PI's research vector and all
    seed PI vectors.  Returns 0.0 if no vector data is available.
    """
    pi_keywords = pi.get("keywords")
    if not pi_keywords:
        return 0.0
    try:
        pi_vec = json.loads(pi_keywords)
    except (json.JSONDecodeError, TypeError):
        return 0.0

    if not seed_vectors:
        return 0.0

    similarities = [_cosine_similarity(pi_vec, sv) for sv in seed_vectors]
    # Use maximum similarity (best match among seeds) rather than average
    return max(similarities)


def _score_connection_strength(pi_id: int) -> float:
    """Score based on coauthorship and citation counts.

    - Number of seed PIs connected to (via coauthorships table)
    - Number of shared papers
    - Citation overlap (from citations table)

    Normalised to [0, 1] using a sigmoid-like function.
    """
    coauthor_score = 0.0
    citation_score = 0.0

    with db.get_connection() as conn:
        # Coauthorship counts
        row = conn.execute(
            """
            SELECT COUNT(DISTINCT CASE
                       WHEN pi_id_1 = ? THEN pi_id_2 ELSE pi_id_1
                   END) AS seed_count,
                   COALESCE(SUM(shared_papers), 0) AS total_shared
            FROM coauthorships
            WHERE pi_id_1 = ? OR pi_id_2 = ?
            """,
            (pi_id, pi_id, pi_id),
        ).fetchone()
        if row:
            seed_connections = row["seed_count"]
            total_shared = row["total_shared"]
            # sigmoid: saturates around 5 seed connections
            coauthor_score = 1.0 - (1.0 / (1.0 + 0.5 * seed_connections + 0.1 * total_shared))

        # Citation counts
        cite_row = conn.execute(
            """
            SELECT COALESCE(SUM(citation_count), 0) AS total
            FROM citations
            WHERE citing_pi_id = ? OR cited_pi_id = ?
            """,
            (pi_id, pi_id),
        ).fetchone()
        if cite_row:
            total_cites = cite_row["total"]
            citation_score = 1.0 - (1.0 / (1.0 + 0.3 * total_cites))

    return 0.6 * coauthor_score + 0.4 * citation_score


def _score_institution_ranking(pi: dict, rankings: dict) -> float:
    """Score based on institution tier.

    Tier mapping from ``institution_rankings.json``:
    - Tier 1 -> 1.0
    - Tier 2 / top_companies -> 0.75
    - Tier 3 / companies -> 0.5
    - Tier 4 -> 0.3
    - Tier 5 (unknown) -> 0.15
    """
    tier = pi.get("tier")
    tier_scores = {1: 1.0, 2: 0.75, 3: 0.5, 4: 0.3, 5: 0.15}

    if tier is not None:
        return tier_scores.get(tier, 0.15)

    # Try to look up institute in rankings
    institute = pi.get("institute", "")
    if institute and rankings:
        for inst_name, info in rankings.items():
            if inst_name.lower() in institute.lower() or institute.lower() in inst_name.lower():
                tier_val = info if isinstance(info, int) else info.get("tier", 5)
                return tier_scores.get(tier_val, 0.15)

    return 0.15  # unknown


def _score_h_index(pi: dict, max_h: int) -> float:
    """Normalised h-index score in [0, 1].

    Uses min-max normalisation against the highest h-index among all PIs.
    """
    h = pi.get("h_index")
    if h is None or max_h == 0:
        return 0.0
    return min(float(h) / max_h, 1.0)


def _score_recent_activity(pi: dict) -> float:
    """Score based on number of papers in the last N years with recency decay.

    More recent papers contribute more to the score via exponential decay:
    a paper from the current year gets full weight (1.0), while a paper
    from ``_RECENT_YEARS`` ago gets weight ~0.37 (1/e).

    Fetches recent paper count from Semantic Scholar if a semantic_id is
    available; otherwise returns a low default.
    """
    semantic_id = pi.get("semantic_id")
    if not semantic_id:
        return 0.2  # unknown -- give small benefit of the doubt

    s2 = _get_s2_client()
    try:
        author = s2.get_author(
            semantic_id,
            fields=["papers", "papers.year"],
        )
        time.sleep(_S2_DELAY)
    except Exception:
        logger.debug("Could not fetch recent activity for %s", pi.get("name"))
        return 0.2

    if author is None or not author.papers:
        return 0.0

    current_year = datetime.now().year
    cutoff = current_year - _RECENT_YEARS

    # Recency-weighted count: exponential decay by age
    # decay_factor = exp(-age / _RECENT_YEARS) so current year = 1.0
    weighted_count = 0.0
    for p in author.papers:
        if p.year is not None and p.year >= cutoff:
            age = max(current_year - p.year, 0)
            weight = np.exp(-age / _RECENT_YEARS)
            weighted_count += weight

    # Sigmoid normalisation: saturates around 15 weighted papers
    return 1.0 - (1.0 / (1.0 + 0.2 * weighted_count))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def score_all_pis(dry_run: bool = False) -> list[dict]:
    """Calculate and persist recommendation scores for all non-seed PIs.

    Parameters
    ----------
    dry_run : bool
        If True, compute and return scores without writing to the database.

    Returns
    -------
    list[dict]
        List of ``{"pi_id": int, "name": str, "score": float, "breakdown": dict}``
        sorted descending by score.
    """
    all_pis = db.get_all_pis()
    seed_pis = [p for p in all_pis if p.get("is_seed")]
    candidate_pis = [p for p in all_pis if not p.get("is_seed")]

    if not candidate_pis:
        logger.info("No candidate PIs to score.")
        return []

    logger.info(
        "Scoring %d candidate PIs against %d seeds",
        len(candidate_pis),
        len(seed_pis),
    )

    # Pre-load seed research vectors
    seed_vectors: list[list[float]] = []
    for sp in seed_pis:
        kw = sp.get("keywords")
        if kw:
            try:
                seed_vectors.append(json.loads(kw))
            except (json.JSONDecodeError, TypeError):
                pass

    # Load institution rankings
    rankings = load_rankings()

    # Determine max h-index for normalisation
    all_h = [p["h_index"] for p in all_pis if p.get("h_index") is not None]
    max_h = max(all_h) if all_h else 1

    weights = RECOMMENDER_WEIGHTS
    results: list[dict] = []

    for pi in candidate_pis:
        pi_id = pi["id"]
        name = pi["name"]

        fs = _score_field_similarity(pi, seed_vectors)
        cs = _score_connection_strength(pi_id)
        ir = _score_institution_ranking(pi, rankings)
        hi = _score_h_index(pi, max_h)
        ra = _score_recent_activity(pi)

        composite = (
            weights["field_similarity"] * fs
            + weights["connection_strength"] * cs
            + weights["institution_ranking"] * ir
            + weights["h_index"] * hi
            + weights["recent_activity"] * ra
        )

        breakdown = {
            "field_similarity": round(fs, 4),
            "connection_strength": round(cs, 4),
            "institution_ranking": round(ir, 4),
            "h_index": round(hi, 4),
            "recent_activity": round(ra, 4),
        }

        results.append(
            {
                "pi_id": pi_id,
                "name": name,
                "score": round(composite, 4),
                "breakdown": breakdown,
            }
        )

        logger.debug(
            "PI %s: score=%.4f breakdown=%s", name, composite, breakdown
        )

    # Sort by score descending
    results.sort(key=lambda r: r["score"], reverse=True)

    if not dry_run:
        with db.get_connection() as conn:
            for r in results:
                conn.execute(
                    "UPDATE pis SET recommendation_score = ?, "
                    "updated_at = datetime('now') WHERE id = ?",
                    (r["score"], r["pi_id"]),
                )
        logger.info("Recommendation scores persisted for %d PIs", len(results))
    else:
        logger.info("Dry run: scores computed but NOT persisted")

    return results


def get_top_recommendations(n: int = 50) -> list[dict]:
    """Return the top *n* recommended PIs from the database.

    This reads persisted scores (call ``score_all_pis()`` first to refresh).
    """
    with db.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM pis WHERE is_recommended = 1 "
            "ORDER BY recommendation_score DESC LIMIT ?",
            (n,),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point with ``--dry-run`` flag."""
    parser = argparse.ArgumentParser(
        description="Score and rank discovered PIs"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute scores without writing to the database",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Number of top PIs to display (default: 20)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    results = score_all_pis(dry_run=args.dry_run)

    print(f"\n{'='*70}")
    print(f" Top {args.top} PI Recommendations")
    print(f"{'='*70}")
    for i, r in enumerate(results[: args.top], 1):
        bd = r["breakdown"]
        print(
            f" {i:3d}. {r['name']:<35s}  score={r['score']:.4f}  "
            f"[FS={bd['field_similarity']:.2f} CS={bd['connection_strength']:.2f} "
            f"IR={bd['institution_ranking']:.2f} HI={bd['h_index']:.2f} "
            f"RA={bd['recent_activity']:.2f}]"
        )
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
