"""Seed PI profile collector (Semantic Scholar only).

Fetches academic profiles from Semantic Scholar, builds a TF-IDF research
vector for each seed PI, and persists everything back to the database.
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional

import numpy as np
from semanticscholar import SemanticScholar
from sklearn.feature_extraction.text import TfidfVectorizer

from src import db
from src.config import SEMANTIC_SCHOLAR_API_KEY

logger = logging.getLogger(__name__)

_S2_DELAY = 3.1  # ~100 requests per 5 min for free tier

# ---------------------------------------------------------------------------
# Known Semantic Scholar author IDs for famous PIs with common names.
# The S2 search API (limit=20) often fails to return these researchers
# because their profiles are buried under namesakes with empty affiliations.
# These IDs were verified via DOI→paper→author lookups on S2.
# ---------------------------------------------------------------------------
KNOWN_S2_IDS: dict[str, str] = {
    "George Church": "145892667",      # h=173, Harvard genetics/genomics
    "David Baker": "2241617405",       # h=153, UW protein design, Nobel 2024
    "Feng Zhang": "145126988",         # h=95, MIT/Broad CRISPR
    "James Collins": "2231125920",     # h=124, MIT synthetic biology
    "Frances Arnold": "2795724",       # h=116, Caltech directed evolution, Nobel 2018
    "Christa Schleper": "6940827",     # h=74, Vienna archaea/extremophiles
    "William Whitman": "2073798315",   # Prokaryotes/archaea taxonomy
    "Ahmed Badran": "144777372",       # h=24, Scripps directed evolution
    "David Liu": "2949942",            # h=100, Harvard base editing/prime editing
}

_s2_client: Optional[SemanticScholar] = None


def _get_s2_client() -> SemanticScholar:
    """Lazily initialise the Semantic Scholar client."""
    global _s2_client
    if _s2_client is None:
        kwargs = {}
        if SEMANTIC_SCHOLAR_API_KEY:
            kwargs["api_key"] = SEMANTIC_SCHOLAR_API_KEY
        _s2_client = SemanticScholar(**kwargs)
    return _s2_client


# ---------------------------------------------------------------------------
# Semantic Scholar helpers
# ---------------------------------------------------------------------------

def _disambiguate_author(
    name: str,
    institute: Optional[str] = None,
) -> Optional[str]:
    """Find the correct Semantic Scholar author ID for *name*.

    First checks KNOWN_S2_IDS for pre-verified famous researchers.
    Otherwise uses the REST API with limit=20, ranks candidates by:
    1. Affiliation match (if institute given)
    2. h-index (highest wins — real PIs have much higher h-index than namesakes)

    Returns the best-matching authorId, or None.
    """
    import requests as _req

    # Check known IDs first (handles famous PIs with common names)
    known_id = KNOWN_S2_IDS.get(name)
    if known_id:
        logger.info("Using known S2 ID for %s: %s", name, known_id)
        return known_id

    try:
        params = {
            "query": name,
            "fields": "authorId,name,hIndex,citationCount,paperCount,affiliations",
            "limit": 20,
        }
        headers = {}
        if SEMANTIC_SCHOLAR_API_KEY:
            headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

        resp = _req.get(
            "https://api.semanticscholar.org/graph/v1/author/search",
            params=params,
            headers=headers,
            timeout=15,
        )
        time.sleep(_S2_DELAY)

        if resp.status_code == 429:
            logger.debug("S2 rate limited searching for %s", name)
            return None
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            logger.warning("No Semantic Scholar results for %s", name)
            return None

        # Score each candidate
        def _score(candidate: dict) -> tuple[int, int, int]:
            h = candidate.get("hIndex") or 0
            papers = candidate.get("paperCount") or 0
            aff_match = 0
            if institute:
                affs = candidate.get("affiliations") or []
                inst_lower = institute.lower()
                for a in affs:
                    if inst_lower in a.lower() or a.lower() in inst_lower:
                        aff_match = 1000  # strong boost for affiliation match
                        break
            return (aff_match, h, papers)

        best = max(data, key=_score)
        best_score = _score(best)
        logger.info(
            "S2 disambiguated %s → %s (h=%d, papers=%d, aff_match=%s)",
            name, best.get("name"), best.get("hIndex", 0),
            best.get("paperCount", 0), best_score[0] > 0,
        )
        return best.get("authorId")

    except Exception:
        logger.exception("Error disambiguating S2 author for %s", name)
        return None


def _fetch_semantic_profile(
    name: str,
    institute: Optional[str] = None,
    known_s2_id: Optional[str] = None,
) -> Optional[dict]:
    """Fetch Semantic Scholar profile data.

    If *known_s2_id* is provided, fetches directly.
    Otherwise uses _disambiguate_author() to find the best match
    via REST API (ranked by h-index + affiliation).

    Returns a dict with keys: semantic_id, h_index, citations, affiliations,
    papers (list of dicts with paperId, title, abstract, year, authors).
    """
    try:
        s2 = _get_s2_client()

        if known_s2_id:
            author_id = known_s2_id
        else:
            author_id = _disambiguate_author(name, institute)
            if not author_id:
                return None

        time.sleep(_S2_DELAY)

        author = s2.get_author(
            author_id,
            fields=[
                "authorId",
                "name",
                "hIndex",
                "citationCount",
                "paperCount",
                "affiliations",
                "homepage",
                "papers",
                "papers.paperId",
                "papers.title",
                "papers.abstract",
                "papers.year",
                "papers.authors",
            ],
        )
        if author is None:
            return None

        papers: list[dict] = []
        for p in author.papers or []:
            papers.append(
                {
                    "paperId": p.paperId,
                    "title": p.title or "",
                    "abstract": p.abstract or "",
                    "year": p.year,
                    "authors": [a.name for a in (p.authors or []) if a.name],
                }
            )

        return {
            "semantic_id": author.authorId,
            "h_index": author.hIndex,
            "citations": author.citationCount,
            "affiliations": author.affiliations or [],
            "homepage": getattr(author, "homepage", None),
            "papers": papers,
        }
    except Exception:
        logger.exception("Error fetching Semantic Scholar profile for %s", name)
        return None


# ---------------------------------------------------------------------------
# Semantic Scholar metadata (lightweight, REST API)
# ---------------------------------------------------------------------------

def fetch_semantic_scholar_metadata(
    name: str, institute: Optional[str] = None,
) -> Optional[dict]:
    """Fetch lightweight metadata from Semantic Scholar for *name*.

    Uses the same disambiguation logic as _disambiguate_author:
    ranks candidates by affiliation match + h-index to pick the real person.

    Returns a dict with keys: ``h_index``, ``citations``, ``homepage``,
    ``s2_url``, ``full_name``, ``authorId``.
    """
    import requests as _req

    is_single = " " not in name.strip()

    if is_single and not institute:
        logger.debug("Skipping S2 metadata for single-name without institute: %s", name)
        return None

    try:
        params = {
            "query": name,
            "fields": "authorId,name,hIndex,citationCount,homepage,url,paperCount,affiliations",
            "limit": 20,
        }
        headers = {}
        if SEMANTIC_SCHOLAR_API_KEY:
            headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

        resp = _req.get(
            "https://api.semanticscholar.org/graph/v1/author/search",
            params=params,
            headers=headers,
            timeout=15,
        )
        time.sleep(_S2_DELAY)

        if resp.status_code == 429:
            logger.debug("S2 rate limited for %s", name)
            return None
        resp.raise_for_status()

        data = resp.json().get("data", [])
        if not data:
            logger.debug("No Semantic Scholar match for %s", name)
            return None

        if is_single:
            name_lower = name.lower()
            data = [
                r for r in data
                if name_lower in (r.get("name") or "").lower()
                and (r.get("paperCount") or 0) >= 5
            ]
            if not data:
                logger.debug("No S2 match for single-name %s (no candidate with >=5 papers)", name)
                return None

        # Rank by affiliation match + h-index (same logic as _disambiguate_author)
        def _score(r: dict) -> tuple[int, int, int]:
            h = r.get("hIndex") or 0
            papers = r.get("paperCount") or 0
            aff_match = 0
            if institute:
                affs = r.get("affiliations") or []
                inst_lower = institute.lower()
                for a in affs:
                    if inst_lower in a.lower() or a.lower() in inst_lower:
                        aff_match = 1000
                        break
            return (aff_match, h, papers)

        best = max(data, key=_score)

        return {
            "h_index": best.get("hIndex"),
            "citations": best.get("citationCount"),
            "homepage": best.get("homepage") or None,
            "s2_url": best.get("url") or f"https://www.semanticscholar.org/author/{best['authorId']}",
            "full_name": best.get("name"),
            "authorId": best.get("authorId"),
        }
    except Exception:
        logger.debug("Semantic Scholar metadata fetch failed for %s", name, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Paper fetching
# ---------------------------------------------------------------------------

def fetch_author_papers(author_id: str) -> Optional[list[dict]]:
    """Fetch papers for a Semantic Scholar author by authorId."""
    import requests as _req

    try:
        params = {
            "fields": "title,year,citationCount,url",
            "limit": 100,
        }
        headers = {}
        if SEMANTIC_SCHOLAR_API_KEY:
            headers["x-api-key"] = SEMANTIC_SCHOLAR_API_KEY

        resp = _req.get(
            f"https://api.semanticscholar.org/graph/v1/author/{author_id}/papers",
            params=params,
            headers=headers,
            timeout=15,
        )
        time.sleep(_S2_DELAY)

        if resp.status_code == 429:
            logger.debug("S2 rate limited fetching papers for author %s", author_id)
            return None
        resp.raise_for_status()

        data = resp.json().get("data", [])
        papers = []
        for p in data:
            papers.append({
                "title": p.get("title") or "",
                "year": p.get("year"),
                "citation_count": p.get("citationCount") or 0,
                "url": p.get("url") or "",
            })
        return papers
    except Exception:
        logger.debug("S2 paper fetch failed for author %s", author_id, exc_info=True)
        return None


def fetch_pi_papers(
    name: str,
    institute: Optional[str] = None,
    s2_author_id: Optional[str] = None,
) -> Optional[dict]:
    """Fetch recent and top-cited papers for a PI."""
    if not s2_author_id:
        meta = fetch_semantic_scholar_metadata(name, institute)
        if not meta or not meta.get("authorId"):
            logger.debug("Cannot resolve S2 author ID for %s", name)
            return None
        s2_author_id = meta["authorId"]

    papers = fetch_author_papers(s2_author_id)
    if not papers:
        return None

    with_year = [p for p in papers if p.get("year")]
    recent = sorted(with_year, key=lambda p: p["year"], reverse=True)[:5]
    top_cited = sorted(papers, key=lambda p: p["citation_count"], reverse=True)[:5]

    return {
        "recent_papers": recent,
        "top_cited_papers": top_cited,
        "s2_author_id": s2_author_id,
    }


# ---------------------------------------------------------------------------
# Research vector (TF-IDF)
# ---------------------------------------------------------------------------

def build_research_vectors(pi_papers: dict[int, list[str]]) -> dict[int, list[float]]:
    """Build TF-IDF research vectors for a set of PIs."""
    if not pi_papers:
        return {}

    pi_ids = list(pi_papers.keys())
    corpus = [" ".join(abstracts) for abstracts in pi_papers.values()]

    non_empty_indices = [i for i, doc in enumerate(corpus) if doc.strip()]
    if not non_empty_indices:
        return {}

    vectorizer = TfidfVectorizer(
        max_features=500,
        stop_words="english",
        min_df=1,
        max_df=0.95,
    )
    filtered_corpus = [corpus[i] for i in non_empty_indices]
    tfidf_matrix = vectorizer.fit_transform(filtered_corpus)

    vectors: dict[int, list[float]] = {}
    for idx, matrix_row in zip(non_empty_indices, range(tfidf_matrix.shape[0])):
        vec = tfidf_matrix[matrix_row].toarray().flatten().tolist()
        vectors[pi_ids[idx]] = vec

    return vectors


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def profile_single_pi(name: str, institute: Optional[str] = None) -> dict:
    """Collect profile data for a single PI and update the database."""
    logger.info("Profiling PI: %s (%s)", name, institute or "unknown institute")

    profile: dict = {}

    sem_data = _fetch_semantic_profile(name, institute)
    time.sleep(_S2_DELAY)
    if sem_data:
        profile["semantic_id"] = sem_data["semantic_id"]
        if sem_data.get("h_index") is not None:
            profile["h_index"] = sem_data["h_index"]
        if sem_data.get("citations") is not None:
            profile["citations"] = sem_data["citations"]
        if sem_data.get("homepage"):
            profile["lab_url"] = sem_data["homepage"]

        abstracts = [p["abstract"] for p in sem_data["papers"] if p.get("abstract")]
        if abstracts:
            vectors = build_research_vectors({0: abstracts})
            if 0 in vectors:
                profile["keywords"] = json.dumps(vectors[0])

    # Persist to DB
    if profile:
        pi_record = {"name": name}
        if institute:
            pi_record["institute"] = institute
        pi_record.update(profile)
        pi_record["last_scraped"] = datetime.now().isoformat()
        pi_id, is_new = db.upsert_pi(pi_record)
        logger.info(
            "PI %s (id=%d) %s — h=%s, cites=%s",
            name, pi_id,
            "created" if is_new else "updated",
            profile.get("h_index", "?"),
            profile.get("citations", "?"),
        )
    else:
        logger.warning("No profile data collected for %s", name)

    return profile


def profile_seed_pis() -> None:
    """Profile every seed PI in the database using Semantic Scholar."""
    seed_pis = db.get_seed_pis()
    if not seed_pis:
        logger.info("No seed PIs found in database.")
        return

    logger.info("Profiling %d seed PIs (Semantic Scholar only)", len(seed_pis))

    all_abstracts: dict[int, list[str]] = {}

    for pi in seed_pis:
        name = pi["name"]
        institute = pi.get("institute")
        pi_id = pi["id"]

        known_s2_id = pi.get("semantic_id") or KNOWN_S2_IDS.get(name)
        logger.info("Profiling seed PI: %s (id=%d, s2=%s)", name, pi_id, known_s2_id or "unknown")

        sem_data = _fetch_semantic_profile(name, institute, known_s2_id=known_s2_id)
        time.sleep(_S2_DELAY)

        update: dict = {}
        if sem_data:
            update["semantic_id"] = sem_data["semantic_id"]
            if sem_data.get("h_index") is not None:
                update["h_index"] = sem_data["h_index"]
            if sem_data.get("citations") is not None:
                update["citations"] = sem_data["citations"]
            if sem_data.get("homepage"):
                update["lab_url"] = sem_data["homepage"]

            abstracts = [
                p["abstract"] for p in sem_data["papers"] if p.get("abstract")
            ]
            if abstracts:
                all_abstracts[pi_id] = abstracts

        if update:
            update["name"] = name
            update["institute"] = institute
            db.upsert_pi(update)
            logger.info(
                "Updated seed PI %s — h=%s, cites=%s, semantic_id=%s",
                name,
                update.get("h_index", "?"),
                update.get("citations", "?"),
                update.get("semantic_id", "?"),
            )

    # Build research vectors for all seed PIs at once
    if all_abstracts:
        logger.info("Building research vectors for %d PIs", len(all_abstracts))
        vectors = build_research_vectors(all_abstracts)
        with db.get_connection() as conn:
            for pi_id, vec in vectors.items():
                conn.execute(
                    "UPDATE pis SET keywords = ?, updated_at = datetime('now') WHERE id = ?",
                    (json.dumps(vec), pi_id),
                )
        logger.info("Research vectors stored for %d PIs", len(vectors))

    logger.info("Seed PI profiling complete.")
