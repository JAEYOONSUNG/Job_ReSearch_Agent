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

def _fetch_semantic_profile(
    name: str,
    institute: Optional[str] = None,
    known_s2_id: Optional[str] = None,
) -> Optional[dict]:
    """Fetch Semantic Scholar profile data.

    If *known_s2_id* is provided, skips search and fetches directly.
    Otherwise searches by name (limit=5 to avoid pagination storm).

    Returns a dict with keys: semantic_id, h_index, citations, affiliations,
    papers (list of dicts with paperId, title, abstract, year, authors).
    """
    try:
        s2 = _get_s2_client()

        if known_s2_id:
            author_id = known_s2_id
        else:
            results = s2.search_author(name, limit=5)
            if not results:
                logger.warning("No Semantic Scholar profile found for %s", name)
                return None

            # Pick the best result — prefer matching affiliation if institute given
            best = results[0]
            if institute and len(results) > 1:
                inst_lower = institute.lower()
                for r in results:
                    affs = getattr(r, "affiliations", []) or []
                    if any(inst_lower in a.lower() for a in affs):
                        best = r
                        break
            author_id = best.authorId

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

    Returns a dict with keys: ``h_index``, ``citations``, ``homepage``,
    ``s2_url``, ``full_name``, ``authorId``.
    """
    import requests as _req

    is_single = " " not in name.strip()

    if is_single and not institute:
        logger.debug("Skipping S2 metadata for single-name without institute: %s", name)
        return None

    query = name

    try:
        params = {
            "query": query,
            "fields": "authorId,name,hIndex,citationCount,homepage,url,paperCount,affiliations",
            "limit": 10 if is_single else 5,
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

        if is_single and institute:
            name_lower = name.lower()
            candidates = [
                r for r in data
                if name_lower in (r.get("name") or "").lower()
                and (r.get("paperCount") or 0) >= 5
            ]
            if not candidates:
                logger.debug("No S2 match for single-name %s (no candidate with >=5 papers)", name)
                return None
            best = max(candidates, key=lambda r: (r.get("hIndex") or 0, r.get("paperCount") or 0))
        else:
            best = max(data, key=lambda r: r.get("paperCount", 0) or 0)

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

        known_s2_id = pi.get("semantic_id")
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
