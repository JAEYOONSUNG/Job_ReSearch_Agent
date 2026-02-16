"""Seed PI profile collector.

Fetches academic profiles from Google Scholar (via scholarly) and Semantic
Scholar, then builds a TF-IDF research vector for each seed PI and persists
everything back to the database.
"""

import json
import logging
import time
from typing import Optional

import numpy as np
from scholarly import scholarly
from semanticscholar import SemanticScholar
from sklearn.feature_extraction.text import TfidfVectorizer

from src import db
from src.config import SEMANTIC_SCHOLAR_API_KEY

logger = logging.getLogger(__name__)

# Rate-limit constants
_SCHOLARLY_DELAY = 1.0  # seconds between Google Scholar requests
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
# Google Scholar helpers
# ---------------------------------------------------------------------------

def _fetch_scholar_profile(name: str, institute: Optional[str] = None) -> Optional[dict]:
    """Search Google Scholar for *name* and return profile data.

    Returns a dict with keys: scholar_id, scholar_url, h_index, citations,
    fields, coauthors.  Returns ``None`` when no match is found.
    """
    try:
        query = name
        if institute:
            query = f"{name} {institute}"
        search_results = scholarly.search_author(query)
        author = next(search_results, None)
        if author is None:
            logger.warning("No Scholar profile found for %s", name)
            return None

        # Fill the full profile (costs one extra request)
        time.sleep(_SCHOLARLY_DELAY)
        author = scholarly.fill(author)

        coauthor_names: list[str] = []
        for ca in author.get("coauthors", []):
            ca_name = ca.get("name")
            if ca_name:
                coauthor_names.append(ca_name)

        return {
            "scholar_id": author.get("scholar_id", ""),
            "scholar_url": (
                f"https://scholar.google.com/citations?user={author.get('scholar_id', '')}"
                if author.get("scholar_id")
                else ""
            ),
            "h_index": author.get("hindex"),
            "citations": author.get("citedby"),
            "fields": author.get("interests", []),
            "coauthors": coauthor_names,
        }
    except StopIteration:
        logger.warning("No Scholar results for %s", name)
        return None
    except Exception:
        logger.exception("Error fetching Scholar profile for %s", name)
        return None


# ---------------------------------------------------------------------------
# Semantic Scholar helpers
# ---------------------------------------------------------------------------

def _fetch_semantic_profile(name: str) -> Optional[dict]:
    """Search Semantic Scholar for *name* and return profile data.

    Returns a dict with keys: semantic_id, papers (list of dicts with
    paperId, title, abstract, year, authors).
    """
    try:
        s2 = _get_s2_client()
        results = s2.search_author(name, limit=5)
        if not results:
            logger.warning("No Semantic Scholar profile found for %s", name)
            return None

        # Pick the first result (highest relevance)
        best = results[0]
        time.sleep(_S2_DELAY)

        author = s2.get_author(
            best.authorId,
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
            "papers": papers,
        }
    except Exception:
        logger.exception("Error fetching Semantic Scholar profile for %s", name)
        return None


# ---------------------------------------------------------------------------
# Research vector (TF-IDF)
# ---------------------------------------------------------------------------

def build_research_vectors(pi_papers: dict[int, list[str]]) -> dict[int, list[float]]:
    """Build TF-IDF research vectors for a set of PIs.

    Parameters
    ----------
    pi_papers : dict
        Mapping of ``pi_id`` to a list of paper abstract strings.

    Returns
    -------
    dict
        Mapping of ``pi_id`` to a list-of-float vector.
    """
    if not pi_papers:
        return {}

    pi_ids = list(pi_papers.keys())
    corpus = [" ".join(abstracts) for abstracts in pi_papers.values()]

    # Skip PIs with no text
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
    """Collect profile data for a single PI and update the database.

    Returns the merged profile dict (may be partial if some APIs fail).
    """
    logger.info("Profiling PI: %s (%s)", name, institute or "unknown institute")

    profile: dict = {}

    # --- Google Scholar ---
    scholar_data = _fetch_scholar_profile(name, institute)
    time.sleep(_SCHOLARLY_DELAY)
    if scholar_data:
        profile.update(
            {
                "scholar_id": scholar_data["scholar_id"],
                "scholar_url": scholar_data["scholar_url"],
                "h_index": scholar_data["h_index"],
                "citations": scholar_data["citations"],
                "fields": json.dumps(scholar_data["fields"]),
            }
        )

    # --- Semantic Scholar ---
    sem_data = _fetch_semantic_profile(name)
    time.sleep(_S2_DELAY)
    if sem_data:
        profile["semantic_id"] = sem_data["semantic_id"]

        # Build a research vector from abstracts
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
        pi_record["last_scraped"] = "datetime('now')"
        pi_id, is_new = db.upsert_pi(pi_record)
        logger.info(
            "PI %s (id=%d) %s",
            name,
            pi_id,
            "created" if is_new else "updated",
        )
    else:
        logger.warning("No profile data collected for %s", name)

    return profile


def profile_seed_pis() -> None:
    """Profile every seed PI in the database.

    Fetches Google Scholar and Semantic Scholar data, builds research
    vectors, and persists updates.
    """
    seed_pis = db.get_seed_pis()
    if not seed_pis:
        logger.info("No seed PIs found in database.")
        return

    logger.info("Profiling %d seed PIs", len(seed_pis))

    # Collect abstracts keyed by PI id for batch TF-IDF
    all_abstracts: dict[int, list[str]] = {}

    for pi in seed_pis:
        name = pi["name"]
        institute = pi.get("institute")
        pi_id = pi["id"]

        logger.info("Profiling seed PI: %s (id=%d)", name, pi_id)

        # --- Google Scholar ---
        scholar_data = _fetch_scholar_profile(name, institute)
        time.sleep(_SCHOLARLY_DELAY)
        update: dict = {}
        if scholar_data:
            update.update(
                {
                    "scholar_id": scholar_data["scholar_id"],
                    "scholar_url": scholar_data["scholar_url"],
                    "h_index": scholar_data["h_index"],
                    "citations": scholar_data["citations"],
                    "fields": json.dumps(scholar_data["fields"]),
                }
            )

        # --- Semantic Scholar ---
        sem_data = _fetch_semantic_profile(name)
        time.sleep(_S2_DELAY)
        if sem_data:
            update["semantic_id"] = sem_data["semantic_id"]
            abstracts = [
                p["abstract"] for p in sem_data["papers"] if p.get("abstract")
            ]
            if abstracts:
                all_abstracts[pi_id] = abstracts

        if update:
            update["name"] = name
            update["institute"] = institute
            db.upsert_pi(update)
            logger.info("Updated seed PI %s with profile data", name)

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
