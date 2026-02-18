"""Topic-based PI discovery via PubMed E-utilities.

Searches PubMed for recent papers matching CV keywords, extracts
corresponding authors, and adds new PIs to the database.

Uses the NCBI E-utilities REST API directly (no MCP tools):
  https://eutils.ncbi.nlm.nih.gov/entrez/eutils/

Supports fuzzy/semantic matching of keywords: PubMed queries use MeSH
expansion and OR-group synonyms, while local relevance re-ranking uses
Levenshtein-ratio fuzzy matching on titles/abstracts.
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from typing import Optional
from urllib.parse import urlencode

import requests

from src import db
from src.config import CV_KEYWORDS

logger = logging.getLogger(__name__)

_BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
_ESEARCH_URL = f"{_BASE_URL}/esearch.fcgi"
_EFETCH_URL = f"{_BASE_URL}/efetch.fcgi"

# NCBI asks for max 3 requests/second without an API key, 10 with one.
_NCBI_DELAY = 0.4  # seconds between requests (safe for unauthenticated)

# How far back to search
_LOOKBACK_YEARS = 2
_MAX_RESULTS_PER_QUERY = 100

# Fuzzy match threshold: 0.0-1.0, higher = stricter
_FUZZY_THRESHOLD = 0.65

# Synonym mapping: keyword -> list of synonyms/related terms for PubMed OR queries.
# This allows broader discovery while the fuzzy re-ranking keeps precision.
_KEYWORD_SYNONYMS: dict[str, list[str]] = {
    "synthetic biology": ["synthetic genomics", "genetic circuit", "bioengineering"],
    "CRISPR": ["CRISPR-Cas", "Cas9", "Cas12", "Cas13", "gene editing", "genome editing"],
    "Cas9": ["SpCas9", "CRISPR-Cas9"],
    "Cas12": ["Cas12a", "Cpf1", "CRISPR-Cas12"],
    "protein engineering": ["rational protein design", "computational protein design"],
    "directed evolution": ["adaptive laboratory evolution", "phage display", "error-prone PCR"],
    "extremophile": ["thermophile", "halophile", "psychrophile", "acidophile", "extremozyme"],
    "thermophile": ["thermostable enzyme", "thermus", "hyperthermophile"],
    "metabolic engineering": ["pathway engineering", "flux balance", "metabolic flux"],
    "genome engineering": ["genome editing", "chromosomal engineering"],
    "gene editing": ["genome editing", "base editing", "prime editing"],
    "cell-free": ["cell-free protein synthesis", "CFPS", "TX-TL", "in vitro transcription"],
    "high-throughput screening": ["HTS", "combinatorial screening", "droplet microfluidics"],
    "enzyme engineering": ["biocatalysis", "enzyme evolution", "enzyme design"],
}


def _fuzzy_keyword_match(text: str, keywords: list[str], threshold: float = _FUZZY_THRESHOLD) -> float:
    """Score how well *text* matches any of the *keywords* using fuzzy matching.

    Returns a value in [0.0, 1.0]: the best fuzzy match ratio found.
    Uses ``SequenceMatcher`` (stdlib) for Levenshtein-like ratio without
    requiring external dependencies.
    """
    if not text:
        return 0.0
    text_lower = text.lower()

    best_score = 0.0
    for kw in keywords:
        kw_lower = kw.lower()

        # Exact substring match -> 1.0
        if kw_lower in text_lower:
            return 1.0

        # Fuzzy match: slide a window of keyword length over the text
        kw_len = len(kw_lower)
        for i in range(max(1, len(text_lower) - kw_len + 1)):
            window = text_lower[i : i + kw_len + 5]  # slight overshoot for partial matches
            ratio = SequenceMatcher(None, kw_lower, window).ratio()
            if ratio > best_score:
                best_score = ratio

    return best_score


def _build_expanded_query(keyword: str) -> str:
    """Build a PubMed query with synonym expansion for broader recall.

    Combines the original keyword with synonyms using OR, wrapped in
    a single Title/Abstract field search.
    """
    terms = [keyword]
    synonyms = _KEYWORD_SYNONYMS.get(keyword, [])
    terms.extend(synonyms)

    # Wrap multi-word terms in quotes
    parts = []
    for t in terms:
        if " " in t:
            parts.append(f'"{t}"[Title/Abstract]')
        else:
            parts.append(f"{t}[Title/Abstract]")

    return "(" + " OR ".join(parts) + ")"


# ---------------------------------------------------------------------------
# PubMed HTTP helpers
# ---------------------------------------------------------------------------

def _esearch(query: str, max_results: int = _MAX_RESULTS_PER_QUERY) -> list[str]:
    """Run an ESearch and return a list of PubMed IDs (PMIDs)."""
    params = {
        "db": "pubmed",
        "term": query,
        "retmax": max_results,
        "retmode": "json",
        "sort": "date",
    }
    try:
        resp = requests.get(_ESEARCH_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        pmids = data.get("esearchresult", {}).get("idlist", [])
        logger.debug("ESearch '%s': %d results", query[:60], len(pmids))
        return pmids
    except Exception:
        logger.exception("ESearch failed for query: %s", query[:60])
        return []


def _efetch_articles(pmids: list[str]) -> list[dict]:
    """Fetch article metadata for a batch of PMIDs.

    Returns a list of dicts with keys: pmid, title, abstract, authors,
    corresponding_author, journal, year, affiliation.
    """
    if not pmids:
        return []

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
        "rettype": "abstract",
    }
    try:
        resp = requests.get(_EFETCH_URL, params=params, timeout=60)
        resp.raise_for_status()
    except Exception:
        logger.exception("EFetch failed for %d PMIDs", len(pmids))
        return []

    return _parse_pubmed_xml(resp.text)


def _parse_pubmed_xml(xml_text: str) -> list[dict]:
    """Parse PubMed XML into structured article dicts."""
    articles: list[dict] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        logger.error("Failed to parse PubMed XML response")
        return []

    for article_el in root.findall(".//PubmedArticle"):
        try:
            medline = article_el.find("MedlineCitation")
            if medline is None:
                continue

            pmid_el = medline.find("PMID")
            pmid = pmid_el.text if pmid_el is not None else ""

            art = medline.find("Article")
            if art is None:
                continue

            # Title
            title_el = art.find("ArticleTitle")
            title = title_el.text or "" if title_el is not None else ""

            # Abstract
            abstract_parts: list[str] = []
            abstract_el = art.find("Abstract")
            if abstract_el is not None:
                for at in abstract_el.findall("AbstractText"):
                    if at.text:
                        abstract_parts.append(at.text)
            abstract = " ".join(abstract_parts)

            # Year
            year = None
            date_el = art.find(".//PubDate/Year")
            if date_el is not None and date_el.text:
                try:
                    year = int(date_el.text)
                except ValueError:
                    pass
            if year is None:
                medline_date = art.find(".//PubDate/MedlineDate")
                if medline_date is not None and medline_date.text:
                    match = re.search(r"(\d{4})", medline_date.text)
                    if match:
                        year = int(match.group(1))

            # Journal
            journal_el = art.find(".//Journal/Title")
            journal = journal_el.text or "" if journal_el is not None else ""

            # Authors
            authors: list[dict] = []
            author_list = art.find("AuthorList")
            if author_list is not None:
                for author_el in author_list.findall("Author"):
                    last = author_el.find("LastName")
                    fore = author_el.find("ForeName")
                    if last is not None and last.text:
                        name = last.text
                        if fore is not None and fore.text:
                            name = f"{fore.text} {last.text}"

                        affiliation = ""
                        aff_el = author_el.find(".//AffiliationInfo/Affiliation")
                        if aff_el is not None and aff_el.text:
                            affiliation = aff_el.text

                        authors.append({"name": name, "affiliation": affiliation})

            # Corresponding author: typically the last author
            corresponding = authors[-1] if authors else None

            articles.append(
                {
                    "pmid": pmid,
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "corresponding_author": corresponding,
                    "journal": journal,
                    "year": year,
                }
            )
        except Exception:
            logger.exception("Error parsing a PubmedArticle element")
            continue

    return articles


def _extract_institute(affiliation: str) -> str:
    """Best-effort extraction of institute name from an affiliation string."""
    if not affiliation:
        return ""
    # Common patterns: "Department of X, University of Y, City, Country"
    # Try to grab the university/institute part
    parts = [p.strip() for p in affiliation.split(",")]
    for part in parts:
        lower = part.lower()
        if any(
            kw in lower
            for kw in ["university", "institute", "college", "school", "hospital", "center", "centre", "lab"]
        ):
            return part
    # Fallback: return the full affiliation truncated
    return affiliation[:120]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def discover_by_topic() -> dict:
    """Search PubMed for recent papers matching CV keywords, extract
    corresponding authors, and add new PIs to the database.

    Uses synonym-expanded PubMed queries for broader recall and fuzzy
    matching for relevance re-ranking of discovered articles.

    Returns
    -------
    dict
        Summary: ``{"queries_run": int, "papers_found": int,
        "new_pis": int, "skipped_known": int, "fuzzy_filtered": int}``.
    """
    cutoff = datetime.now() - timedelta(days=_LOOKBACK_YEARS * 365)
    date_filter = f'{cutoff.strftime("%Y/%m/%d")}[PDAT] : "3000"[PDAT]'

    total_papers = 0
    new_pis = 0
    skipped_known = 0
    fuzzy_filtered = 0
    queries_run = 0

    # Get existing PI names for deduplication
    existing_pis: set[str] = set()
    all_pis = db.get_all_pis()
    for pi in all_pis:
        existing_pis.add(pi["name"].lower().strip())

    # Build synonym-expanded search queries
    queries: list[tuple[str, str]] = []  # (query_string, original_keyword)
    for kw in CV_KEYWORDS:
        expanded = _build_expanded_query(kw)
        q = f"{expanded} AND {date_filter}"
        queries.append((q, kw))

    logger.info(
        "Running %d PubMed topic queries (with synonym expansion)",
        len(queries),
    )

    # All CV keywords + synonyms for fuzzy re-ranking
    all_match_terms = list(CV_KEYWORDS)
    for syns in _KEYWORD_SYNONYMS.values():
        all_match_terms.extend(syns)
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_terms: list[str] = []
    for t in all_match_terms:
        t_lower = t.lower()
        if t_lower not in seen:
            seen.add(t_lower)
            unique_terms.append(t)
    all_match_terms = unique_terms

    for query, original_kw in queries:
        queries_run += 1
        time.sleep(_NCBI_DELAY)

        pmids = _esearch(query)
        if not pmids:
            continue

        time.sleep(_NCBI_DELAY)

        # Fetch in batches of 50
        for batch_start in range(0, len(pmids), 50):
            batch = pmids[batch_start : batch_start + 50]
            articles = _efetch_articles(batch)
            time.sleep(_NCBI_DELAY)

            total_papers += len(articles)

            for article in articles:
                # Fuzzy relevance check on title + abstract
                title = article.get("title", "")
                abstract = article.get("abstract", "")
                combined_text = f"{title} {abstract}"
                relevance = _fuzzy_keyword_match(combined_text, all_match_terms)
                if relevance < _FUZZY_THRESHOLD:
                    fuzzy_filtered += 1
                    continue

                ca = article.get("corresponding_author")
                if ca is None:
                    continue

                ca_name = ca.get("name", "").strip()
                if not ca_name:
                    continue

                # Skip if already known
                if ca_name.lower() in existing_pis:
                    skipped_known += 1
                    continue

                institute = _extract_institute(ca.get("affiliation", ""))

                pi_record: dict = {
                    "name": ca_name,
                    "institute": institute,
                    "is_recommended": 1,
                }
                if article.get("year"):
                    pi_record["fields"] = original_kw

                pi_id, is_new = db.upsert_pi(pi_record)
                if is_new:
                    new_pis += 1
                    existing_pis.add(ca_name.lower())
                    logger.info(
                        "New PI from PubMed: %s (%s) via '%s' (relevance=%.2f)",
                        ca_name,
                        institute[:50],
                        original_kw,
                        relevance,
                    )

    summary = {
        "queries_run": queries_run,
        "papers_found": total_papers,
        "new_pis": new_pis,
        "skipped_known": skipped_known,
        "fuzzy_filtered": fuzzy_filtered,
    }
    logger.info("Topic discovery complete: %s", summary)
    return summary
