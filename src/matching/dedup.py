"""Deduplicate job listings across sources.

Uses a multi-stage approach for efficient deduplication:

1. **Fast-path exact checks** — URL, PI+institute keys (O(1) hash lookups).
2. **MinHash / LSH** — approximate nearest-neighbour candidate generation on
   normalised title + institute text.  Reduces pairwise comparisons from
   O(n^2) to O(n) amortised.
3. **SequenceMatcher verification** — applied only to LSH candidate pairs to
   confirm true duplicates with the original similarity thresholds.

The MinHash / LSH implementation is self-contained (no external dependencies)
and uses the same 32-bit hash family approach as *datasketch*.
"""

import hashlib
import logging
import re
import struct
import unicodedata
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Optional

logger = logging.getLogger(__name__)

# Normalisation patterns for postdoc position variants
_POSITION_VARIANTS = re.compile(
    r"\b(post[\s-]?doc(?:toral)?|postdoc)\b", re.IGNORECASE
)

# ── MinHash / LSH constants ───────────────────────────────────────────────
# 128 hash functions gives Jaccard estimation error ~1/sqrt(128) ≈ 0.088
_NUM_PERM = 128
# Number of LSH bands.  With 128 perms and 16 bands (8 rows each) the
# probability of a candidate pair being detected is:
#   P(candidate | Jaccard=0.5) ≈ 1 - (1 - 0.5^8)^16 ≈ 0.96
#   P(candidate | Jaccard=0.3) ≈ 1 - (1 - 0.3^8)^16 ≈ 0.0016
_NUM_BANDS = 16
_ROWS_PER_BAND = _NUM_PERM // _NUM_BANDS  # 8

# Large prime for hash family (Mersenne prime 2^31-1)
_MERSENNE_PRIME = (1 << 31) - 1
_MAX_HASH = (1 << 32) - 1

# Pre-computed random coefficients for the hash family  h(x) = (a*x + b) % p
# Seeded deterministically so results are reproducible across runs.
_A_COEFFS: list[int] = []
_B_COEFFS: list[int] = []


def _init_hash_coefficients() -> None:
    """Lazily initialise hash coefficients from a fixed seed."""
    if _A_COEFFS:
        return
    import random
    rng = random.Random(42)
    for _ in range(_NUM_PERM):
        _A_COEFFS.append(rng.randint(1, _MERSENNE_PRIME - 1))
        _B_COEFFS.append(rng.randint(0, _MERSENNE_PRIME - 1))


# ── Text normalisation (unchanged public API) ────────────────────────────


def normalize_title(title: str) -> str:
    """Normalize job title for comparison."""
    title = (title or "").lower().strip()
    title = re.sub(r"\s+", " ", title)
    # Unify postdoc variants: "Post-Doc" / "Postdoctoral" / "Post Doc" -> "postdoc"
    title = _POSITION_VARIANTS.sub("postdoc", title)
    # Remove common prefixes
    for prefix in ("postdoc", "research"):
        title = re.sub(rf"^{prefix}\s+(position|fellow|researcher|associate)\s*[-:–]?\s*", "", title)
    return title


# Names that are aggregator site names, not real institutions.
# If normalize_institute() returns one of these, treat the institute as empty.
_AGGREGATOR_INSTITUTES = {
    "academic positions", "academicpositions", "the university",
    "inside higher ed", "phdfinder", "higher ed jobs", "higheredjobs",
    "academickeys", "chronicle of higher education",
    "nature careers", "nature", "the",
    "times higher education", "researchgate", "scholarshipdb",
    "linkedin", "indeed", "glassdoor",
}

# Substrings that indicate an aggregator/portal (not a real institute)
_AGGREGATOR_SUBSTRINGS = [
    "stipendier", "stipendiemodul",
]

# Regex to strip long institutional suffixes that vary across sources
_INST_SUFFIX_RE = re.compile(
    r"\s*(?:[-–—,]\s*(?:research center|centre|center|faculty|"
    r"of the [\w\s]+academy[\w\s]*|"
    r"stipendier|stipendiemodul).*)",
    re.IGNORECASE,
)

# Common abbreviation expansions (applied after lowercasing)
_INST_ABBREVIATIONS: list[tuple[str, str]] = [
    ("university of california, ", "uc "),
    ("university of california ", "uc "),
    ("massachusetts institute of technology", "mit"),
    ("california institute of technology", "caltech"),
    ("university of north carolina at chapel hill", "unc"),
    ("university of north carolina", "unc"),
    ("unc school of medicine", "unc"),
    ("komen graduate training program ut mdacc", "md anderson"),
    ("md anderson cancer center", "md anderson"),
    ("washington university in st. louis", "washu"),
    ("georgia institute of technology", "georgia tech"),
    ("eth zürich", "eth zurich"),
]


def _strip_diacritics(text: str) -> str:
    """Remove diacritics (ü→u, é→e, å→a, etc.)."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def normalize_institute(name: str) -> str:
    """Normalize institution name for comparison.

    Handles diacritics, abbreviations, aggregator names, and long suffixes.
    """
    name = (name or "").lower().strip()
    if not name:
        return ""
    # Strip diacritics early
    name = _strip_diacritics(name)
    # Apply abbreviation expansions
    for old, new in _INST_ABBREVIATIONS:
        name = name.replace(old, new)
    # Remove long suffixes that vary across sources
    name = _INST_SUFFIX_RE.sub("", name)
    name = re.sub(r"\s+", " ", name).strip()
    # Return empty if it's an aggregator name
    if name in _AGGREGATOR_INSTITUTES:
        return ""
    # Check aggregator substrings
    for sub in _AGGREGATOR_SUBSTRINGS:
        if sub in name:
            return ""
    return name


def similarity(a: str, b: str) -> float:
    """Calculate string similarity ratio."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ── MinHash helpers ───────────────────────────────────────────────────────


def _shingle(text: str, k: int = 3) -> set[int]:
    """Convert text to a set of k-character shingle hashes."""
    if len(text) < k:
        return {hash(text) & _MAX_HASH} if text else set()
    return {hash(text[i:i + k]) & _MAX_HASH for i in range(len(text) - k + 1)}


def _minhash_signature(shingles: set[int]) -> list[int]:
    """Compute a MinHash signature (list of _NUM_PERM minimum hashes)."""
    _init_hash_coefficients()
    if not shingles:
        return [_MAX_HASH] * _NUM_PERM
    sig = [_MAX_HASH] * _NUM_PERM
    for val in shingles:
        for i in range(_NUM_PERM):
            h = (_A_COEFFS[i] * val + _B_COEFFS[i]) % _MERSENNE_PRIME
            if h < sig[i]:
                sig[i] = h
    return sig


def _lsh_buckets(signature: list[int]) -> list[bytes]:
    """Hash each band of the signature to a bucket key."""
    buckets: list[bytes] = []
    for b in range(_NUM_BANDS):
        start = b * _ROWS_PER_BAND
        band = signature[start:start + _ROWS_PER_BAND]
        # Pack band values and hash to a compact bucket key
        raw = struct.pack(f">{_ROWS_PER_BAND}I", *band)
        buckets.append(hashlib.md5(raw).digest())
    return buckets


# ── Duplicate detection (unchanged public API) ───────────────────────────


def _is_full_name(name: str) -> bool:
    """Check if a PI name looks like a full name (first + last)."""
    parts = name.strip().split()
    return len(parts) >= 2 and all(len(p) > 1 for p in parts)


def is_duplicate(job_a: dict, job_b: dict, threshold: float = 0.85) -> bool:
    """Check if two jobs are duplicates.

    Uses multi-level matching:
    1. Exact URL match
    2. Same PI name (full name) — very lenient on institute/title
    3. Same PI name (last name only) — moderate institute/title check
    4. Same institute + similar title
    5. High combined title+institute similarity
    """
    # Same URL = definite duplicate
    if job_a.get("url") and job_a.get("url") == job_b.get("url"):
        return True

    title_sim = similarity(
        normalize_title(job_a.get("title", "")),
        normalize_title(job_b.get("title", "")),
    )
    inst_a = normalize_institute(job_a.get("institute", ""))
    inst_b = normalize_institute(job_b.get("institute", ""))
    inst_sim = similarity(inst_a, inst_b) if inst_a and inst_b else 0.0

    pi_a = (job_a.get("pi_name") or "").strip()
    pi_b = (job_b.get("pi_name") or "").strip()
    both_have_pis = bool(pi_a) and bool(pi_b)
    same_pi = both_have_pis and pi_a.lower() == pi_b.lower()

    # GUARD: different PIs at the same institute → distinct jobs, never merge
    if both_have_pis and not same_pi:
        return False

    if same_pi:
        full_name = _is_full_name(pi_a)
        if full_name:
            # Full name match (e.g. "Basile Wicky") — very strong signal.
            # Only need minimal title OR institute overlap to confirm.
            if title_sim > 0.3 or inst_sim > 0.3:
                return True
            # If one institute is empty (aggregator), accept unconditionally.
            # ScholarshipDB often has garbage titles like "100%, Basel, fixed-term".
            if not inst_a or not inst_b:
                return True
        else:
            # Last-name-only match (e.g. "Bock") — need more evidence.
            # Very similar titles → almost certainly the same job
            if title_sim > 0.7:
                return True
            # Moderate title similarity + some institute overlap
            if title_sim > 0.35 and inst_sim > 0.3:
                return True
            if inst_sim > 0.5 and title_sim > 0.25:
                return True

    # Same institute + very similar normalised title
    # Safety: if either PI name is missing, require much higher title similarity
    # to avoid merging distinct positions at the same institute
    inst_title_threshold = 0.6 if both_have_pis else 0.85
    if inst_a and inst_b and inst_a == inst_b and title_sim >= inst_title_threshold:
        return True

    # High title + institute similarity (only when PI names don't conflict)
    combined = (title_sim * 0.6) + (inst_sim * 0.4)
    return combined >= threshold


def _pick_best(job_a: dict, job_b: dict) -> dict:
    """Between two duplicates, merge fields and keep the richer entry."""
    len_a = len(job_a.get("description") or "")
    len_b = len(job_b.get("description") or "")
    best = dict(job_a) if len_a >= len_b else dict(job_b)
    other = job_b if len_a >= len_b else job_a

    # Merge: fill empty fields from the other source
    for key in ("pi_name", "deadline", "department", "field", "keywords",
                "requirements", "conditions", "country", "region",
                "scholar_url", "lab_url", "dept_url", "h_index", "citations"):
        if not best.get(key) and other.get(key):
            best[key] = other[key]

    # Preserve alt URL from the other source for cross-source duplicates.
    # Skip if same source (separate postings on one site are distinct jobs)
    # or if either side lacks a PI name (risk of false merge).
    if (
        best.get("source") != other.get("source")
        and best.get("pi_name")
        and other.get("pi_name")
        and other.get("url")
    ):
        alt_urls = best.get("alt_urls", [])
        alt_urls.append({"url": other["url"], "source": other.get("source", "")})
        best["alt_urls"] = alt_urls

    return best


def _make_dedup_text(job: dict) -> str:
    """Build the text blob used for MinHash shingling."""
    title = normalize_title(job.get("title", ""))
    inst = normalize_institute(job.get("institute", ""))
    pi = (job.get("pi_name") or "").lower().strip()
    return f"{title} {inst} {pi}"


def deduplicate_jobs(jobs: list[dict], threshold: float = 0.85) -> list[dict]:
    """Remove duplicate jobs, keeping the entry with the longest description.

    Algorithm
    ---------
    1. Build fast-path indexes (URL set, PI+institute key map) for O(1) exact
       duplicate detection.
    2. Compute MinHash signatures and insert into LSH band buckets for
       approximate nearest-neighbour candidate generation.
    3. For each incoming job, check fast-path indexes first, then query LSH
       buckets for candidates, and verify with ``is_duplicate()``.
    4. Non-duplicate jobs are appended to the unique list and registered in
       all indexes.

    Complexity: O(n) amortised (vs. O(n^2) in the previous implementation).
    """
    if not jobs:
        return []

    unique: list[dict] = []

    # Fast-path index: URL -> index in unique[]
    url_index: dict[str, int] = {}
    # Fast-path index: (pi_name_lower, institute_normalized) -> index in unique[]
    pi_inst_index: dict[tuple[str, str], int] = {}
    # Fast-path index: pi_name_lower -> list of indices in unique[]
    # Used for PI-only matching (full names across different institutes)
    pi_only_index: dict[str, list[int]] = defaultdict(list)
    # LSH band tables: band_number -> {bucket_key -> set of indices in unique[]}
    lsh_tables: list[dict[bytes, list[int]]] = [defaultdict(list) for _ in range(_NUM_BANDS)]
    # Cached signatures for jobs in unique[]
    signatures: list[list[int]] = []

    def _register(idx: int, job: dict, sig: list[int]) -> None:
        """Register a job in all fast-path and LSH indexes."""
        url = job.get("url")
        if url:
            url_index[url] = idx
        pi = (job.get("pi_name") or "").lower().strip()
        inst = normalize_institute(job.get("institute", ""))
        if pi and inst:
            pi_inst_index[(pi, inst)] = idx
        if pi:
            pi_only_index[pi].append(idx)
        buckets = _lsh_buckets(sig)
        for band_num, bucket_key in enumerate(buckets):
            lsh_tables[band_num][bucket_key].append(idx)
        signatures.append(sig)

    for job in jobs:
        # ── Fast path 1: exact URL match ──────────────────────────────
        url = job.get("url")
        if url and url in url_index:
            dup_idx = url_index[url]
            logger.debug(
                "Duplicate (URL): '%s' ≈ '%s'",
                job.get("title", "")[:50],
                unique[dup_idx].get("title", "")[:50],
            )
            unique[dup_idx] = _pick_best(unique[dup_idx], job)
            continue

        # ── Fast path 2: same PI at same institute ────────────────────
        pi = (job.get("pi_name") or "").lower().strip()
        inst = normalize_institute(job.get("institute", ""))
        if pi and inst and (pi, inst) in pi_inst_index:
            dup_idx = pi_inst_index[(pi, inst)]
            title_sim = similarity(
                normalize_title(job.get("title", "")),
                normalize_title(unique[dup_idx].get("title", "")),
            )
            if title_sim > 0.4:
                logger.debug(
                    "Duplicate (PI+inst): '%s' ≈ '%s'",
                    job.get("title", "")[:50],
                    unique[dup_idx].get("title", "")[:50],
                )
                unique[dup_idx] = _pick_best(unique[dup_idx], job)
                continue

        # ── Fast path 3: same PI name across different institutes ─────
        if pi and pi in pi_only_index:
            matched_idx: Optional[int] = None
            for cand_idx in pi_only_index[pi]:
                if is_duplicate(job, unique[cand_idx], threshold):
                    matched_idx = cand_idx
                    break
            if matched_idx is not None:
                logger.debug(
                    "Duplicate (PI-only): '%s' ≈ '%s'",
                    job.get("title", "")[:50],
                    unique[matched_idx].get("title", "")[:50],
                )
                unique[matched_idx] = _pick_best(unique[matched_idx], job)
                continue

        # ── LSH candidate generation ──────────────────────────────────
        text = _make_dedup_text(job)
        shingles = _shingle(text)
        sig = _minhash_signature(shingles)
        buckets = _lsh_buckets(sig)

        # Collect candidate indices from all bands
        candidates: set[int] = set()
        for band_num, bucket_key in enumerate(buckets):
            candidates.update(lsh_tables[band_num].get(bucket_key, []))

        # ── Verify candidates with full is_duplicate() ────────────────
        dup_idx: Optional[int] = None
        for cand_idx in candidates:
            if is_duplicate(job, unique[cand_idx], threshold):
                dup_idx = cand_idx
                break

        if dup_idx is not None:
            logger.debug(
                "Duplicate (LSH): '%s' ≈ '%s'",
                job.get("title", "")[:50],
                unique[dup_idx].get("title", "")[:50],
            )
            unique[dup_idx] = _pick_best(unique[dup_idx], job)
        else:
            new_idx = len(unique)
            unique.append(job)
            _register(new_idx, job, sig)

    removed = len(jobs) - len(unique)
    if removed:
        logger.info("Deduplicated: %d → %d jobs (%d removed)", len(jobs), len(unique), removed)
    return unique
