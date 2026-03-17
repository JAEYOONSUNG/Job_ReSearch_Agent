"""Score job postings against CV keywords and preferences."""

import logging
import re
import unicodedata

from src.config import (
    COUNTRY_TO_REGION,
    CV_KEYWORDS,
    INSTITUTE_COUNTRY_RULES,
    REGION_PRIORITY,
    load_rankings,
)

logger = logging.getLogger(__name__)


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower().strip())


def _strip_accents(text: str) -> str:
    """Remove accents/diacritics: Zürich→Zurich, Université→Universite, Poznań→Poznan."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _norm_inst(text: str) -> str:
    """Normalize institute name for matching: lowercase, strip accents, remove punctuation."""
    text = _strip_accents(text.lower().strip())
    # Remove commas, periods, "the " prefix, ", inc." suffix
    text = re.sub(r"[,.]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^the ", "", text)
    text = re.sub(r"\s+inc$", "", text)
    return text


# English CV keyword → Korean synonyms for bilingual matching
_CV_KEYWORD_KR: dict[str, list[str]] = {
    "synthetic biology": ["합성생물학", "합성 생물학"],
    "crispr": ["크리스퍼", "유전자가위", "유전자 가위"],
    "cas9": ["크리스퍼"],
    "cas12": ["크리스퍼"],
    "protein engineering": ["단백질공학", "단백질 공학"],
    "directed evolution": ["지향진화", "지향 진화"],
    "extremophile": ["극한미생물", "극한 미생물"],
    "thermophile": ["고온성 미생물", "호열균"],
    "archaea": ["고세균"],
    "metabolic engineering": ["대사공학", "대사 공학"],
    "genome engineering": ["유전체공학", "유전체 공학"],
    "gene editing": ["유전자편집", "유전자 편집"],
    "microbiology": ["미생물학", "미생물"],
    "molecular biology": ["분자생물학", "분자 생물학"],
    "systems biology": ["시스템생물학", "시스템 생물학"],
    "bioinformatics": ["생물정보학", "생물정보"],
    "structural biology": ["구조생물학", "구조 생물학"],
    "enzyme engineering": ["효소공학", "효소 공학"],
    "genomics": ["유전체학", "유전체", "게노믹스"],
    "proteomics": ["단백체학", "단백질체학", "프로테오믹스"],
    "metagenomics": ["메타유전체", "메타지노믹스", "마이크로바이옴"],
    "fermentation": ["발효"],
    "bioprocess": ["바이오공정"],
    "immunology": ["면역학", "면역"],
    "neuroscience": ["신경과학", "뇌과학", "신경"],
    "drug discovery": ["약학", "약물 발견", "신약"],
    "pharmacology": ["약리학"],
    "biochemistry": ["생화학"],
    "biophysics": ["생물물리학"],
    "chemical biology": ["화학생물학", "화학 생물학"],
    "cell biology": ["세포생물학", "세포 생물학"],
    "stem cell": ["줄기세포"],
    "organoid": ["오가노이드"],
    "epigenetics": ["후성유전학", "후성 유전학"],
    "biotechnology": ["생명공학", "생명 공학", "바이오"],
}


def keyword_match_score(job_text: str, keywords: list[str] = None) -> float:
    """Calculate keyword overlap score (0.0 - 1.0).

    Supports bilingual matching: if an English keyword doesn't match,
    tries Korean synonyms from ``_CV_KEYWORD_KR``.
    """
    if keywords is None:
        keywords = CV_KEYWORDS
    if not keywords or not job_text:
        return 0.0

    text = _normalize(job_text)
    matches = 0
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower in text:
            matches += 1
        else:
            # Try Korean synonyms
            kr_syns = _CV_KEYWORD_KR.get(kw_lower, [])
            if any(syn in text for syn in kr_syns):
                matches += 1
    return matches / len(keywords)


def get_institution_tier(institute: str) -> int:
    """Look up institution tier from rankings. Returns 1-5 (5 = unranked).

    Checks tiers 1-4, then companies (top_companies -> 2, companies -> 3).
    Also resolves aliases from tier_lookup_aliases.
    Uses accent-stripped matching (ETH Zürich == ETH Zurich).
    """
    if not institute:
        return 5

    rankings = load_rankings()
    tiers = rankings.get("tiers", {})
    aliases = rankings.get("tier_lookup_aliases", {})
    inst_norm = _norm_inst(institute)

    # Check aliases first (normalized, short aliases require exact match)
    for alias, canonical in aliases.items():
        alias_n = _norm_inst(alias)
        if alias_n == inst_norm:
            inst_norm = _norm_inst(canonical)
            break
        # Only do substring match for aliases >= 5 chars to avoid false positives
        if len(alias_n) >= 5 and alias_n in inst_norm:
            inst_norm = _norm_inst(canonical)
            break

    # Check tiers 1-4
    for tier_num, tier_data in tiers.items():
        try:
            tier_int = int(tier_num)
        except (ValueError, TypeError):
            continue
        for inst in tier_data.get("institutions", []):
            ref = _norm_inst(inst)
            if ref in inst_norm or inst_norm in ref:
                return tier_int

    # Check companies section
    companies = rankings.get("companies", {})
    top_co = companies.get("top_companies", {})
    top_list = top_co.get("institutions", []) if isinstance(top_co, dict) else top_co
    for inst in top_list:
        ref = _norm_inst(inst)
        if ref in inst_norm or inst_norm in ref:
            return top_co.get("tier_equivalent", 2) if isinstance(top_co, dict) else 2

    std_co = companies.get("companies", {})
    std_list = std_co.get("institutions", []) if isinstance(std_co, dict) else std_co
    for inst in std_list:
        ref = _norm_inst(inst)
        if ref in inst_norm or inst_norm in ref:
            return std_co.get("tier_equivalent", 3) if isinstance(std_co, dict) else 3

    return 5


def is_company(institute: str) -> bool:
    """Return True if the institute is a company (not academic institution).

    Uses stricter matching than tier lookup to avoid false positives
    (e.g., "MIT" must not match "GlaxoSmithKline").
    """
    if not institute:
        return False
    rankings = load_rankings()
    companies = rankings.get("companies", {})
    inst_n = _norm_inst(institute)

    for group in companies.values():
        comp_list = group.get("institutions", []) if isinstance(group, dict) else group
        for comp in comp_list:
            comp_n = _norm_inst(comp)
            shorter = min(len(inst_n), len(comp_n))
            if shorter < 5:
                if inst_n == comp_n:
                    return True
            else:
                if comp_n in inst_n or inst_n in comp_n:
                    return True
    return False


def guess_country_from_institute(institute: str) -> str:
    """Infer country from institution name using known patterns.

    Returns a country string (e.g. "United States") or empty string if unknown.
    """
    if not institute:
        return ""
    inst_lower = institute.lower()
    for pattern, country in INSTITUTE_COUNTRY_RULES:
        if pattern in inst_lower:
            return country
    return ""


def get_region(country: str) -> str:
    """Map country to region."""
    if not country:
        return "Other"
    # Direct match
    if country in COUNTRY_TO_REGION:
        return COUNTRY_TO_REGION[country]
    # Partial match
    country_lower = country.lower()
    for key, region in COUNTRY_TO_REGION.items():
        if key.lower() in country_lower or country_lower in key.lower():
            return region
    return "Other"


def score_job(job: dict, keywords: list[str] = None) -> dict:
    """Score a job and enrich with region/tier info.

    Returns the job dict with added fields:
      - match_score: float (0.0 - 1.0)
      - region: str
      - tier: int (1-5, where 5 = unranked)
      - sort_key: tuple for sorting
    """
    text = " ".join(
        str(job.get(f, ""))
        for f in ("title", "description", "field", "pi_name", "institute")
    )

    match_score = keyword_match_score(text, keywords)
    country = job.get("country") or ""
    if not country:
        country = guess_country_from_institute(job.get("institute", ""))
    region = job.get("region") or get_region(country)
    tier = get_institution_tier(job.get("institute", "")) or job.get("tier") or 5
    h_index = job.get("h_index") or 0

    job["match_score"] = round(match_score, 3)
    job["region"] = region
    job["tier"] = tier

    # Sort key: region priority ASC, tier ASC, h_index DESC, match_score DESC
    job["sort_key"] = (
        REGION_PRIORITY.get(region, 99),
        tier,
        -h_index,
        -match_score,
    )

    return job


def score_and_sort_jobs(jobs: list[dict], keywords: list[str] = None) -> list[dict]:
    """Score and sort a list of jobs by priority."""
    scored = [score_job(j, keywords) for j in jobs]
    scored.sort(key=lambda j: j["sort_key"])
    return scored
