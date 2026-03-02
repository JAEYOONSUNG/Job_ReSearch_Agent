"""Export job data to Excel files with parsed, structured columns."""

import json
import logging
import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from src.config import EXCEL_OUTPUT_DIR, EXCLUDE_KEYWORDS
from src.db import get_all_pis, get_jobs, get_recommended_pis
from src.matching.job_parser import parse_structured_description
from src.matching.scorer import is_company

logger = logging.getLogger(__name__)

PI_COLUMNS = [
    "PI Name",
    "Institute",
    "Country",
    "Tier",
    "h-index",
    "Citations",
    "Fields",
    "Connected Seeds",
    "Recommendation Score",
    "Lab URL",
    "Scholar URL",
]


# ---------------------------------------------------------------------------
# Text cleaning helpers
# ---------------------------------------------------------------------------

def _clean_text(text: str | None, max_len: int = 500) -> str:
    """Strip markdown, excessive whitespace, and truncate.

    Flattens all newlines into ``|`` section dividers for readable
    single-line display.  Adjacent short lines are merged; headers
    (``### Header`` or ``Header =====``) become ``| HEADER |``.
    """
    if not text:
        return ""
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    # Remove markdown header markers: ### Header
    text = re.sub(r"^#{1,4}\s*", "", text, flags=re.MULTILINE)
    # Convert "Header ====+" or "====+" underline-style headers to section break
    text = re.sub(r"^(.+?)\s*={4,}\s*$", r"| \1 |", text, flags=re.MULTILINE)
    text = re.sub(r"^={4,}\s*$", "", text, flags=re.MULTILINE)
    # Convert "---+" horizontal rules to section break
    text = re.sub(r"^-{4,}\s*$", "|", text, flags=re.MULTILINE)
    # Remove markdown links but keep text
    text = re.sub(r"\[(.+?)\]\(.+?\)", r"\1", text)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Unescape backslash-escaped characters (e.g. \- from Indeed)
    text = re.sub(r"\\([^\\])", r"\1", text)
    # Replace paragraph breaks (2+ newlines) with " | "
    text = re.sub(r"\s*\n\s*\n\s*", " | ", text)
    # Replace single newlines with space
    text = text.replace("\n", " ").replace("\r", " ")
    # Collapse all whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Clean up redundant pipe separators
    text = re.sub(r"\|\s*\|", "|", text)
    text = re.sub(r"^\s*\|\s*", "", text)
    text = re.sub(r"\s*\|\s*$", "", text)
    return text[:max_len]


_MAX_PAPER_COLS = 5  # number of individual paper columns


def _parse_papers(papers_json: str | None) -> list[dict]:
    """Parse JSON paper list into a list of paper dicts."""
    if not papers_json:
        return []
    try:
        papers = json.loads(papers_json)
    except (json.JSONDecodeError, TypeError):
        return []
    return papers if isinstance(papers, list) else []


def _papers_to_columns(prefix: str, papers: list[dict]) -> dict[str, str]:
    """Convert a list of paper dicts to individual column values.

    Returns e.g. {"Recent Paper 1": "(2024) Title [10 cites]", ...}.
    The display text is used as a placeholder; actual hyperlinks are written
    by _write_paper_links() after the DataFrame is exported.
    """
    result: dict[str, str] = {}
    for i in range(_MAX_PAPER_COLS):
        col_name = f"{prefix} {i + 1}"
        if i < len(papers):
            p = papers[i]
            year = p.get("year") or "?"
            title = (p.get("title") or "Untitled")[:80]
            cites = p.get("citation_count", 0)
            result[col_name] = f"({year}) {title} [{cites} cites]"
        else:
            result[col_name] = ""
    return result


def _clean_list(text: str | None, max_len: int = 400) -> str:
    """Clean a list-style section (requirements, etc).

    Flattens bullets into semicolon-separated items for compact display.
    """
    if not text:
        return ""
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    # Remove markdown headers
    text = re.sub(r"^#{1,4}\s*", "", text, flags=re.MULTILINE)
    # Remove underline-style headers
    text = re.sub(r"={4,}", "", text)
    text = re.sub(r"-{4,}", "", text)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Unescape backslash-escaped characters
    text = re.sub(r"\\([^\\])", r"\1", text)
    # Normalise bullet markers to "; "
    text = re.sub(r"\n\s*[-•*]\s*", "; ", text)
    text = re.sub(r"\n\s*\d+[.)]\s*", "; ", text)
    # Replace remaining newlines with spaces
    text = text.replace("\n", " ").replace("\r", " ")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Clean up leading/trailing semicolons
    text = text.strip("; ").strip()
    return text[:max_len]


# ---------------------------------------------------------------------------
# Condition parsing — split into sub-fields
# ---------------------------------------------------------------------------

def _format_salary_number(s: str) -> str:
    """Add thousand-separator commas to bare salary numbers.

    ``$56484`` → ``$56,484``;  ``€45000-€55000`` → ``€45,000-€55,000``
    ``3800`` → ``3,800``;  ``42.185,28`` → ``42,185``
    Leaves already-formatted strings (``$56,484``) and non-numeric
    salary scales (``TV-L E13``) unchanged.
    """
    # First, normalise EU thousands separator (42.185,28 → 42185.28)
    s = re.sub(r"(\d{1,3})\.(\d{3}),(\d{2})\b", lambda m: f"{m.group(1)}{m.group(2)}", s)
    # Also handle "42.185" standalone (EU thousands, no decimals)
    s = re.sub(r"\b(\d{1,3})\.(\d{3})\b(?![\d.])", lambda m: f"{m.group(1)}{m.group(2)}", s)

    def _add_commas(m: re.Match) -> str:
        return f"{int(m.group()):,}"

    # Currency-prefixed: 4+ digits after $, €, £
    s = re.sub(r"(?<=[\$€£])\d{4,}", _add_commas, s)
    # Bare numbers: 4+ digits (not preceded/followed by digit or dot)
    s = re.sub(r"(?<![.\d])\b(\d{4,})\b(?![\d.])", lambda m: f"{int(m.group(1)):,}", s)
    # Strip decimal places from numbers (e.g. $60,000.00 → $60,000, 36.0 → 36)
    s = re.sub(r"(\d)\.0{1,2}\b", r"\1", s)          # .0 or .00
    s = re.sub(r"(\d)\.\d{1,2}(?=\s*[€$£%])", r"\1", s)  # 42,392.88€ → 42,392€
    s = re.sub(r"(\d)\.\d{1,2}(?=\s*/)", r"\1", s)   # $60,000.00/yr → $60,000/yr
    # Large comma-formatted numbers: strip remaining decimals (€ 70,167.44 etc)
    s = re.sub(r"(\d{1,3}(?:,\d{3})+)\.\d{1,2}\b", r"\1", s)
    return s


def _parse_salary(conditions: str, description: str) -> str:
    """Extract salary info from conditions or description."""
    blob = f"{conditions} {description}"
    patterns = [
        r"(?:salary|stipend|compensation|remuneration)\s*(?:of|is|:)?\s*([\$€£]?\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–]\s*[\$€£]?\s*\d[\d,]*(?:\.\d+)?)?)\s*(?:k|K|per\s+(?:year|annum|month)|/\s*(?:yr|year|annum|month(?:ly)?)|euros?(?:/month)?)?",
        r"([\$€£]\s*\d[\d,]*(?:\.\d+)?(?:\s*[-–]\s*[\$€£]?\s*\d[\d,]*(?:\.\d+)?)?)\s*(?:k|K)?\s*(?:per\s+(?:year|annum)|/\s*(?:yr|year|annum|yearly))",
        r"(TV-?L\s*E?\s*1[3-5](?:\s*/\s*E?\s*1[3-5])?)",
        r"(NIH\s+(?:scale|salary))",
        r"(Grade\s+\d+|Band\s+\d+|Scale\s+\d+)",
        r"Salary:\s*([\$€£]?\d[\d,.]*(?:\s*[-–]\s*[\$€£]?\d[\d,.]*)?(?:/\w+)?)",
        r"(\d[\d.]*,\d{2}\s*€)",  # EU format: "42.185,28 €"
    ]
    for p in patterns:
        m = re.search(p, blob, re.IGNORECASE)
        if m:
            raw = m.group(1).strip() if m.lastindex else m.group(0).strip()
            return _format_salary_number(raw)
    return ""


def _parse_duration(conditions: str, description: str) -> str:
    """Extract duration/contract length."""
    blob = f"{conditions} {description}"
    patterns = [
        r"(\d+(?:\s*[-–]\s*\d+)?)\s*(?:year|yr)s?\s*(?:position|appointment|contract|renewable)?",
        r"(\d+(?:\s*[-–]\s*\d+)?)\s*months?\s*(?:position|appointment|contract)?",
        r"(?:up to|minimum|at least|initially)\s+(\d+)\s+(?:year|yr|month)s?",
        r"(?:duration|length|term)\s*(?:of|:)\s*(\d+\s*(?:year|yr|month)s?)",
        r"Contract:\s*(\w[\w\s,-]*)",
    ]
    for p in patterns:
        m = re.search(p, blob, re.IGNORECASE)
        if m:
            return m.group(0).strip()[:60]
    return ""


def _parse_contract_type(conditions: str, description: str) -> str:
    """Extract contract type (full-time, fixed-term, etc)."""
    blob = f"{conditions} {description}".lower()
    types = []
    if "full-time" in blob or "full time" in blob or "fulltime" in blob:
        types.append("Full-time")
    elif "part-time" in blob or "part time" in blob:
        types.append("Part-time")
    if "fixed-term" in blob or "fixed term" in blob or "temporary" in blob:
        types.append("Fixed-term")
    elif "permanent" in blob or "tenure" in blob:
        types.append("Permanent")
    return ", ".join(types) if types else ""


def _parse_start_date(conditions: str, description: str) -> str:
    """Extract start date."""
    blob = f"{conditions} {description}"
    patterns = [
        r"(?:start(?:ing)?\s*(?:date)?|commencement|begin(?:ning)?)\s*[:=]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
        r"(?:start(?:ing)?|available|begin)\s*(?:date)?\s*[:=]?\s*((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
        r"(?:start(?:ing)?)\s*[:=]?\s*(\d{4}-\d{2}-\d{2})",
        r"(?:as soon as possible|immediately|ASAP)",
    ]
    for p in patterns:
        m = re.search(p, blob, re.IGNORECASE)
        if m:
            return m.group(1).strip() if m.lastindex else m.group(0).strip()
    return ""


# ---------------------------------------------------------------------------
# Requirements parsing — split into sub-fields
# ---------------------------------------------------------------------------

def _parse_degree(requirements: str, description: str) -> str:
    """Extract required degree."""
    blob = f"{requirements} {description}"
    patterns = [
        r"(Ph\.?D\.?\s+in\s+[\w\s,/&]+?)(?:\.|;|\n|$)",
        r"((?:Doctoral|PhD|Ph\.D)\s+(?:degree\s+)?in\s+[\w\s,/&]+?)(?:\.|;|\n|$)",
        r"((?:Master'?s?|M\.?S\.?|M\.?Sc\.?)\s+(?:degree\s+)?in\s+[\w\s,/&]+?)(?:\.|;|\n|$)",
    ]
    for p in patterns:
        m = re.search(p, blob, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:120]
    return ""


def _parse_skills(requirements: str, description: str) -> str:
    """Extract required skills/techniques."""
    blob = f"{requirements} {description}".lower()
    skills = []
    skill_keywords = [
        "CRISPR", "Cas9", "Cas12", "flow cytometry", "FACS",
        "mass spectrometry", "NGS", "PCR", "qPCR", "RT-PCR",
        "Western blot", "microscopy", "confocal", "cryo-EM",
        "Python", "R programming", "bioinformatics", "machine learning",
        "cell culture", "cloning", "protein purification",
        "HPLC", "NMR", "X-ray crystallography",
        "mouse model", "animal model", "in vivo",
        "single-cell", "RNA-seq", "ChIP-seq", "ATAC-seq",
        "electrophysiology", "patch clamp", "optogenetics",
        "fermentation", "bioprocess", "metabolic flux",
        "directed evolution", "high-throughput screening",
        "gene editing", "genome engineering",
    ]
    for skill in skill_keywords:
        if skill.lower() in blob:
            skills.append(skill)
    return ", ".join(skills[:10]) if skills else ""


# ---------------------------------------------------------------------------
# Main row builder — parsed, pivoted columns
# ---------------------------------------------------------------------------

JOB_COLUMNS = [
    "Tier",
    "Title",
    "PI Name",
    "Institute",
    "Department",
    "Country",
    "Region",
    # Research
    "Field",
    "Keywords",
    # Structured sections (parsed from LinkedIn/Indeed descriptions)
    "Position Summary",
    "Responsibilities",
    "Preferred Qualifications",
    # Requirements (parsed)
    "Degree Required",
    "Skills/Techniques",
    "Requirements (Full)",
    # Conditions (parsed)
    "Salary",
    "Duration",
    "Contract Type",
    "Start Date",
    "Conditions (Full)",
    # Dates
    "Posted Date",
    "Deadline",
    "Application Materials",
    # Description
    "Description",
    # PI Papers — individual columns
    *[f"Recent Paper {i+1}" for i in range(_MAX_PAPER_COLS)],
    *[f"Top Cited Paper {i+1}" for i in range(_MAX_PAPER_COLS)],
    # Contact & info
    "Contact Email",
    "Info URL 1",
    "Info URL 2",
    "Info URL 3",
    # Links
    "Job URL",
    "Job URL 2",
    "Lab URL",
    "Scholar URL",
    "Dept URL",
    # Meta
    "Match Score",
    "Source",
    "Status",
]


_MAX_INFO_URLS = 3  # number of Info URL columns


def _info_urls_to_columns(info_urls_json: str | None) -> dict[str, str]:
    """Convert info_urls JSON to individual column values."""
    urls: list[str] = []
    if info_urls_json:
        try:
            urls = json.loads(info_urls_json)
        except (json.JSONDecodeError, TypeError):
            pass
    result: dict[str, str] = {}
    for i in range(_MAX_INFO_URLS):
        col = f"Info URL {i + 1}"
        result[col] = urls[i] if i < len(urls) else ""
    return result


def _format_tier(tier, institute: str = "") -> str:
    """Format tier for display: T1-T4 for institutions, 'Company' for companies, '' for unranked."""
    if institute and is_company(institute):
        return "Company"
    if not tier or tier >= 5:
        return ""
    return f"T{tier}"


def _job_to_row(job: dict) -> dict:
    tier = job.get("tier")
    institute = job.get("institute") or ""
    desc = job.get("description") or ""
    req = job.get("requirements") or ""
    cond = job.get("conditions") or ""

    # Parse structured sections from description (LinkedIn/Indeed format)
    sections = parse_structured_description(desc)

    # Use parsed requirements/conditions as fallback for empty fields
    if not req and sections.get("requirements"):
        req = sections["requirements"]
    if not cond and sections.get("conditions"):
        cond = sections["conditions"]

    return {
        "Title": _clean_text(job.get("title"), 100),
        "PI Name": job.get("pi_name") or "",
        "Institute": institute,
        "Department": job.get("department") or "",
        "Tier": _format_tier(tier, institute),
        "Country": job.get("country") or "",
        "Region": job.get("region") or "",
        # Research
        "Field": job.get("field") or "",
        "Keywords": job.get("keywords") or "",
        # Structured sections
        "Position Summary": _clean_text(sections.get("summary"), 800),
        "Responsibilities": _clean_list(sections.get("responsibilities"), 800),
        "Preferred Qualifications": _clean_list(sections.get("preferred"), 600),
        # Requirements parsed
        "Degree Required": _parse_degree(req, desc),
        "Skills/Techniques": _parse_skills(req, desc),
        "Requirements (Full)": _clean_list(req, 600),
        # Conditions parsed
        "Salary": _parse_salary(cond, desc),
        "Duration": _parse_duration(cond, desc),
        "Contract Type": _parse_contract_type(cond, desc),
        "Start Date": _parse_start_date(cond, desc),
        "Conditions (Full)": _format_salary_number(_clean_list(cond, 400)),
        # Dates
        "Posted Date": job.get("posted_date") or "",
        "Deadline": job.get("deadline") or "",
        "Application Materials": _clean_list(job.get("application_materials"), 400),
        # Description
        "Description": _clean_text(desc, 15000),
        # PI Papers — individual columns (title text; hyperlinks added in _write_paper_links)
        **_papers_to_columns("Recent Paper", _parse_papers(job.get("recent_papers"))),
        **_papers_to_columns("Top Cited Paper", _parse_papers(job.get("top_cited_papers"))),
        # Contact & info
        "Contact Email": job.get("contact_email") or "",
        **_info_urls_to_columns(job.get("info_urls")),
        # Links
        "Job URL": job.get("url") or "",
        "Job URL 2": job.get("alt_url") or "",
        "Lab URL": job.get("lab_url") or "",
        "Scholar URL": job.get("scholar_url") or "",
        "Dept URL": job.get("dept_url") or "",
        # Meta
        "Match Score": job.get("match_score", 0),
        "Source": job.get("source") or "",
        "Status": job.get("status") or "",
    }


def _pi_to_row(pi: dict) -> dict:
    institute = pi.get("institute", "")
    return {
        "PI Name": pi.get("name", ""),
        "Institute": institute,
        "Country": pi.get("country", ""),
        "Tier": _format_tier(pi.get("tier"), institute),
        "h-index": pi.get("h_index", ""),
        "Citations": pi.get("citations", ""),
        "Fields": pi.get("fields", ""),
        "Connected Seeds": pi.get("connected_seeds", ""),
        "Recommendation Score": pi.get("recommendation_score", 0),
        "Lab URL": pi.get("lab_url", ""),
        "Scholar URL": pi.get("scholar_url", ""),
    }


def _write_paper_links(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
    jobs: list[dict],
) -> None:
    """Overwrite paper cells with clickable hyperlinks pointing to S2/DOI URLs."""
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    link_fmt = workbook.add_format({
        "font_color": "#0563C1",
        "underline": True,
        "valign": "vcenter",
        "text_wrap": True,
        "font_size": 9,
    })

    # Build column index lookup for paper columns
    paper_col_indices: dict[str, int] = {}
    for col_name in df.columns:
        if col_name.startswith("Recent Paper ") or col_name.startswith("Top Cited Paper "):
            paper_col_indices[col_name] = df.columns.get_loc(col_name)

    for row_idx, job in enumerate(jobs):
        excel_row = row_idx + 1  # +1 for header row

        # Recent papers
        recent = _parse_papers(job.get("recent_papers"))
        for i in range(min(_MAX_PAPER_COLS, len(recent))):
            col_name = f"Recent Paper {i + 1}"
            col_idx = paper_col_indices.get(col_name)
            if col_idx is None:
                continue
            p = recent[i]
            url = p.get("url", "")
            year = p.get("year") or "?"
            title = (p.get("title") or "Untitled")[:80]
            cites = p.get("citation_count", 0)
            display = f"({year}) {title} [{cites} cites]"
            if url:
                worksheet.write_url(excel_row, col_idx, url, link_fmt, display)
            else:
                worksheet.write(excel_row, col_idx, display)

        # Top cited papers
        top_cited = _parse_papers(job.get("top_cited_papers"))
        for i in range(min(_MAX_PAPER_COLS, len(top_cited))):
            col_name = f"Top Cited Paper {i + 1}"
            col_idx = paper_col_indices.get(col_name)
            if col_idx is None:
                continue
            p = top_cited[i]
            url = p.get("url", "")
            year = p.get("year") or "?"
            title = (p.get("title") or "Untitled")[:80]
            cites = p.get("citation_count", 0)
            display = f"({year}) {title} [{cites} cites]"
            if url:
                worksheet.write_url(excel_row, col_idx, url, link_fmt, display)
            else:
                worksheet.write(excel_row, col_idx, display)


def _style_worksheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Apply formatting to a worksheet with auto-filters and conditional formatting."""
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book

    # Header format
    header_fmt = workbook.add_format({
        "bold": True,
        "bg_color": "#16213e",
        "font_color": "white",
        "valign": "vcenter",
        "border": 1,
    })

    # Write headers with format
    for i, col in enumerate(df.columns):
        worksheet.write(0, i, col, header_fmt)

    # Column widths (tuned per column type)
    width_map = {
        "Title": 40, "PI Name": 18, "Institute": 25, "Department": 20,
        "Tier": 5, "Country": 12, "Region": 6,
        "Field": 20, "Keywords": 35,
        "Position Summary": 45, "Responsibilities": 45, "Preferred Qualifications": 40,
        "Degree Required": 30, "Skills/Techniques": 30, "Requirements (Full)": 40,
        "Salary": 18, "Duration": 18, "Contract Type": 12, "Start Date": 14,
        "Conditions (Full)": 30,
        "Posted Date": 11, "Deadline": 11,
        "Description": 50,
        **{f"Recent Paper {i+1}": 35 for i in range(_MAX_PAPER_COLS)},
        **{f"Top Cited Paper {i+1}": 35 for i in range(_MAX_PAPER_COLS)},
        "Contact Email": 25,
        **{f"Info URL {i+1}": 20 for i in range(_MAX_INFO_URLS)},
        "Job URL": 15, "Job URL 2": 15, "Lab URL": 15, "Scholar URL": 15, "Dept URL": 15,
        "Match Score": 8, "Source": 12, "Status": 7,
    }

    url_fmt = workbook.add_format({"font_color": "blue", "underline": True, "valign": "vcenter"})
    default_fmt = workbook.add_format({"valign": "vcenter"})

    for i, col in enumerate(df.columns):
        width = width_map.get(col, 15)
        if col in ("Job URL", "Job URL 2", "Lab URL", "Scholar URL", "Dept URL",
                   "Info URL 1", "Info URL 2", "Info URL 3"):
            worksheet.set_column(i, i, width, url_fmt)
        else:
            worksheet.set_column(i, i, width, default_fmt)

    # Compact row height
    worksheet.set_default_row(20)

    # Auto-filter on all columns
    if len(df) > 0:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

    # Conditional formatting: highlight Tier column
    if "Tier" in df.columns:
        tier_col = df.columns.get_loc("Tier")
        tier_range = f"{chr(65 + tier_col)}2:{chr(65 + tier_col)}{len(df) + 1}"
        # T1 = gold, T2 = blue, T3 = green, T4 = light gray
        worksheet.conditional_format(
            tier_range,
            {"type": "text", "criteria": "containing", "value": "T1",
             "format": workbook.add_format({"bg_color": "#FFF3CD", "font_color": "#856404", "bold": True})},
        )
        worksheet.conditional_format(
            tier_range,
            {"type": "text", "criteria": "containing", "value": "T2",
             "format": workbook.add_format({"bg_color": "#CCE5FF", "font_color": "#004085"})},
        )
        worksheet.conditional_format(
            tier_range,
            {"type": "text", "criteria": "containing", "value": "T3",
             "format": workbook.add_format({"bg_color": "#D4EDDA", "font_color": "#155724"})},
        )
        worksheet.conditional_format(
            tier_range,
            {"type": "text", "criteria": "containing", "value": "T4",
             "format": workbook.add_format({"bg_color": "#E2E3E5", "font_color": "#383D41"})},
        )
        worksheet.conditional_format(
            tier_range,
            {"type": "text", "criteria": "containing", "value": "Company",
             "format": workbook.add_format({"bg_color": "#E8DAEF", "font_color": "#6C3483", "bold": True})},
        )

    # Conditional formatting: color scale on Match Score
    if "Match Score" in df.columns:
        score_col = df.columns.get_loc("Match Score")
        worksheet.conditional_format(
            1, score_col, len(df), score_col,
            {"type": "3_color_scale",
             "min_color": "#F8D7DA", "mid_color": "#FFF3CD", "max_color": "#D4EDDA"},
        )

    # Conditional formatting: color scale on Recommendation Score (PI sheet)
    if "Recommendation Score" in df.columns:
        rec_col = df.columns.get_loc("Recommendation Score")
        worksheet.conditional_format(
            1, rec_col, len(df), rec_col,
            {"type": "3_color_scale",
             "min_color": "#F8D7DA", "mid_color": "#FFF3CD", "max_color": "#D4EDDA"},
        )

    # Freeze top row for easy scrolling
    worksheet.freeze_panes(1, 0)


def _deadline_bg_color(deadline_str: str) -> tuple[str, int] | tuple[None, int]:
    """Return (hex background color, days_remaining) based on deadline urgency.

    Smooth gradient: dark red #CC0000 (0 days) → light green #D5F5E3 (90+ days).
    Expired: gray #AAAAAA. Empty/unparseable: (None, 999).
    """
    if not deadline_str:
        return None, 999
    try:
        dl = date.fromisoformat(deadline_str)
    except (ValueError, TypeError):
        return None, 999

    days_remaining = (dl - date.today()).days
    if days_remaining < 0:
        return "#AAAAAA", days_remaining

    # Clamp to 0–90 range for interpolation
    t = min(days_remaining, 90) / 90.0
    # Dark red (0.80, 0.00, 0.00) → Light green (0.84, 0.96, 0.89)
    r = int((0.80 * (1 - t) + 0.84 * t) * 255)
    g = int((0.00 * (1 - t) + 0.96 * t) * 255)
    b = int((0.00 * (1 - t) + 0.89 * t) * 255)
    return f"#{r:02X}{g:02X}{b:02X}", days_remaining


def _style_deadline_cells(
    writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame
) -> None:
    """Apply urgency-based background colors to Deadline column cells."""
    if "Deadline" not in df.columns:
        return
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    col_idx = df.columns.get_loc("Deadline")

    for row_idx in range(len(df)):
        val = df.iloc[row_idx, col_idx]
        deadline_str = str(val) if pd.notna(val) else ""
        color, days_remaining = _deadline_bg_color(deadline_str)
        if not color:
            continue

        fmt_props: dict = {
            "bg_color": color,
            "valign": "vcenter",
            "font_size": 9,
        }
        if days_remaining < 0:
            fmt_props["font_color"] = "#666666"
        elif days_remaining <= 7:
            fmt_props["bold"] = True
            fmt_props["font_color"] = "white"
        elif days_remaining <= 30:
            fmt_props["font_color"] = "white"

        cell_fmt = workbook.add_format(fmt_props)
        worksheet.write(row_idx + 1, col_idx, deadline_str, cell_fmt)


_CITIZENSHIP_PATTERNS = re.compile(
    r"U\.?S\.?\s*citizen|"
    r"citizenship\s+(?:is\s+)?require|"
    r"must\s+be\s+a?\s*(?:U\.?S\.?|American)\s*citizen|"
    r"U\.?S\.?\s*persons?\s+only|"
    r"(?:require|need)s?\s+(?:U\.?S\.?\s*)?(?:security\s+)?clearance|"
    r"\bITAR\b|"
    r"\bEAR\b.*(?:restrict|require|compliance)|"
    r"(?:only\s+)?(?:U\.?S\.?\s*)?(?:citizens?\s+(?:and|or)\s+)?permanent\s+residents?\s+(?:only|eligible|may\s+apply)",
    re.IGNORECASE,
)


def _has_citizenship_restriction(job: dict) -> bool:
    """Return True if the job requires specific citizenship/visa status."""
    blob = " ".join(
        (job.get(k) or "") for k in ("description", "conditions", "requirements")
    )
    return bool(_CITIZENSHIP_PATTERNS.search(blob))


def _style_citizenship_cells(
    writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, jobs: list[dict],
) -> None:
    """Apply red text to the Institute cell when citizenship restrictions are detected."""
    if "Institute" not in df.columns:
        return
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book
    col_idx = df.columns.get_loc("Institute")

    red_fmt = workbook.add_format({
        "font_color": "#CC0000",
        "bold": True,
        "valign": "vcenter",
    })

    for row_idx, job in enumerate(jobs):
        if _has_citizenship_restriction(job):
            val = df.iloc[row_idx, col_idx]
            institute_str = str(val) if pd.notna(val) else ""
            worksheet.write(row_idx + 1, col_idx, institute_str, red_fmt)


def _write_summary_dashboard(
    writer: pd.ExcelWriter,
    all_jobs: list[dict],
    rec_pis: list[dict],
) -> None:
    """Write a premium Summary Dashboard sheet with KPI cards, charts, and tables."""
    from collections import Counter

    workbook = writer.book
    ws = workbook.add_worksheet("Summary Dashboard")
    writer.sheets["Summary Dashboard"] = ws

    # ── Color palette ─────────────────────────────────────────────────
    NAVY = "#0F1B2D"
    ACCENT = "#3B82F6"     # bright blue
    ACCENT_DARK = "#1E40AF"
    GOLD = "#F59E0B"
    EMERALD = "#10B981"
    ROSE = "#F43F5E"
    SLATE = "#64748B"
    LIGHT_BG = "#F8FAFC"
    CARD_BG = "#FFFFFF"
    SUBTLE_BORDER = "#E2E8F0"
    TEXT_PRIMARY = "#1E293B"
    TEXT_SECONDARY = "#475569"
    TEXT_MUTED = "#94A3B8"

    # ── Reusable formats ──────────────────────────────────────────────
    bg_fmt = workbook.add_format({"bg_color": LIGHT_BG})

    header_fmt = workbook.add_format({
        "bold": True, "font_size": 22, "font_color": NAVY,
        "font_name": "Calibri", "bg_color": LIGHT_BG,
        "bottom": 0,
    })
    subtitle_fmt = workbook.add_format({
        "font_size": 10, "font_color": TEXT_MUTED,
        "font_name": "Calibri", "bg_color": LIGHT_BG,
    })
    section_title_fmt = workbook.add_format({
        "bold": True, "font_size": 13, "font_color": NAVY,
        "font_name": "Calibri", "bg_color": LIGHT_BG,
        "bottom": 2, "bottom_color": ACCENT,
    })

    # KPI card formats
    def _make_kpi_card(color: str):
        """Return (top_border, big_number, label, bg) formats for a KPI card."""
        top = workbook.add_format({
            "top": 5, "top_color": color,
            "bg_color": CARD_BG, "font_size": 1,
            "left": 1, "right": 1, "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        })
        big = workbook.add_format({
            "bold": True, "font_size": 28, "font_color": color,
            "font_name": "Calibri", "align": "center", "valign": "vcenter",
            "bg_color": CARD_BG,
            "left": 1, "right": 1, "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        })
        lbl = workbook.add_format({
            "font_size": 9, "font_color": TEXT_SECONDARY,
            "font_name": "Calibri", "align": "center", "valign": "top",
            "bg_color": CARD_BG,
            "left": 1, "right": 1, "bottom": 1,
            "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
            "bottom_color": SUBTLE_BORDER,
        })
        return top, big, lbl

    kpi_blue = _make_kpi_card(ACCENT)
    kpi_gold = _make_kpi_card(GOLD)
    kpi_emerald = _make_kpi_card(EMERALD)
    kpi_rose = _make_kpi_card(ROSE)

    # Table formats
    tbl_header_fmt = workbook.add_format({
        "bold": True, "font_size": 10, "font_color": "#FFFFFF",
        "bg_color": NAVY, "font_name": "Calibri",
        "align": "center", "valign": "vcenter",
        "top": 1, "bottom": 1, "left": 1, "right": 1,
        "top_color": NAVY, "bottom_color": NAVY,
        "left_color": NAVY, "right_color": NAVY,
    })
    tbl_row_fmt = workbook.add_format({
        "font_size": 10, "font_color": TEXT_PRIMARY,
        "font_name": "Calibri", "valign": "vcenter",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })
    tbl_row_alt_fmt = workbook.add_format({
        "font_size": 10, "font_color": TEXT_PRIMARY,
        "font_name": "Calibri", "valign": "vcenter",
        "bg_color": "#F1F5F9",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })
    tbl_num_fmt = workbook.add_format({
        "font_size": 10, "font_color": TEXT_PRIMARY,
        "font_name": "Calibri", "valign": "vcenter",
        "align": "center", "num_format": "#,##0",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })
    tbl_num_alt_fmt = workbook.add_format({
        "font_size": 10, "font_color": TEXT_PRIMARY,
        "font_name": "Calibri", "valign": "vcenter",
        "align": "center", "num_format": "#,##0",
        "bg_color": "#F1F5F9",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })

    # Tier badge formats
    tier_fmts = {
        1: workbook.add_format({
            "bold": True, "font_size": 10, "font_color": "#92400E",
            "bg_color": "#FEF3C7", "align": "center", "valign": "vcenter",
            "left": 1, "right": 1, "bottom": 1,
            "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
            "bottom_color": SUBTLE_BORDER,
        }),
        2: workbook.add_format({
            "bold": True, "font_size": 10, "font_color": "#1E40AF",
            "bg_color": "#DBEAFE", "align": "center", "valign": "vcenter",
            "left": 1, "right": 1, "bottom": 1,
            "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
            "bottom_color": SUBTLE_BORDER,
        }),
        3: workbook.add_format({
            "bold": True, "font_size": 10, "font_color": "#065F46",
            "bg_color": "#D1FAE5", "align": "center", "valign": "vcenter",
            "left": 1, "right": 1, "bottom": 1,
            "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
            "bottom_color": SUBTLE_BORDER,
        }),
        4: workbook.add_format({
            "bold": True, "font_size": 10, "font_color": "#374151",
            "bg_color": "#F3F4F6", "align": "center", "valign": "vcenter",
            "left": 1, "right": 1, "bottom": 1,
            "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
            "bottom_color": SUBTLE_BORDER,
        }),
    }

    # ── Column widths ─────────────────────────────────────────────────
    ws.set_column("A:A", 3, bg_fmt)   # left gutter
    ws.set_column("B:B", 22, bg_fmt)
    ws.set_column("C:C", 12, bg_fmt)
    ws.set_column("D:D", 3, bg_fmt)   # gap
    ws.set_column("E:E", 22, bg_fmt)
    ws.set_column("F:F", 12, bg_fmt)
    ws.set_column("G:G", 3, bg_fmt)   # gap
    ws.set_column("H:H", 22, bg_fmt)
    ws.set_column("I:I", 12, bg_fmt)
    ws.set_column("J:J", 3, bg_fmt)   # gap
    ws.set_column("K:K", 22, bg_fmt)
    ws.set_column("L:L", 12, bg_fmt)
    ws.set_column("M:M", 3, bg_fmt)   # right gutter

    ws.hide_gridlines(2)
    ws.set_tab_color(ACCENT)

    # ── Compute data ──────────────────────────────────────────────────
    region_counts = Counter(j.get("region", "Other") for j in all_jobs)
    tier_counts = Counter(j.get("tier") for j in all_jobs if j.get("tier"))
    field_counts = Counter(
        j.get("field", "Unknown") for j in all_jobs if j.get("field")
    )
    top_fields = field_counts.most_common(10)

    # Top institutions by job count
    inst_counts = Counter(
        j.get("institute", "Unknown") for j in all_jobs
        if j.get("institute") and j["institute"].lower() not in _AGGREGATOR_INSTITUTES
    )
    top_institutions = inst_counts.most_common(10)

    # Deadline urgency buckets
    urgent_count = 0    # ≤7 days
    soon_count = 0      # 8-30 days
    later_count = 0     # >30 days
    no_deadline = 0
    for j in all_jobs:
        dl = j.get("deadline") or ""
        try:
            days_left = (date.fromisoformat(dl) - date.today()).days
            if days_left < 0:
                continue
            elif days_left <= 7:
                urgent_count += 1
            elif days_left <= 30:
                soon_count += 1
            else:
                later_count += 1
        except (ValueError, TypeError):
            no_deadline += 1

    us_count = region_counts.get("US", 0)
    eu_count = region_counts.get("EU", 0)
    asia_count = region_counts.get("Asia", 0)
    other_count = region_counts.get("Other", 0)

    # ══════════════════════════════════════════════════════════════════
    # ROW 0-1: HEADER
    # ══════════════════════════════════════════════════════════════════
    row = 1
    ws.merge_range(row, 1, row, 11,
                   "Job Search Pipeline", header_fmt)
    row += 1
    ws.merge_range(row, 1, row, 11,
                   f"Dashboard generated {datetime.now().strftime('%B %d, %Y at %H:%M')}   |   "
                   f"{len(all_jobs)} total positions tracked   |   "
                   f"{len(rec_pis)} PI recommendations",
                   subtitle_fmt)

    # ══════════════════════════════════════════════════════════════════
    # ROW 3-5: KPI CARDS
    # ══════════════════════════════════════════════════════════════════
    row = 4
    kpi_data = [
        (1,  kpi_blue,    len(all_jobs), "Total Positions"),
        (4,  kpi_gold,    us_count,      "US Positions"),
        (7,  kpi_emerald, eu_count,      "EU Positions"),
        (10, kpi_rose,    asia_count + other_count, "Asia & Other"),
    ]

    for col_start, (top_f, big_f, lbl_f), value, label in kpi_data:
        ws.merge_range(row, col_start, row, col_start + 1, "", top_f)
        ws.merge_range(row + 1, col_start, row + 1, col_start + 1, value, big_f)
        ws.merge_range(row + 2, col_start, row + 2, col_start + 1, label, lbl_f)

    # ══════════════════════════════════════════════════════════════════
    # ROW 8-9: SECONDARY KPI CARDS
    # ══════════════════════════════════════════════════════════════════
    row = 8

    kpi_navy = _make_kpi_card(NAVY)
    kpi_accent = _make_kpi_card(ACCENT_DARK)

    t1_count = tier_counts.get(1, 0) + tier_counts.get(2, 0)
    t3_count = tier_counts.get(3, 0) + tier_counts.get(4, 0)

    secondary_kpis = [
        (1,  kpi_navy,    len(rec_pis),   "PI Recommendations"),
        (4,  kpi_accent,  t1_count,       "Tier 1-2 Positions"),
        (7,  kpi_emerald, t3_count,       "Tier 3-4 Positions"),
        (10, kpi_rose,    urgent_count,   "Urgent (≤7 days)"),
    ]

    for col_start, (top_f, big_f, lbl_f), value, label in secondary_kpis:
        ws.merge_range(row, col_start, row, col_start + 1, "", top_f)
        ws.merge_range(row + 1, col_start, row + 1, col_start + 1, value, big_f)
        ws.merge_range(row + 2, col_start, row + 2, col_start + 1, label, lbl_f)

    # ══════════════════════════════════════════════════════════════════
    # ROW 12: REGION DATA (hidden, for chart) + REGION PIE CHART
    # ══════════════════════════════════════════════════════════════════
    chart_data_row = 12
    ws.merge_range(chart_data_row, 1, chart_data_row, 2,
                   "Region Breakdown", section_title_fmt)
    chart_data_row += 1

    region_data = [
        ("US", us_count),
        ("EU", eu_count),
        ("Asia", asia_count),
        ("Other", other_count),
    ]
    region_colors = [ACCENT, EMERALD, GOLD, SLATE]

    ws.write(chart_data_row, 1, "Region", tbl_header_fmt)
    ws.write(chart_data_row, 2, "Count", tbl_header_fmt)
    chart_data_row += 1
    region_start = chart_data_row
    for i, (region, count) in enumerate(region_data):
        row_f = tbl_row_alt_fmt if i % 2 else tbl_row_fmt
        num_f = tbl_num_alt_fmt if i % 2 else tbl_num_fmt
        ws.write(chart_data_row, 1, region, row_f)
        ws.write(chart_data_row, 2, count, num_f)
        chart_data_row += 1
    region_end = chart_data_row - 1

    if any(c > 0 for _, c in region_data):
        chart = workbook.add_chart({"type": "doughnut"})
        chart.add_series({
            "name": "Jobs by Region",
            "categories": ["Summary Dashboard", region_start, 1, region_end, 1],
            "values": ["Summary Dashboard", region_start, 2, region_end, 2],
            "data_labels": {"percentage": True, "category": True,
                            "font": {"name": "Calibri", "size": 10, "color": TEXT_PRIMARY}},
            "points": [{"fill": {"color": c}} for c in region_colors],
        })
        chart.set_title({"name": "Jobs by Region",
                         "name_font": {"name": "Calibri", "size": 12,
                                       "color": NAVY, "bold": True}})
        chart.set_size({"width": 380, "height": 280})
        chart.set_legend({"position": "bottom",
                          "font": {"name": "Calibri", "size": 9}})
        chart.set_chartarea({"fill": {"color": LIGHT_BG}, "border": {"none": True}})
        chart.set_plotarea({"fill": {"color": LIGHT_BG}, "border": {"none": True}})
        ws.insert_chart("E13", chart)

    # ══════════════════════════════════════════════════════════════════
    # ROW 12: TIER BREAKDOWN TABLE (right side)
    # ══════════════════════════════════════════════════════════════════
    tier_row = 12
    ws.merge_range(tier_row, 8, tier_row, 9,
                   "Tier Distribution", section_title_fmt)
    tier_row += 1
    ws.write(tier_row, 8, "Tier", tbl_header_fmt)
    ws.write(tier_row, 9, "Count", tbl_header_fmt)
    tier_row += 1

    tier_labels = [
        (1, "Tier 1 (Top 50)"),
        (2, "Tier 2 (51-200)"),
        (3, "Tier 3 (201-500)"),
        (4, "Tier 4 (501+)"),
    ]
    tier_data_start = tier_row
    for i, (t, label) in enumerate(tier_labels):
        count = tier_counts.get(t, 0)
        t_fmt = tier_fmts.get(t, tbl_row_fmt)
        num_f = tbl_num_alt_fmt if i % 2 else tbl_num_fmt
        ws.write(tier_row, 8, label, t_fmt)
        ws.write(tier_row, 9, count, num_f)
        tier_row += 1

    # Unranked row
    ranked_total = sum(tier_counts.get(t, 0) for t in (1, 2, 3, 4))
    unranked = len(all_jobs) - ranked_total
    ws.write(tier_row, 8, "Unranked", tbl_row_alt_fmt)
    ws.write(tier_row, 9, unranked, tbl_num_alt_fmt)
    tier_row += 1

    # Deadline urgency mini-table
    tier_row += 1
    ws.merge_range(tier_row, 8, tier_row, 9,
                   "Deadline Urgency", section_title_fmt)
    tier_row += 1
    ws.write(tier_row, 8, "Window", tbl_header_fmt)
    ws.write(tier_row, 9, "Count", tbl_header_fmt)
    tier_row += 1

    urgency_data = [
        ("≤ 7 days (Urgent)", urgent_count),
        ("8-30 days (Soon)", soon_count),
        ("> 30 days", later_count),
        ("No deadline", no_deadline),
    ]

    urgent_fmt = workbook.add_format({
        "bold": True, "font_size": 10, "font_color": "#991B1B",
        "bg_color": "#FEE2E2", "valign": "vcenter",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })
    soon_fmt = workbook.add_format({
        "bold": True, "font_size": 10, "font_color": "#92400E",
        "bg_color": "#FEF3C7", "valign": "vcenter",
        "left": 1, "right": 1, "bottom": 1,
        "left_color": SUBTLE_BORDER, "right_color": SUBTLE_BORDER,
        "bottom_color": SUBTLE_BORDER,
    })
    urgency_fmts = [urgent_fmt, soon_fmt, tbl_row_fmt, tbl_row_alt_fmt]
    for i, ((label, count), uf) in enumerate(zip(urgency_data, urgency_fmts)):
        num_f = tbl_num_alt_fmt if i % 2 else tbl_num_fmt
        ws.write(tier_row, 8, label, uf)
        ws.write(tier_row, 9, count, num_f)
        tier_row += 1

    # ══════════════════════════════════════════════════════════════════
    # ROW ~20: TOP FIELDS BAR CHART + TABLE
    # ══════════════════════════════════════════════════════════════════
    fields_row = max(chart_data_row, tier_row) + 2
    ws.merge_range(fields_row, 1, fields_row, 2,
                   "Top Research Fields", section_title_fmt)
    fields_row += 1

    ws.write(fields_row, 1, "Field", tbl_header_fmt)
    ws.write(fields_row, 2, "Count", tbl_header_fmt)
    fields_row += 1
    field_start = fields_row
    for i, (field, count) in enumerate(top_fields):
        row_f = tbl_row_alt_fmt if i % 2 else tbl_row_fmt
        num_f = tbl_num_alt_fmt if i % 2 else tbl_num_fmt
        ws.write(fields_row, 1, field[:40], row_f)
        ws.write(fields_row, 2, count, num_f)
        fields_row += 1
    field_end = fields_row - 1

    if top_fields:
        chart2 = workbook.add_chart({"type": "bar"})
        gradient_colors = [
            "#1E3A5F", "#1E40AF", "#2563EB", "#3B82F6", "#60A5FA",
            "#93C5FD", "#BFDBFE", "#DBEAFE", "#EFF6FF", "#F0F9FF",
        ]
        chart2.add_series({
            "name": "Jobs by Field",
            "categories": ["Summary Dashboard", field_start, 1, field_end, 1],
            "values": ["Summary Dashboard", field_start, 2, field_end, 2],
            "points": [{"fill": {"color": gradient_colors[i % len(gradient_colors)]}}
                       for i in range(len(top_fields))],
            "gap": 80,
        })
        chart2.set_title({"name": "Top Research Fields",
                          "name_font": {"name": "Calibri", "size": 12,
                                        "color": NAVY, "bold": True}})
        chart2.set_size({"width": 520, "height": 320})
        chart2.set_legend({"none": True})
        chart2.set_chartarea({"fill": {"color": LIGHT_BG}, "border": {"none": True}})
        chart2.set_plotarea({"fill": {"color": LIGHT_BG}, "border": {"none": True}})
        chart2.set_y_axis({"num_font": {"name": "Calibri", "size": 9, "color": TEXT_SECONDARY}})
        chart2.set_x_axis({"num_font": {"name": "Calibri", "size": 9, "color": TEXT_SECONDARY},
                           "major_gridlines": {"visible": True, "line": {"color": SUBTLE_BORDER}}})
        ws.insert_chart(field_start, 4, chart2)

    # ══════════════════════════════════════════════════════════════════
    # TOP INSTITUTIONS TABLE (right of fields chart)
    # ══════════════════════════════════════════════════════════════════
    inst_row = fields_row - len(top_fields) - 2  # align with fields section
    ws.merge_range(inst_row, 8, inst_row, 9,
                   "Top Institutions", section_title_fmt)
    inst_row += 1

    ws.write(inst_row, 8, "Institution", tbl_header_fmt)
    ws.write(inst_row, 9, "Jobs", tbl_header_fmt)
    inst_row += 1
    for i, (inst, count) in enumerate(top_institutions):
        row_f = tbl_row_alt_fmt if i % 2 else tbl_row_fmt
        num_f = tbl_num_alt_fmt if i % 2 else tbl_num_fmt
        ws.write(inst_row, 8, inst[:35], row_f)
        ws.write(inst_row, 9, count, num_f)
        inst_row += 1

    # ══════════════════════════════════════════════════════════════════
    # FOOTER
    # ══════════════════════════════════════════════════════════════════
    footer_row = max(fields_row, inst_row) + 2
    footer_fmt = workbook.add_format({
        "font_size": 9, "font_color": TEXT_MUTED,
        "font_name": "Calibri", "bg_color": LIGHT_BG,
        "top": 1, "top_color": SUBTLE_BORDER,
    })
    ws.merge_range(footer_row, 1, footer_row, 11,
                   f"Generated by Job Search Pipeline   |   "
                   f"Data covers {len(all_jobs)} positions across "
                   f"{len(set(j.get('country') for j in all_jobs if j.get('country')))} countries   |   "
                   f"{datetime.now().strftime('%Y-%m-%d %H:%M')}",
                   footer_fmt)

    # Fill background for empty areas
    for r in range(footer_row + 1):
        for empty_col in (0, 3, 6, 9, 12):
            if empty_col < 13:
                ws.write_blank(r, empty_col, "", bg_fmt)


def _is_excluded(job: dict) -> bool:
    """Return True if the job matches any EXCLUDE_KEYWORDS (neuroscience etc)."""
    blob = " ".join(
        (job.get(k) or "") for k in ("title", "field", "keywords", "description")
    ).lower()
    return any(kw.lower() in blob for kw in EXCLUDE_KEYWORDS)


def _build_institute_rank() -> dict[str, int]:
    """Build lowercase institute -> ranking position within its tier.

    Lower number = higher ranked.  Institutions not in the rankings
    get a large default value.
    """
    from src.config import load_rankings
    rankings = load_rankings()
    rank: dict[str, int] = {}
    pos = 0
    for tier_key in ("1", "2", "3", "4"):
        info = rankings.get("tiers", {}).get(tier_key, {})
        for inst in info.get("institutions", []):
            rank[inst.lower()] = pos
            pos += 1
    # Companies
    companies = rankings.get("companies", {})
    for section in ("top_companies", "companies"):
        sec = companies.get(section, {})
        insts = sec.get("institutions", []) if isinstance(sec, dict) else sec
        for inst in insts:
            rank[inst.lower()] = pos
            pos += 1
    # Aliases
    aliases = rankings.get("tier_lookup_aliases", {})
    for alias, canonical in aliases.items():
        if canonical.lower() in rank and alias.lower() not in rank:
            rank[alias.lower()] = rank[canonical.lower()]
    return rank


_INSTITUTE_RANK: dict[str, int] | None = None


def _get_institute_rank(institute: str) -> int:
    """Return the ranking position for an institute (lower = better)."""
    global _INSTITUTE_RANK
    if _INSTITUTE_RANK is None:
        _INSTITUTE_RANK = _build_institute_rank()
    key = institute.strip().lower()
    if key in _INSTITUTE_RANK:
        return _INSTITUTE_RANK[key]
    # Partial match
    for name, pos in _INSTITUTE_RANK.items():
        if name in key or key in name:
            return pos
    return 99999


def _sort_by_tier(jobs: list[dict]) -> list[dict]:
    """Sort jobs by tier, then by institution ranking within each tier.

    Sort order: Company → T1 → T2 → T3 → T4 → Unranked
    Within each tier: institution ranking order (descending).
    """
    def _key(j: dict):
        tier = j.get("tier")
        tier_num = tier if isinstance(tier, int) else 999
        company = is_company(j.get("institute") or "")
        # Companies sort at the top (before T1)
        if company:
            sort_tier = 0
        else:
            sort_tier = tier_num
        inst_rank = _get_institute_rank(j.get("institute") or "")
        return (sort_tier, inst_rank)
    return sorted(jobs, key=_key)


def _sort_pis_by_tier(pis: list[dict]) -> list[dict]:
    """Sort recommended PIs: Company first, then by tier and institution ranking.

    Sort order: Company → T1 → T2 → T3 → T4 → Unranked (no tier)
    Within each tier: institution ranking order.
    """
    def _key(p: dict):
        tier = p.get("tier")
        tier_num = tier if isinstance(tier, int) and 1 <= tier <= 4 else 999
        company = is_company(p.get("institute") or "")
        if company:
            sort_tier = 0
        else:
            sort_tier = tier_num
        inst_rank = _get_institute_rank(p.get("institute") or "")
        return (sort_tier, inst_rank)
    return sorted(pis, key=_key)


def _find_previous_excel(output_dir: Path) -> Path | None:
    """Find the most recent previous Excel file to preserve user edits."""
    import glob as _glob

    # Fixed name first
    fixed = output_dir / "JobSearch_Auto.xlsx"
    if fixed.exists():
        return fixed
    # Fallback: dated files
    pattern = str(output_dir / "JobSearch_Auto_*.xlsx")
    files = sorted(_glob.glob(pattern), key=lambda f: Path(f).stat().st_mtime, reverse=True)
    return Path(files[0]) if files else None


def _read_previous_excel(output_dir: Path) -> dict[str, dict[str, pd.DataFrame]]:
    """Read previous Excel into DataFrames keyed by sheet name.

    Returns {sheet_name: DataFrame} for job sheets, preserving all user edits.
    Returns empty dict if no previous file exists.
    """
    prev_path = _find_previous_excel(output_dir)
    if not prev_path:
        return {}

    sheets: dict[str, pd.DataFrame] = {}
    for sheet in ("US Positions", "EU Positions", "Other Positions"):
        try:
            df = pd.read_excel(str(prev_path), sheet_name=sheet, engine="openpyxl")
            sheets[sheet] = df
        except Exception:
            continue
    return sheets


def _detect_dismissed_urls(
    prev_sheets: dict[str, pd.DataFrame],
    exportable_urls: set[str],
) -> set[str]:
    """Detect URLs that the user removed from the previous Excel.

    Compares previously exported URLs (from DB exported_at) against
    what's still in the Excel. Missing URLs = user-dismissed.
    Records them in the dismissed_urls table.
    """
    from src.db import get_connection, dismiss_urls

    # Collect all Job URLs currently in the Excel
    prev_urls: set[str] = set()
    for df in prev_sheets.values():
        if "Job URL" not in df.columns:
            continue
        for url in df["Job URL"].dropna():
            url = str(url).strip()
            if url and url != "nan" and url.startswith("http"):
                prev_urls.add(url)

    if not prev_urls:
        return set()

    # Sync user status changes from Excel → DB
    with get_connection() as conn:
        for df in prev_sheets.values():
            if "Job URL" not in df.columns or "Status" not in df.columns:
                continue
            for _, row in df.iterrows():
                url = str(row.get("Job URL", ""))
                status = row.get("Status", "")
                if url and url.startswith("http") and pd.notna(status):
                    status = str(status).strip()
                    if status and status not in ("new", "nan"):
                        conn.execute(
                            "UPDATE jobs SET status = ? WHERE url = ?",
                            (status, url),
                        )

    # Detect dismissed: previously exported but removed from Excel
    dismissed: set[str] = set()
    with get_connection() as conn:
        exported_rows = conn.execute(
            "SELECT url FROM jobs WHERE exported_at IS NOT NULL "
            "AND (status IS NULL OR status NOT IN ('dismissed'))"
        ).fetchall()

        for row in exported_rows:
            url = row["url"]
            if url in exportable_urls and url not in prev_urls:
                dismissed.add(url)

    # Persist to dismissed_urls table (permanent blacklist)
    if dismissed:
        dismiss_urls(list(dismissed))
        logger.info("Detected %d user-dismissed jobs from previous Excel", len(dismissed))

    return dismissed


def _merge_with_previous(
    prev_sheets: dict[str, pd.DataFrame],
    new_jobs_by_region: dict[str, list[dict]],
    dismissed_urls: set[str],
) -> dict[str, tuple[pd.DataFrame, list[dict]]]:
    """Merge previous Excel data with new jobs.

    Preserves all existing rows from the previous Excel (user edits intact).
    Only adds genuinely new jobs (URL not already present).
    Returns {sheet_name: (merged_df, new_job_dicts)} for sorting.
    """
    sheet_region_map = {
        "US Positions": "US",
        "EU Positions": "EU",
        "Other Positions": "Other",
    }

    result: dict[str, tuple[pd.DataFrame, list[dict]]] = {}

    for sheet_name, region_key in sheet_region_map.items():
        prev_df = prev_sheets.get(sheet_name)
        new_jobs = new_jobs_by_region.get(region_key, [])

        # Collect existing URLs from previous Excel
        existing_urls: set[str] = set()
        if prev_df is not None and "Job URL" in prev_df.columns:
            for url in prev_df["Job URL"].dropna():
                url_str = str(url).strip()
                if url_str and url_str != "nan":
                    existing_urls.add(url_str)

        # Filter new jobs: only those not already in Excel and not dismissed
        genuinely_new = [
            j for j in new_jobs
            if j.get("url")
            and j["url"] not in existing_urls
            and j["url"] not in dismissed_urls
        ]

        if prev_df is not None and len(prev_df) > 0:
            # Remove dismissed URLs from previous data too
            if dismissed_urls and "Job URL" in prev_df.columns:
                prev_df = prev_df[~prev_df["Job URL"].isin(dismissed_urls)]

            # Ensure columns match
            if genuinely_new:
                new_rows_df = pd.DataFrame(
                    [_job_to_row(j) for j in genuinely_new], columns=JOB_COLUMNS
                )
                # Align columns: use the union of both, preserving prev columns
                all_cols = list(prev_df.columns)
                for col in new_rows_df.columns:
                    if col not in all_cols:
                        all_cols.append(col)

                # Reindex both to same columns
                prev_df = prev_df.reindex(columns=all_cols, fill_value="")
                new_rows_df = new_rows_df.reindex(columns=all_cols, fill_value="")
                merged_df = pd.concat([prev_df, new_rows_df], ignore_index=True)
            else:
                merged_df = prev_df
        elif genuinely_new:
            merged_df = pd.DataFrame(
                [_job_to_row(j) for j in genuinely_new], columns=JOB_COLUMNS
            )
        else:
            merged_df = pd.DataFrame(columns=JOB_COLUMNS)

        result[sheet_name] = (merged_df, genuinely_new)

    return result


def _sort_merged_df(df: pd.DataFrame) -> pd.DataFrame:
    """Sort a merged DataFrame by Tier then Institute ranking."""
    if df.empty:
        return df

    # Sort by: Company first → T1 → T2 → T3 → T4 → Unranked
    # Within each tier: by institute ranking
    def _tier_sort_key(tier_val):
        tier_str = str(tier_val).strip() if pd.notna(tier_val) else ""
        if tier_str == "Company":
            return 0
        if tier_str.startswith("T") and len(tier_str) == 2 and tier_str[1].isdigit():
            return int(tier_str[1])
        return 99

    def _inst_sort_key(inst_val):
        inst = str(inst_val).strip() if pd.notna(inst_val) else ""
        return _get_institute_rank(inst)

    if "Tier" in df.columns and "Institute" in df.columns:
        df = df.assign(
            _tier_sort=df["Tier"].apply(_tier_sort_key),
            _inst_sort=df["Institute"].apply(_inst_sort_key),
        )
        df = df.sort_values(["_tier_sort", "_inst_sort"], ascending=True)
        df = df.drop(columns=["_tier_sort", "_inst_sort"])
        df = df.reset_index(drop=True)

    return df


def _mark_exported(urls: list[str]) -> None:
    """Set exported_at timestamp for all exported job URLs."""
    if not urls:
        return
    from src.db import get_connection
    with get_connection() as conn:
        conn.executemany(
            "UPDATE jobs SET exported_at = datetime('now') WHERE url = ?",
            [(u,) for u in urls],
        )


def export_to_excel(output_dir: Path = None) -> Path:
    """Export job data to a multi-sheet Excel file with incremental updates.

    Incremental strategy:
    1. Read the previous Excel to preserve all user edits (notes, reordering, etc.)
    2. Detect user deletions → record in dismissed_urls (permanent blacklist)
    3. Only add genuinely NEW jobs (not already in Excel, not dismissed)
    4. Re-sort the combined data by tier and institute ranking
    5. Write the merged result

    This ensures the user's manual work is never destroyed.
    """
    output_dir = output_dir or EXCEL_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    filepath = output_dir / "JobSearch_Auto.xlsx"

    # Phase 1: Read previous Excel (preserves user edits)
    prev_sheets = _read_previous_excel(output_dir)

    # Phase 2: Detect user deletions from previous Excel
    raw_pre = get_jobs(limit=10000)
    _skip_statuses = {"dismissed", "merged"}
    pre_urls = {j.get("url") for j in raw_pre
                if j.get("url") and not _is_excluded(j) and j.get("status") not in _skip_statuses}
    dismissed = _detect_dismissed_urls(prev_sheets, pre_urls)

    # Also include all permanently dismissed URLs
    from src.db import get_dismissed_urls
    all_dismissed = get_dismissed_urls() | dismissed

    # Phase 3: Load all non-excluded, non-dismissed jobs from DB
    raw_jobs = get_jobs(limit=10000)
    all_jobs = [j for j in raw_jobs
                if not _is_excluded(j)
                and j.get("status") not in _skip_statuses
                and j.get("url") not in all_dismissed]

    logger.info("Filtered %d → %d jobs (excluded %d by keywords, %d dismissed)",
                len(raw_jobs), len(all_jobs),
                len(raw_jobs) - len(all_jobs) - len(all_dismissed), len(all_dismissed))

    # Phase 4: Split new jobs by region
    us_jobs = _sort_by_tier([j for j in all_jobs if j.get("region") == "US"])
    eu_jobs = _sort_by_tier([j for j in all_jobs if j.get("region") == "EU"])
    other_jobs = _sort_by_tier([j for j in all_jobs if j.get("region") not in ("US", "EU")])

    new_jobs_by_region = {
        "US": us_jobs,
        "EU": eu_jobs,
        "Other": other_jobs,
    }

    # Phase 5: Merge with previous Excel data
    if prev_sheets:
        merged = _merge_with_previous(prev_sheets, new_jobs_by_region, all_dismissed)
        # Sort the merged DataFrames
        df_us = _sort_merged_df(merged.get("US Positions", (pd.DataFrame(columns=JOB_COLUMNS), []))[0])
        df_eu = _sort_merged_df(merged.get("EU Positions", (pd.DataFrame(columns=JOB_COLUMNS), []))[0])
        df_other = _sort_merged_df(merged.get("Other Positions", (pd.DataFrame(columns=JOB_COLUMNS), []))[0])

        new_count = sum(len(m[1]) for m in merged.values())
        logger.info("Incremental merge: %d new jobs added, preserving existing rows", new_count)
    else:
        # First run: generate from scratch
        df_us = pd.DataFrame([_job_to_row(j) for j in us_jobs], columns=JOB_COLUMNS)
        df_eu = pd.DataFrame([_job_to_row(j) for j in eu_jobs], columns=JOB_COLUMNS)
        df_other = pd.DataFrame([_job_to_row(j) for j in other_jobs], columns=JOB_COLUMNS)

    rec_pis = _sort_pis_by_tier(get_recommended_pis())

    with pd.ExcelWriter(str(filepath), engine="xlsxwriter") as writer:
        # Sheet 0: Summary Dashboard (charts + statistics)
        _write_summary_dashboard(writer, all_jobs, rec_pis)

        # Sheet 1: US Positions
        df_us.to_excel(writer, sheet_name="US Positions", index=False)
        if len(df_us) > 0:
            _style_worksheet(writer, "US Positions", df_us)
            _style_deadline_cells(writer, "US Positions", df_us)
            # Paper links and citizenship styling only for fresh rows
            if not prev_sheets:
                _write_paper_links(writer, "US Positions", df_us, us_jobs)
                _style_citizenship_cells(writer, "US Positions", df_us, us_jobs)

        # Sheet 2: EU Positions
        df_eu.to_excel(writer, sheet_name="EU Positions", index=False)
        if len(df_eu) > 0:
            _style_worksheet(writer, "EU Positions", df_eu)
            _style_deadline_cells(writer, "EU Positions", df_eu)
            if not prev_sheets:
                _write_paper_links(writer, "EU Positions", df_eu, eu_jobs)
                _style_citizenship_cells(writer, "EU Positions", df_eu, eu_jobs)

        # Sheet 3: Other Positions
        df_other.to_excel(writer, sheet_name="Other Positions", index=False)
        if len(df_other) > 0:
            _style_worksheet(writer, "Other Positions", df_other)
            _style_deadline_cells(writer, "Other Positions", df_other)
            if not prev_sheets:
                _write_paper_links(writer, "Other Positions", df_other, other_jobs)
                _style_citizenship_cells(writer, "Other Positions", df_other, other_jobs)

        # Sheet 4: PI Recommendations
        df_rec = pd.DataFrame([_pi_to_row(p) for p in rec_pis], columns=PI_COLUMNS)
        df_rec.to_excel(writer, sheet_name="PI Recommendations", index=False)
        if len(df_rec) > 0:
            _style_worksheet(writer, "PI Recommendations", df_rec)

    # Mark all exported jobs so we can detect user deletions next time
    exported_urls = [j.get("url") for j in all_jobs if j.get("url")]
    _mark_exported(exported_urls)

    total_rows = len(df_us) + len(df_eu) + len(df_other)
    logger.info("Excel exported to %s (%d total rows)", filepath, total_rows)
    return filepath
