"""Export job data to Excel files with parsed, structured columns."""

import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import EXCEL_OUTPUT_DIR
from src.db import get_all_pis, get_jobs, get_recommended_pis

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

    Preserves real newlines so that Excel's text-wrap renders paragraphs.
    """
    if not text:
        return ""
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    # Remove markdown headers
    text = re.sub(r"^#{1,4}\s*", "", text, flags=re.MULTILINE)
    # Remove markdown links but keep text
    text = re.sub(r"\[(.+?)\]\(.+?\)", r"\1", text)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse runs of 3+ newlines to double-newline (paragraph break)
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Collapse horizontal whitespace (spaces/tabs) but keep newlines
    text = re.sub(r"[^\S\n]+", " ", text)
    # Strip each line
    text = "\n".join(line.strip() for line in text.splitlines())
    # Remove leading/trailing whitespace
    text = text.strip()
    return text[:max_len]


def _clean_list(text: str | None, max_len: int = 400) -> str:
    """Clean a list-style section (requirements, etc).

    Preserves bullet-point structure with ``\\n- `` format for Excel readability.
    """
    if not text:
        return ""
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Normalise bullet markers to "- "
    text = re.sub(r"\n\s*[-•*]\s*", "\n- ", text)
    text = re.sub(r"\n\s*\d+[.)]\s*", "\n- ", text)
    # Collapse blank lines but keep single newlines
    text = re.sub(r"\n{2,}", "\n", text)
    # Collapse horizontal whitespace
    text = re.sub(r"[^\S\n]+", " ", text)
    # Strip each line
    text = "\n".join(line.strip() for line in text.splitlines())
    text = text.strip()
    return text[:max_len]


# ---------------------------------------------------------------------------
# Condition parsing — split into sub-fields
# ---------------------------------------------------------------------------

def _parse_salary(conditions: str, description: str) -> str:
    """Extract salary info from conditions or description."""
    blob = f"{conditions} {description}"
    patterns = [
        r"(?:salary|stipend|compensation|remuneration)\s*(?:of|is|:)?\s*([\$€£]?\s*[\d,]+(?:\.\d+)?(?:\s*[-–]\s*[\$€£]?\s*[\d,]+(?:\.\d+)?)?)\s*(?:k|K|per\s+(?:year|annum|month)|/\s*(?:yr|year|annum|month(?:ly)?))?",
        r"([\$€£]\s*[\d,]+(?:\.\d+)?(?:\s*[-–]\s*[\$€£]?\s*[\d,]+(?:\.\d+)?)?)\s*(?:k|K)?\s*(?:per\s+(?:year|annum)|/\s*(?:yr|year|annum|yearly))",
        r"(TV-?L\s*E?\s*1[3-5](?:\s*/\s*E?\s*1[3-5])?)",
        r"(NIH\s+(?:scale|salary))",
        r"(Grade\s+\d+|Band\s+\d+|Scale\s+\d+)",
        r"Salary:\s*([\$€£]?[\d,.]+-?[\$€£]?[\d,.]*(?:/\w+)?)",
    ]
    for p in patterns:
        m = re.search(p, blob, re.IGNORECASE)
        if m:
            return m.group(1).strip() if m.lastindex else m.group(0).strip()
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
    "Title",
    "PI Name",
    "Institute",
    "Department",
    "Tier",
    "Country",
    "Region",
    # Research
    "Field",
    "Keywords",
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
    # Description
    "Description",
    # Links
    "Job URL",
    "Lab URL",
    "Scholar URL",
    "Dept URL",
    # Meta
    "Match Score",
    "Source",
    "Status",
]


def _job_to_row(job: dict) -> dict:
    tier = job.get("tier")
    desc = job.get("description") or ""
    req = job.get("requirements") or ""
    cond = job.get("conditions") or ""

    return {
        "Title": _clean_text(job.get("title"), 100),
        "PI Name": job.get("pi_name") or "",
        "Institute": job.get("institute") or "",
        "Department": job.get("department") or "",
        "Tier": f"T{tier}" if tier else "",
        "Country": job.get("country") or "",
        "Region": job.get("region") or "",
        # Research
        "Field": job.get("field") or "",
        "Keywords": job.get("keywords") or "",
        # Requirements parsed
        "Degree Required": _parse_degree(req, desc),
        "Skills/Techniques": _parse_skills(req, desc),
        "Requirements (Full)": _clean_list(req, 600),
        # Conditions parsed
        "Salary": _parse_salary(cond, desc),
        "Duration": _parse_duration(cond, desc),
        "Contract Type": _parse_contract_type(cond, desc),
        "Start Date": _parse_start_date(cond, desc),
        "Conditions (Full)": _clean_list(cond, 400),
        # Dates
        "Posted Date": job.get("posted_date") or "",
        "Deadline": job.get("deadline") or "",
        # Description
        "Description": _clean_text(desc, 1500),
        # Links
        "Job URL": job.get("url") or "",
        "Lab URL": job.get("lab_url") or "",
        "Scholar URL": job.get("scholar_url") or "",
        "Dept URL": job.get("dept_url") or "",
        # Meta
        "Match Score": job.get("match_score", 0),
        "Source": job.get("source") or "",
        "Status": job.get("status") or "",
    }


def _pi_to_row(pi: dict) -> dict:
    return {
        "PI Name": pi.get("name", ""),
        "Institute": pi.get("institute", ""),
        "Country": pi.get("country", ""),
        "Tier": f"T{pi.get('tier', 4)}",
        "h-index": pi.get("h_index", ""),
        "Citations": pi.get("citations", ""),
        "Fields": pi.get("fields", ""),
        "Connected Seeds": pi.get("connected_seeds", ""),
        "Recommendation Score": pi.get("recommendation_score", 0),
        "Lab URL": pi.get("lab_url", ""),
        "Scholar URL": pi.get("scholar_url", ""),
    }


def _style_worksheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """Apply formatting to a worksheet."""
    worksheet = writer.sheets[sheet_name]
    workbook = writer.book

    # Header format
    header_fmt = workbook.add_format({
        "bold": True,
        "bg_color": "#16213e",
        "font_color": "white",
        "text_wrap": True,
        "valign": "top",
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
        "Degree Required": 30, "Skills/Techniques": 30, "Requirements (Full)": 40,
        "Salary": 18, "Duration": 18, "Contract Type": 12, "Start Date": 14,
        "Conditions (Full)": 30,
        "Posted Date": 11, "Deadline": 11,
        "Description": 50,
        "Job URL": 15, "Lab URL": 15, "Scholar URL": 15, "Dept URL": 15,
        "Match Score": 8, "Source": 12, "Status": 7,
    }

    url_fmt = workbook.add_format({"font_color": "blue", "underline": True, "valign": "top"})
    wrap_fmt = workbook.add_format({"text_wrap": True, "valign": "top"})

    # Columns that benefit from text wrapping
    wrap_cols = {
        "Description", "Requirements (Full)", "Conditions (Full)",
        "Keywords", "Title", "Degree Required", "Skills/Techniques",
        "Field",
    }

    for i, col in enumerate(df.columns):
        width = width_map.get(col, 15)
        if col in ("Job URL", "Lab URL", "Scholar URL", "Dept URL"):
            worksheet.set_column(i, i, width, url_fmt)
        elif col in wrap_cols:
            worksheet.set_column(i, i, width, wrap_fmt)
        else:
            worksheet.set_column(i, i, width)

    # Default row height for better readability with wrapped text
    worksheet.set_default_row(45)

    # Freeze panes: freeze header row + first 3 columns
    worksheet.freeze_panes(1, 3)

    # Auto-filter
    if len(df) > 0:
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)


def export_to_excel(output_dir: Path = None) -> Path:
    """Export all job data to a multi-sheet Excel file."""
    output_dir = output_dir or EXCEL_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"JobSearch_Auto_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    filepath = output_dir / filename

    # Gather data
    all_jobs = get_jobs(limit=10000)
    us_jobs = [j for j in all_jobs if j.get("region") == "US"]
    eu_jobs = [j for j in all_jobs if j.get("region") == "EU"]
    other_jobs = [j for j in all_jobs if j.get("region") not in ("US", "EU")]
    rec_pis = get_recommended_pis()

    with pd.ExcelWriter(str(filepath), engine="xlsxwriter") as writer:
        # Sheet 1: US Positions
        df_us = pd.DataFrame([_job_to_row(j) for j in us_jobs], columns=JOB_COLUMNS)
        df_us.to_excel(writer, sheet_name="US Positions", index=False)
        if len(df_us) > 0:
            _style_worksheet(writer, "US Positions", df_us)

        # Sheet 2: EU Positions
        df_eu = pd.DataFrame([_job_to_row(j) for j in eu_jobs], columns=JOB_COLUMNS)
        df_eu.to_excel(writer, sheet_name="EU Positions", index=False)
        if len(df_eu) > 0:
            _style_worksheet(writer, "EU Positions", df_eu)

        # Sheet 3: Other Positions
        df_other = pd.DataFrame([_job_to_row(j) for j in other_jobs], columns=JOB_COLUMNS)
        df_other.to_excel(writer, sheet_name="Other Positions", index=False)
        if len(df_other) > 0:
            _style_worksheet(writer, "Other Positions", df_other)

        # Sheet 4: PI Recommendations
        df_rec = pd.DataFrame([_pi_to_row(p) for p in rec_pis], columns=PI_COLUMNS)
        df_rec.to_excel(writer, sheet_name="PI Recommendations", index=False)
        if len(df_rec) > 0:
            _style_worksheet(writer, "PI Recommendations", df_rec)

        # Sheet 5: All History
        df_all = pd.DataFrame([_job_to_row(j) for j in all_jobs], columns=JOB_COLUMNS)
        df_all.to_excel(writer, sheet_name="All History", index=False)
        if len(df_all) > 0:
            _style_worksheet(writer, "All History", df_all)

    logger.info("Excel exported to %s (%d jobs)", filepath, len(all_jobs))
    return filepath
