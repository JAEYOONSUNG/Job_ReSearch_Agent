"""Export job data to Excel files with parsed, structured columns."""

import json
import logging
import re
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import EXCEL_OUTPUT_DIR
from src.db import get_all_pis, get_jobs, get_recommended_pis
from src.matching.job_parser import parse_structured_description

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

    Flattens all newlines into spaces for compact single-line display.
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
    # Replace all newlines with spaces
    text = text.replace("\n", " ").replace("\r", " ")
    # Collapse all whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len]


def _format_papers(papers_json: str | None) -> str:
    """Format JSON paper list to '(Year) Title [N cites]; ...' display string."""
    if not papers_json:
        return ""
    try:
        papers = json.loads(papers_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    if not papers:
        return ""
    parts = []
    for p in papers:
        year = p.get("year") or "?"
        title = (p.get("title") or "Untitled")[:80]
        cites = p.get("citation_count", 0)
        parts.append(f"({year}) {title} [{cites} cites]")
    return "; ".join(parts)


def _clean_list(text: str | None, max_len: int = 400) -> str:
    """Clean a list-style section (requirements, etc).

    Flattens bullets into semicolon-separated items for compact display.
    """
    if not text:
        return ""
    # Remove markdown bold/italic
    text = re.sub(r"\*{1,3}(.+?)\*{1,3}", r"\1", text)
    # Remove HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
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
    # Description
    "Description",
    # PI Papers
    "Recent Papers (5)",
    "Top Cited Papers (5)",
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
        "Institute": job.get("institute") or "",
        "Department": job.get("department") or "",
        "Tier": f"T{tier}" if tier else "",
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
        "Conditions (Full)": _clean_list(cond, 400),
        # Dates
        "Posted Date": job.get("posted_date") or "",
        "Deadline": job.get("deadline") or "",
        # Description
        "Description": _clean_text(desc, 1500),
        # PI Papers
        "Recent Papers (5)": _format_papers(job.get("recent_papers")),
        "Top Cited Papers (5)": _format_papers(job.get("top_cited_papers")),
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
        "Recent Papers (5)": 50, "Top Cited Papers (5)": 50,
        "Job URL": 15, "Lab URL": 15, "Scholar URL": 15, "Dept URL": 15,
        "Match Score": 8, "Source": 12, "Status": 7,
    }

    url_fmt = workbook.add_format({"font_color": "blue", "underline": True, "valign": "vcenter"})
    default_fmt = workbook.add_format({"valign": "vcenter"})

    for i, col in enumerate(df.columns):
        width = width_map.get(col, 15)
        if col in ("Job URL", "Lab URL", "Scholar URL", "Dept URL"):
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
        # T1 = gold, T2 = blue, T3 = green
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


def _write_summary_dashboard(
    writer: pd.ExcelWriter,
    all_jobs: list[dict],
    rec_pis: list[dict],
) -> None:
    """Write a Summary Dashboard sheet with statistics and charts."""
    workbook = writer.book
    worksheet = workbook.add_worksheet("Summary Dashboard")
    writer.sheets["Summary Dashboard"] = worksheet

    # Formats
    title_fmt = workbook.add_format({
        "bold": True, "font_size": 16, "font_color": "#16213e",
        "bottom": 2, "bottom_color": "#16213e",
    })
    section_fmt = workbook.add_format({
        "bold": True, "font_size": 12, "font_color": "#16213e",
        "bg_color": "#f0f4f8",
    })
    label_fmt = workbook.add_format({"bold": True, "valign": "vcenter"})
    value_fmt = workbook.add_format({"valign": "vcenter", "num_format": "#,##0"})
    pct_fmt = workbook.add_format({"valign": "vcenter", "num_format": "0.0%"})

    # Title
    worksheet.merge_range("A1:F1", "Job Search Pipeline - Summary Dashboard", title_fmt)
    worksheet.write("A2", f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # --- Region breakdown ---
    row = 3
    worksheet.write(row, 0, "Region Breakdown", section_fmt)
    worksheet.write(row, 1, "", section_fmt)
    worksheet.write(row, 2, "", section_fmt)
    row += 1

    from collections import Counter
    region_counts = Counter(j.get("region", "Other") for j in all_jobs)
    region_data = [
        ("US", region_counts.get("US", 0)),
        ("EU", region_counts.get("EU", 0)),
        ("Asia", region_counts.get("Asia", 0)),
        ("Other", region_counts.get("Other", 0)),
    ]
    worksheet.write(row, 0, "Region", label_fmt)
    worksheet.write(row, 1, "Count", label_fmt)
    row += 1
    data_start = row
    for region, count in region_data:
        worksheet.write(row, 0, region)
        worksheet.write(row, 1, count, value_fmt)
        row += 1
    data_end = row - 1

    # Pie chart for regions
    if any(c > 0 for _, c in region_data):
        chart = workbook.add_chart({"type": "pie"})
        chart.add_series({
            "name": "Jobs by Region",
            "categories": ["Summary Dashboard", data_start, 0, data_end, 0],
            "values": ["Summary Dashboard", data_start, 1, data_end, 1],
            "data_labels": {"percentage": True, "category": True},
        })
        chart.set_title({"name": "Jobs by Region"})
        chart.set_size({"width": 400, "height": 300})
        worksheet.insert_chart("D4", chart)

    # --- Top fields ---
    row += 1
    worksheet.write(row, 0, "Top Fields", section_fmt)
    worksheet.write(row, 1, "", section_fmt)
    worksheet.write(row, 2, "", section_fmt)
    row += 1

    field_counts = Counter(
        j.get("field", "Unknown") for j in all_jobs if j.get("field")
    )
    top_fields = field_counts.most_common(10)

    worksheet.write(row, 0, "Field", label_fmt)
    worksheet.write(row, 1, "Count", label_fmt)
    row += 1
    field_start = row
    for field, count in top_fields:
        worksheet.write(row, 0, field[:40])
        worksheet.write(row, 1, count, value_fmt)
        row += 1
    field_end = row - 1

    # Bar chart for top fields
    if top_fields:
        chart2 = workbook.add_chart({"type": "bar"})
        chart2.add_series({
            "name": "Jobs by Field",
            "categories": ["Summary Dashboard", field_start, 0, field_end, 0],
            "values": ["Summary Dashboard", field_start, 1, field_end, 1],
            "fill": {"color": "#16213e"},
        })
        chart2.set_title({"name": "Top Research Fields"})
        chart2.set_size({"width": 500, "height": 350})
        chart2.set_legend({"none": True})
        worksheet.insert_chart("D16", chart2)

    # --- Key metrics ---
    row += 1
    worksheet.write(row, 0, "Key Metrics", section_fmt)
    worksheet.write(row, 1, "", section_fmt)
    row += 1
    worksheet.write(row, 0, "Total Jobs", label_fmt)
    worksheet.write(row, 1, len(all_jobs), value_fmt)
    row += 1
    worksheet.write(row, 0, "Total PI Recommendations", label_fmt)
    worksheet.write(row, 1, len(rec_pis), value_fmt)
    row += 1
    tier_counts = Counter(j.get("tier") for j in all_jobs if j.get("tier"))
    for tier in sorted(tier_counts.keys()):
        worksheet.write(row, 0, f"Tier {tier} Jobs", label_fmt)
        worksheet.write(row, 1, tier_counts[tier], value_fmt)
        row += 1

    # Set column widths
    worksheet.set_column(0, 0, 25)
    worksheet.set_column(1, 1, 12)
    worksheet.set_column(2, 2, 5)


def export_to_excel(output_dir: Path = None) -> Path:
    """Export all job data to a multi-sheet Excel file with charts and formatting."""
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
        # Sheet 0: Summary Dashboard (charts + statistics)
        _write_summary_dashboard(writer, all_jobs, rec_pis)

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
