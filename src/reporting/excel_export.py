"""Export job data to Excel files with multiple sheets."""

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.config import EXCEL_OUTPUT_DIR
from src.db import get_all_pis, get_jobs, get_recommended_pis

logger = logging.getLogger(__name__)

JOB_COLUMNS = [
    "PI Name",
    "Institute",
    "Tier",
    "Country",
    "Department",
    "Field",
    "Keywords",
    "Requirements",
    "Conditions",
    "h-index",
    "Citations",
    "Posted Date",
    "Deadline",
    "Description",
    "PI Research",
    "Job URL",
    "Lab URL",
    "Scholar URL",
    "Match Score",
    "Status",
    "Notes",
    "Source",
]

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


def _job_to_row(job: dict) -> dict:
    tier = job.get("tier")
    desc = job.get("description") or ""
    return {
        "PI Name": job.get("pi_name") or "",
        "Institute": job.get("institute") or "",
        "Tier": f"T{tier}" if tier else "",
        "Country": job.get("country") or "",
        "Department": job.get("department") or "",
        "Field": job.get("field") or "",
        "Keywords": job.get("keywords") or "",
        "Requirements": job.get("requirements") or "",
        "Conditions": job.get("conditions") or "",
        "h-index": job.get("h_index") or "",
        "Citations": job.get("citations") or "",
        "Posted Date": job.get("posted_date") or "",
        "Deadline": job.get("deadline") or "",
        "Description": desc[:500] if desc else "",
        "PI Research": job.get("pi_research_summary") or "",
        "Job URL": job.get("url") or "",
        "Lab URL": job.get("lab_url") or "",
        "Scholar URL": job.get("scholar_url") or "",
        "Match Score": job.get("match_score", 0),
        "Status": job.get("status") or "",
        "Notes": job.get("notes") or "",
        "Source": job.get("source") or "",
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
    # Auto-adjust column widths
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).str.len().max() if len(df) > 0 else 0,
            len(col),
        )
        worksheet.set_column(i, i, min(max_len + 2, 50))


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
