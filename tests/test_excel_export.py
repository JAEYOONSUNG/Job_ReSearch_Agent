"""Tests for src.reporting.excel_export."""

from unittest.mock import patch

from openpyxl import load_workbook

from src.db import dismiss_urls, get_connection, upsert_job
from src.reporting.excel_export import export_to_excel


def _sheet_urls(workbook_path, sheet_name: str) -> list[str]:
    """Return all non-empty Job URL values from a worksheet."""
    wb = load_workbook(workbook_path, read_only=True)
    try:
        ws = wb[sheet_name]
        headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
        url_idx = headers.index("Job URL")
        return [
            row[url_idx]
            for row in ws.iter_rows(min_row=2, values_only=True)
            if row[url_idx]
        ]
    finally:
        wb.close()


def test_full_refresh_excludes_dismissed_jobs(
    test_db, tmp_path, sample_job_synbio, sample_job_crispr
):
    """Full refresh should preserve and exclude user-dismissed jobs."""
    kept_job = {**sample_job_synbio, "status": "new"}
    dismissed_job = {**sample_job_crispr, "status": "new"}

    upsert_job(kept_job)
    upsert_job(dismissed_job)
    dismiss_urls([dismissed_job["url"]])

    with patch("src.reporting.excel_export._is_excluded", return_value=False):
        workbook_path = export_to_excel(output_dir=tmp_path, full_refresh=True)

    exported_urls = _sheet_urls(workbook_path, "US Positions")
    assert kept_job["url"] in exported_urls
    assert dismissed_job["url"] not in exported_urls

    with get_connection() as conn:
        status = conn.execute(
            "SELECT status FROM jobs WHERE url = ?",
            (dismissed_job["url"],),
        ).fetchone()["status"]
        assert status == "dismissed"
