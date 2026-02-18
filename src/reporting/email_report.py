"""Generate and send email reports with weekly trend statistics."""

import logging
import smtplib
from collections import Counter
from datetime import datetime, timedelta
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from src.config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD, REPORT_RECIPIENTS
from src.db import get_connection, get_new_jobs_since, get_recommended_pis

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def _compute_weekly_trends() -> dict:
    """Compute weekly trend statistics from the database.

    Returns a dict with:
    - ``this_week``: job count for the current week
    - ``last_week``: job count for the previous week
    - ``change_pct``: percentage change (positive = growth)
    - ``top_fields``: list of (field, count) tuples, top 5
    - ``top_countries``: list of (country, count) tuples, top 5
    - ``new_pis_this_week``: number of PIs discovered this week
    """
    now = datetime.now()
    this_week_start = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    last_week_start = (now - timedelta(days=14)).strftime("%Y-%m-%d")
    last_week_end = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    with get_connection() as conn:
        # This week count
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM jobs WHERE discovered_at >= ?",
            (this_week_start,),
        ).fetchone()
        this_week = row["cnt"] if row else 0

        # Last week count
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM jobs "
            "WHERE discovered_at >= ? AND discovered_at < ?",
            (last_week_start, last_week_end),
        ).fetchone()
        last_week = row["cnt"] if row else 0

        # Top fields this week
        rows = conn.execute(
            "SELECT field, COUNT(*) AS cnt FROM jobs "
            "WHERE discovered_at >= ? AND field IS NOT NULL AND field != '' "
            "GROUP BY field ORDER BY cnt DESC LIMIT 5",
            (this_week_start,),
        ).fetchall()
        top_fields = [(r["field"], r["cnt"]) for r in rows]

        # Top countries this week
        rows = conn.execute(
            "SELECT country, COUNT(*) AS cnt FROM jobs "
            "WHERE discovered_at >= ? AND country IS NOT NULL AND country != '' "
            "GROUP BY country ORDER BY cnt DESC LIMIT 5",
            (this_week_start,),
        ).fetchall()
        top_countries = [(r["country"], r["cnt"]) for r in rows]

        # New PIs this week
        row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM pis WHERE created_at >= ?",
            (this_week_start,),
        ).fetchone()
        new_pis = row["cnt"] if row else 0

    change_pct = 0.0
    if last_week > 0:
        change_pct = ((this_week - last_week) / last_week) * 100

    return {
        "this_week": this_week,
        "last_week": last_week,
        "change_pct": round(change_pct, 1),
        "top_fields": top_fields,
        "top_countries": top_countries,
        "new_pis_this_week": new_pis,
    }


def render_report(jobs: list[dict], recommendations: list[dict] = None) -> str:
    """Render HTML email report from jobs and PI recommendations.

    Includes weekly trend statistics and summary dashboard.
    """
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("report.html")

    us_jobs = [j for j in jobs if j.get("region") == "US"]
    eu_jobs = [j for j in jobs if j.get("region") == "EU"]
    asia_jobs = [j for j in jobs if j.get("region") == "Asia"]
    other_jobs = [j for j in jobs if j.get("region") not in ("US", "EU", "Asia")]

    trends = _compute_weekly_trends()

    return template.render(
        date=datetime.now().strftime("%b %d, %Y"),
        total_new=len(jobs),
        us_count=len(us_jobs),
        eu_count=len(eu_jobs),
        asia_count=len(asia_jobs),
        other_count=len(other_jobs),
        us_jobs=us_jobs,
        eu_jobs=eu_jobs,
        asia_jobs=asia_jobs,
        other_jobs=other_jobs,
        recommendations=recommendations or [],
        rec_count=len(recommendations or []),
        total_sources=12,
        trends=trends,
    )


def build_subject(jobs: list[dict], recommendations: list[dict] = None) -> str:
    """Build email subject line."""
    date_str = datetime.now().strftime("%b %d")
    us = sum(1 for j in jobs if j.get("region") == "US")
    eu = sum(1 for j in jobs if j.get("region") == "EU")
    asia = sum(1 for j in jobs if j.get("region") in ("Asia", "Other"))

    parts = []
    if us:
        parts.append(f"{us} US")
    if eu:
        parts.append(f"{eu} EU")
    if asia:
        parts.append(f"{asia} Asia")

    region_str = ", ".join(parts) if parts else "0"
    subject = f"[JobSearch] {date_str} - {len(jobs)} new postdocs ({region_str})"
    if recommendations:
        subject += f" + {len(recommendations)} PI recommendations"
    return subject


def send_email(
    subject: str,
    html_body: str,
    recipients: list[str] = None,
    attachments: list[Path] = None,
) -> bool:
    """Send HTML email via Gmail SMTP with optional file attachments.

    Parameters
    ----------
    subject : str
        Email subject line.
    html_body : str
        HTML content for the email body.
    recipients : list[str], optional
        Override default recipients from config.
    attachments : list[Path], optional
        File paths to attach to the email.
    """
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.error("Gmail credentials not configured. Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD.")
        return False

    recipients = recipients or REPORT_RECIPIENTS
    if not recipients:
        logger.error("No recipients configured")
        return False

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ", ".join(recipients)

    # HTML body as an alternative sub-part
    html_part = MIMEText(html_body, "html")
    msg.attach(html_part)

    # Attach files
    for filepath in (attachments or []):
        if not filepath.exists():
            logger.warning("Attachment not found, skipping: %s", filepath)
            continue
        try:
            part = MIMEBase("application", "octet-stream")
            with open(filepath, "rb") as f:
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={filepath.name}",
            )
            msg.attach(part)
            logger.info("Attached file: %s", filepath.name)
        except Exception:
            logger.exception("Failed to attach file: %s", filepath)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, recipients, msg.as_string())
        logger.info("Email sent to %s", ", ".join(recipients))
        return True
    except smtplib.SMTPException as e:
        logger.error("Failed to send email: %s", e)
        return False


def send_report(
    since: str,
    include_recommendations: bool = True,
    attach_excel: bool = True,
) -> bool:
    """Generate and send the full report with optional Excel attachment.

    Parameters
    ----------
    since : str
        ISO datetime string; only jobs discovered after this are included.
    include_recommendations : bool
        Whether to include PI recommendation section.
    attach_excel : bool
        Whether to generate and attach an Excel export file.
    """
    jobs = get_new_jobs_since(since)
    if not jobs:
        logger.info("No new jobs since %s, skipping email", since)
        return False

    recommendations = get_recommended_pis(min_score=0.5) if include_recommendations else []

    html = render_report(jobs, recommendations)
    subject = build_subject(jobs, recommendations)

    attachments: list[Path] = []
    if attach_excel:
        try:
            from src.reporting.excel_export import export_to_excel
            excel_path = export_to_excel()
            attachments.append(excel_path)
            logger.info("Excel report generated for attachment: %s", excel_path)
        except Exception:
            logger.exception("Failed to generate Excel attachment, sending without it")

    return send_email(subject, html, attachments=attachments)
