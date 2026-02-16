"""Generate and send email reports."""

import logging
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from src.config import GMAIL_ADDRESS, GMAIL_APP_PASSWORD, REPORT_RECIPIENTS
from src.db import get_new_jobs_since, get_recommended_pis

logger = logging.getLogger(__name__)

TEMPLATE_DIR = Path(__file__).parent / "templates"


def render_report(jobs: list[dict], recommendations: list[dict] = None) -> str:
    """Render HTML email report from jobs and PI recommendations."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("report.html")

    us_jobs = [j for j in jobs if j.get("region") == "US"]
    eu_jobs = [j for j in jobs if j.get("region") == "EU"]
    asia_jobs = [j for j in jobs if j.get("region") == "Asia"]
    other_jobs = [j for j in jobs if j.get("region") not in ("US", "EU", "Asia")]

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


def send_email(subject: str, html_body: str, recipients: list[str] = None) -> bool:
    """Send HTML email via Gmail SMTP."""
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD:
        logger.error("Gmail credentials not configured. Set GMAIL_ADDRESS and GMAIL_APP_PASSWORD.")
        return False

    recipients = recipients or REPORT_RECIPIENTS
    if not recipients:
        logger.error("No recipients configured")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, recipients, msg.as_string())
        logger.info("Email sent to %s", ", ".join(recipients))
        return True
    except smtplib.SMTPException as e:
        logger.error("Failed to send email: %s", e)
        return False


def send_report(since: str, include_recommendations: bool = True) -> bool:
    """Generate and send the full report."""
    jobs = get_new_jobs_since(since)
    if not jobs:
        logger.info("No new jobs since %s, skipping email", since)
        return False

    recommendations = get_recommended_pis(min_score=0.5) if include_recommendations else []

    html = render_report(jobs, recommendations)
    subject = build_subject(jobs, recommendations)
    return send_email(subject, html)
