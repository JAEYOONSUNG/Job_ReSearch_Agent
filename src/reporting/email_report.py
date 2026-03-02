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


def _enrich_job_for_email(job: dict) -> dict:
    """Fill missing institute/country/tier info for email display.

    Tries to resolve missing fields using DB data and scorer heuristics
    so the email shows as much info as possible.
    """
    enriched = dict(job)

    # Fill country from institute if missing
    if not enriched.get("country") and enriched.get("institute"):
        try:
            from src.matching.scorer import guess_country_from_institute
            country = guess_country_from_institute(enriched["institute"])
            if country:
                enriched["country"] = country
        except Exception:
            pass

    # Fill tier from institute if missing
    if not enriched.get("tier") and enriched.get("institute"):
        try:
            from src.matching.scorer import get_institution_tier
            tier = get_institution_tier(enriched["institute"])
            if tier and tier < 5:
                enriched["tier"] = tier
        except Exception:
            pass

    # Fill region from country if missing
    if not enriched.get("region") and enriched.get("country"):
        try:
            from src.matching.scorer import get_region
            enriched["region"] = get_region(enriched["country"])
        except Exception:
            pass

    return enriched


def render_report(jobs: list[dict], recommendations: list[dict] = None) -> str:
    """Render HTML email report from jobs and PI recommendations.

    Includes weekly trend statistics and summary dashboard.
    Enriches job data to fill missing institute/country/tier info.
    """
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)))
    template = env.get_template("report.html")

    # Enrich jobs to fill missing info for display
    jobs = [_enrich_job_for_email(j) for j in jobs]

    us_jobs = [j for j in jobs if j.get("region") == "US"]
    eu_jobs = [j for j in jobs if j.get("region") == "EU"]
    korea_jobs = [j for j in jobs if j.get("region") == "Korea"]
    asia_jobs = [j for j in jobs if j.get("region") == "Asia"]
    other_jobs = [j for j in jobs if j.get("region") not in ("US", "EU", "Korea", "Asia")]

    trends = _compute_weekly_trends()

    return template.render(
        date=datetime.now().strftime("%b %d, %Y"),
        total_new=len(jobs),
        us_count=len(us_jobs),
        eu_count=len(eu_jobs),
        korea_count=len(korea_jobs),
        asia_count=len(asia_jobs),
        other_count=len(other_jobs),
        us_jobs=us_jobs,
        eu_jobs=eu_jobs,
        korea_jobs=korea_jobs,
        asia_jobs=asia_jobs,
        other_jobs=other_jobs,
        recommendations=recommendations or [],
        rec_count=len(recommendations or []),
        total_sources=13,
        trends=trends,
    )


def build_subject(jobs: list[dict], recommendations: list[dict] = None) -> str:
    """Build email subject line."""
    date_str = datetime.now().strftime("%b %d")
    us = sum(1 for j in jobs if j.get("region") == "US")
    eu = sum(1 for j in jobs if j.get("region") == "EU")
    korea = sum(1 for j in jobs if j.get("region") == "Korea")
    asia = sum(1 for j in jobs if j.get("region") in ("Asia", "Other"))

    parts = []
    if us:
        parts.append(f"{us} US")
    if eu:
        parts.append(f"{eu} EU")
    if korea:
        parts.append(f"{korea} Korea")
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


def _is_relevant(job: dict) -> bool:
    """Return True if the job passes field relevance filters.

    Excludes jobs that:
    - Match any EXCLUDE_KEYWORDS in title/field/keywords/description
    - Match EXCLUDE_TITLE_KEYWORDS in title (non-researcher positions)
    - Have match_score=0 AND no relevant field detected
    """
    from src.config import EXCLUDE_KEYWORDS, EXCLUDE_TITLE_KEYWORDS, FACULTY_TITLE_KEYWORDS, GARBAGE_TITLE_PATTERNS

    blob = " ".join(
        (job.get(k) or "") for k in ("title", "field", "keywords", "description")
    ).lower()

    # Hard exclude: irrelevant fields
    if any(kw.lower() in blob for kw in EXCLUDE_KEYWORDS):
        return False

    # Title-only exclusion: non-researcher positions
    title = (job.get("title") or "").lower()
    if any(kw.lower() in title for kw in EXCLUDE_TITLE_KEYWORDS):
        if not any(k in title for k in ("postdoc", "post-doc", "postdoctoral",
                                         "post-doctoral", "research fellow")):
            return False

    # Faculty: exclude from non-Korea regions
    if any(kw.lower() in title for kw in FACULTY_TITLE_KEYWORDS):
        if job.get("region") != "Korea":
            if not any(k in title for k in ("postdoc", "post-doc", "postdoctoral",
                                             "post-doctoral", "research fellow")):
                return False

    # Garbage: title too short or clearly not a job posting
    if len(title.strip()) < 10:
        return False
    if any(pat in title for pat in GARBAGE_TITLE_PATTERNS):
        return False

    # Soft filter: if no CV keywords matched at all, require at least a
    # plausible field before including in email
    score = job.get("match_score") or 0
    if score == 0:
        # Allow jobs from known relevant institutions regardless of score
        inst = (job.get("institute") or "").lower()
        tier = job.get("tier")
        if tier and tier <= 2:
            return True
        # Otherwise require at least some field relevance signal
        relevant_signals = [
            "biology", "biolog", "biotech", "crispr", "genomic", "genetic",
            "protein", "enzyme", "microbio", "synthetic", "metabol",
            "molecular", "bioengineer", "cell-free", "ferment",
            "bioinformat", "biochem", "genome", "gene edit",
            "high-throughput", "directed evolution", "extremophile",
        ]
        if not any(sig in blob for sig in relevant_signals):
            return False

    return True


def send_report(
    since: str,
    include_recommendations: bool = True,
    attach_excel: bool = True,
) -> bool:
    """Generate and send the full report with optional Excel attachment.

    Applies field relevance filtering so only jobs related to the user's
    research area (synthetic biology, CRISPR, protein engineering, etc.)
    are included in the email.

    Parameters
    ----------
    since : str
        ISO datetime string; only jobs discovered after this are included.
    include_recommendations : bool
        Whether to include PI recommendation section.
    attach_excel : bool
        Whether to generate and attach an Excel export file.
    """
    raw_jobs = get_new_jobs_since(since)
    if not raw_jobs:
        logger.info("No new jobs since %s, skipping email", since)
        return False

    # Filter out irrelevant fields (physics, engineering, clinical, etc.)
    jobs = [j for j in raw_jobs if _is_relevant(j)]
    logger.info("Email relevance filter: %d → %d jobs (excluded %d irrelevant)",
                len(raw_jobs), len(jobs), len(raw_jobs) - len(jobs))

    if not jobs:
        logger.info("No relevant jobs after filtering, skipping email")
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
