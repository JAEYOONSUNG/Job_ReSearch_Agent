"""Parse CV/resume to extract keywords for job matching."""

import json
import logging
import re
from pathlib import Path
from typing import Optional

from src.config import CV_KEYWORDS, CV_KEYWORDS_PATH

logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from a PDF file."""
    try:
        import subprocess

        result = subprocess.run(
            ["pdftotext", "-layout", pdf_path, "-"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            return result.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    # Fallback: try PyPDF2
    try:
        from PyPDF2 import PdfReader

        reader = PdfReader(pdf_path)
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except ImportError:
        logger.warning("Neither pdftotext nor PyPDF2 available for PDF parsing")
        return ""


def extract_text_from_docx(docx_path: str) -> str:
    """Extract text from a DOCX file."""
    try:
        from docx import Document

        doc = Document(docx_path)
        return "\n".join(p.text for p in doc.paragraphs)
    except ImportError:
        logger.warning("python-docx not installed for DOCX parsing")
        return ""


def extract_keywords_from_text(text: str) -> list[str]:
    """Extract matching keywords from text based on CV_KEYWORDS."""
    text_lower = text.lower()
    found = []
    for kw in CV_KEYWORDS:
        if kw.lower() in text_lower:
            found.append(kw)
    return found


def extract_keywords_from_cv(cv_path: Optional[str] = None) -> list[str]:
    """Extract keywords from a CV file. If no path, use default CV_KEYWORDS."""
    if cv_path is None:
        logger.info("No CV path provided, using default keywords")
        return CV_KEYWORDS

    path = Path(cv_path)
    if not path.exists():
        logger.warning("CV file not found: %s", cv_path)
        return CV_KEYWORDS

    if path.suffix.lower() == ".pdf":
        text = extract_text_from_pdf(str(path))
    elif path.suffix.lower() in (".docx", ".doc"):
        text = extract_text_from_docx(str(path))
    elif path.suffix.lower() == ".txt":
        text = path.read_text()
    else:
        logger.warning("Unsupported CV format: %s", path.suffix)
        return CV_KEYWORDS

    keywords = extract_keywords_from_text(text)
    if not keywords:
        logger.warning("No keywords found in CV, falling back to defaults")
        return CV_KEYWORDS

    # Cache extracted keywords
    try:
        CV_KEYWORDS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(CV_KEYWORDS_PATH, "w") as f:
            json.dump(keywords, f, indent=2)
        logger.info("Cached %d CV keywords to %s", len(keywords), CV_KEYWORDS_PATH)
    except OSError as e:
        logger.warning("Failed to cache CV keywords: %s", e)

    return keywords


def load_cached_keywords() -> list[str]:
    """Load previously cached CV keywords."""
    if CV_KEYWORDS_PATH.exists():
        try:
            with open(CV_KEYWORDS_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return CV_KEYWORDS
