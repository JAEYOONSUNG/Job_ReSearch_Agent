"""Shared fixtures for job-search-pipeline tests."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Sample job dicts (realistic postdoc postings)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_job_synbio():
    return {
        "title": "Postdoctoral Research Associate - Synthetic Biology",
        "pi_name": "Ahmed Badran",
        "institute": "Scripps Research Institute",
        "department": "Chemistry",
        "country": "United States",
        "region": "US",
        "field": "Synthetic Biology",
        "description": (
            "A postdoctoral position is available in the lab of Dr. Ahmed Badran "
            "at the Scripps Research Institute. The project focuses on directed evolution "
            "of CRISPR-Cas systems for synthetic biology applications. Candidates should "
            "have experience with protein engineering, high-throughput screening, and "
            "molecular cloning. Salary: $60,000 per year. Duration: 2-3 years."
        ),
        "url": "https://example.com/jobs/synbio-1",
        "source": "academicjobsonline",
        "h_index": 35,
        "citations": 5000,
    }


@pytest.fixture
def sample_job_crispr():
    return {
        "title": "Post-Doc Research Fellow: CRISPR Gene Editing",
        "pi_name": "Feng Zhang",
        "institute": "Massachusetts Institute of Technology",
        "department": "Biological Engineering",
        "country": "United States",
        "region": "US",
        "field": "Genome Engineering",
        "description": (
            "The Zhang Lab at MIT is seeking a postdoctoral fellow to work on "
            "next-generation CRISPR-Cas9 and Cas12 systems for genome engineering. "
            "Requirements: PhD in molecular biology, biochemistry, or related field. "
            "Experience with gene editing, cell culture, and animal models preferred. "
            "We offer competitive salary and benefits."
        ),
        "url": "https://example.com/jobs/crispr-1",
        "source": "nature_careers",
        "h_index": 120,
        "citations": 80000,
    }


@pytest.fixture
def sample_job_extremophile():
    return {
        "title": "Postdoctoral Researcher - Extremophile Enzymology",
        "pi_name": "Joanna Basta-Kaim",
        "institute": "Max Planck Institute for Terrestrial Microbiology",
        "department": "Biochemistry",
        "country": "Germany",
        "region": "EU",
        "field": "Extremophile Biology",
        "description": (
            "A postdoc position is available in the group of Prof. Dr. Joanna Basta-Kaim "
            "studying thermophilic archaea and their enzyme systems. The project involves "
            "metagenomics, protein engineering, and directed evolution of extremozymes. "
            "Deadline: March 15, 2026. Salary: TV-L E13."
        ),
        "url": "https://example.com/jobs/extremophile-1",
        "source": "euraxess",
    }


@pytest.fixture
def sample_job_no_pi():
    return {
        "title": "Research Associate in Machine Learning for Drug Discovery",
        "pi_name": None,
        "institute": "University of Cambridge",
        "department": "Chemistry",
        "country": "United Kingdom",
        "region": "EU",
        "field": "Computational Biology / AI",
        "description": (
            "We are seeking a postdoctoral research associate to develop machine learning "
            "models for drug discovery. The candidate should have a PhD in computational "
            "biology, bioinformatics, or computer science. Experience with deep learning "
            "frameworks (PyTorch, TensorFlow) and molecular simulations is required."
        ),
        "url": "https://example.com/jobs/ml-drug-1",
        "source": "jobs_ac_uk",
    }


@pytest.fixture
def sample_jobs(sample_job_synbio, sample_job_crispr, sample_job_extremophile, sample_job_no_pi):
    """Return a list of all sample jobs."""
    return [sample_job_synbio, sample_job_crispr, sample_job_extremophile, sample_job_no_pi]


# ---------------------------------------------------------------------------
# Test database fixture (in-memory or temp file)
# ---------------------------------------------------------------------------

@pytest.fixture
def test_db_path(tmp_path):
    """Provide a temporary database path."""
    return tmp_path / "test_jobs.db"


@pytest.fixture
def test_db(test_db_path):
    """Initialize a test database and patch DB_PATH to point to it."""
    with patch("src.config.DB_PATH", test_db_path), \
         patch("src.db.DB_PATH", test_db_path):
        from src.db import init_db
        init_db()
        yield test_db_path


# ---------------------------------------------------------------------------
# Rankings fixture (mock institution_rankings.json)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_rankings():
    return {
        "tiers": {
            "1": {
                "institutions": [
                    "MIT", "Stanford University", "Harvard University",
                    "Caltech", "University of Cambridge",
                ]
            },
            "2": {
                "institutions": [
                    "Max Planck", "ETH Zurich", "UC Berkeley",
                    "University of Oxford", "Yale University",
                ]
            },
            "3": {
                "institutions": [
                    "Scripps Research", "University of Michigan",
                    "University of Wisconsin", "KAIST",
                ]
            },
        }
    }


# ---------------------------------------------------------------------------
# Job posting text fixtures (for parser tests)
# ---------------------------------------------------------------------------

@pytest.fixture
def posting_with_pi_lab():
    return (
        "Postdoctoral Position in Synthetic Biology\n\n"
        "A postdoctoral position is available in the laboratory of Dr. Ahmed Badran "
        "at the Scripps Research Institute. The project focuses on directed evolution "
        "of CRISPR systems.\n\n"
        "Requirements:\n"
        "- PhD in biology, chemistry, or related field\n"
        "- Experience with molecular cloning and protein engineering\n\n"
        "We offer:\n"
        "- Salary: $60,000-$70,000 per annum\n"
        "- Duration: 2 years, renewable\n\n"
        "Application deadline: March 15, 2026\n"
        "Contact Dr. Ahmed Badran at badran@scripps.edu"
    )


@pytest.fixture
def posting_with_pi_supervision():
    return (
        "Postdoc in Genome Engineering\n\n"
        "The Department of Biological Engineering at MIT is hiring a postdoctoral "
        "researcher to work on CRISPR-Cas systems under the supervision of "
        "Prof. Feng Zhang.\n\n"
        "The successful candidate will develop new gene editing tools for "
        "therapeutic applications.\n\n"
        "Qualifications:\n"
        "- PhD in molecular biology, biochemistry, or bioengineering\n"
        "- Strong publication record\n\n"
        "Deadline: 2026-04-01"
    )


@pytest.fixture
def posting_with_deadline_variants():
    return (
        "Postdoctoral Fellow - Protein Engineering\n\n"
        "Applications must be received by April 30, 2026.\n\n"
        "A postdoctoral position is available in the Smith Lab at Stanford University."
    )


@pytest.fixture
def posting_german_salary():
    return (
        "Postdoc Position in Microbiology\n\n"
        "We offer a full-time position at the Max Planck Institute.\n\n"
        "Salary: TV-L E13\n"
        "Duration: 3 years\n"
        "Start date: October 1, 2026\n\n"
        "Closing date: 15/06/2026"
    )
