"""Scraper modules for the job search pipeline.

Each scraper inherits from ``BaseScraper`` and implements ``scrape()``.
Use ``ALL_SCRAPERS`` for a convenient list of all available scraper classes.
"""

from src.scrapers.academicpositions import AcademicPositionsScraper
from src.scrapers.base import BaseScraper
from src.scrapers.euraxess import EuraxessScraper
from src.scrapers.jobs_ac_kr import JobsAcKrScraper
from src.scrapers.jobspy_scraper import JobSpyScraper
from src.scrapers.lab_websites import LabWebsiteScraper
from src.scrapers.nature_careers import NatureCareersScraper
from src.scrapers.researchgate import ResearchGateScraper
from src.scrapers.scholarshipdb import ScholarshipDBScraper
from src.scrapers.wanted import WantedScraper

ALL_SCRAPERS: list[type[BaseScraper]] = [
    NatureCareersScraper,
    JobSpyScraper,
    EuraxessScraper,
    JobsAcKrScraper,
    ResearchGateScraper,
    WantedScraper,
    AcademicPositionsScraper,
    ScholarshipDBScraper,
    LabWebsiteScraper,
]

__all__ = [
    "BaseScraper",
    "NatureCareersScraper",
    "JobSpyScraper",
    "EuraxessScraper",
    "JobsAcKrScraper",
    "ResearchGateScraper",
    "WantedScraper",
    "AcademicPositionsScraper",
    "ScholarshipDBScraper",
    "LabWebsiteScraper",
    "ALL_SCRAPERS",
]
