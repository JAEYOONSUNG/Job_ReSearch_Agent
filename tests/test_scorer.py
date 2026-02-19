"""Tests for src/matching/scorer.py — scoring, tier lookup, region mapping."""

import pytest
from unittest.mock import patch

from src.matching.scorer import (
    _normalize,
    keyword_match_score,
    get_institution_tier,
    get_region,
    score_job,
    score_and_sort_jobs,
)


# ===== _normalize =====

class TestNormalize:
    def test_lowercase_and_strip(self):
        assert _normalize("  Hello World  ") == "hello world"

    def test_collapse_whitespace(self):
        assert _normalize("hello   world") == "hello world"


# ===== keyword_match_score =====

class TestKeywordMatchScore:
    def test_perfect_match(self):
        text = "synthetic biology CRISPR protein engineering"
        keywords = ["synthetic biology", "CRISPR", "protein engineering"]
        score = keyword_match_score(text, keywords)
        assert score == 1.0

    def test_partial_match(self):
        text = "Working on CRISPR gene editing systems"
        keywords = ["CRISPR", "protein engineering", "directed evolution", "synthetic biology"]
        score = keyword_match_score(text, keywords)
        assert score == 0.25  # 1 out of 4

    def test_no_match(self):
        text = "Administrative office management position"
        keywords = ["CRISPR", "protein engineering"]
        assert keyword_match_score(text, keywords) == 0.0

    def test_empty_text(self):
        assert keyword_match_score("", ["CRISPR"]) == 0.0

    def test_empty_keywords(self):
        assert keyword_match_score("some text", []) == 0.0

    def test_none_keywords_uses_default(self):
        # When keywords=None, it uses CV_KEYWORDS from config
        text = "synthetic biology and CRISPR research"
        score = keyword_match_score(text, None)
        assert score > 0.0  # Should match at least some CV_KEYWORDS

    def test_case_insensitive(self):
        text = "SYNTHETIC BIOLOGY and crispr"
        keywords = ["synthetic biology", "CRISPR"]
        assert keyword_match_score(text, keywords) == 1.0


# ===== get_institution_tier =====

class TestGetInstitutionTier:
    def test_tier1_institution(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("MIT") == 1
            assert get_institution_tier("Stanford University") == 1
            assert get_institution_tier("Harvard University") == 1

    def test_tier2_institution(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("Max Planck Institute for Biochemistry") == 2
            assert get_institution_tier("ETH Zurich") == 2

    def test_tier3_institution(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("Scripps Research Institute") == 3

    def test_unranked_institution(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("Unknown University") == 5

    def test_empty_institution(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("") == 5
            assert get_institution_tier(None) == 5

    def test_partial_match(self, mock_rankings):
        """Partial string matching should work for institution lookup."""
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert get_institution_tier("University of Cambridge, Department of Chemistry") == 1


# ===== get_region =====

class TestGetRegion:
    @pytest.mark.parametrize("country, expected_region", [
        ("United States", "US"),
        ("USA", "US"),
        ("US", "US"),
        ("Canada", "US"),
        ("Germany", "EU"),
        ("United Kingdom", "EU"),
        ("UK", "EU"),
        ("Switzerland", "EU"),
        ("France", "EU"),
        ("Japan", "Asia"),
        ("South Korea", "Asia"),
        ("China", "Asia"),
        ("Singapore", "Asia"),
        ("Australia", "Other"),
        ("Brazil", "Other"),
    ])
    def test_known_countries(self, country, expected_region):
        assert get_region(country) == expected_region

    def test_unknown_country(self):
        assert get_region("Narnia") == "Other"

    def test_empty_country(self):
        assert get_region("") == "Other"
        assert get_region(None) == "Other"

    def test_partial_match(self):
        # "United States of America" should partial-match "United States"
        assert get_region("United States of America") == "US"


# ===== score_job =====

class TestScoreJob:
    def test_adds_score_fields(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            job = {
                "title": "Postdoc in Synthetic Biology",
                "description": "CRISPR and protein engineering research",
                "field": "Synthetic Biology",
                "pi_name": "John Smith",
                "institute": "MIT",
                "country": "United States",
                "h_index": 50,
            }
            result = score_job(job, keywords=["CRISPR", "protein engineering", "synthetic biology"])
            assert "match_score" in result
            assert "region" in result
            assert "tier" in result
            assert "sort_key" in result
            assert result["match_score"] > 0.0
            assert result["region"] == "US"
            assert result["tier"] == 1

    def test_uses_existing_region(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            job = {
                "title": "Job",
                "description": "",
                "field": "",
                "pi_name": "",
                "institute": "Unknown",
                "region": "EU",
                "country": "",
            }
            result = score_job(job)
            assert result["region"] == "EU"

    def test_uses_existing_tier(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            job = {
                "title": "Job",
                "description": "",
                "field": "",
                "pi_name": "",
                "institute": "Unknown",
                "tier": 2,
                "country": "",
            }
            result = score_job(job)
            assert result["tier"] == 2

    def test_sort_key_structure(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            job = {
                "title": "Postdoc",
                "description": "CRISPR research",
                "field": "",
                "pi_name": "",
                "institute": "MIT",
                "country": "United States",
                "h_index": 50,
            }
            result = score_job(job, keywords=["CRISPR"])
            sort_key = result["sort_key"]
            assert len(sort_key) == 4
            # Region priority for US = 1
            assert sort_key[0] == 1
            # Tier for MIT = 1
            assert sort_key[1] == 1
            # h_index negated
            assert sort_key[2] == -50


# ===== score_and_sort_jobs =====

class TestScoreAndSortJobs:
    def test_sorts_by_priority(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            jobs = [
                {
                    "title": "Job EU",
                    "description": "CRISPR research",
                    "field": "",
                    "pi_name": "",
                    "institute": "Unknown Uni",
                    "country": "Germany",
                    "h_index": 20,
                },
                {
                    "title": "Job US Tier1",
                    "description": "CRISPR research",
                    "field": "",
                    "pi_name": "",
                    "institute": "MIT",
                    "country": "United States",
                    "h_index": 50,
                },
                {
                    "title": "Job Asia",
                    "description": "CRISPR research",
                    "field": "",
                    "pi_name": "",
                    "institute": "Unknown Uni",
                    "country": "Japan",
                    "h_index": 10,
                },
            ]
            result = score_and_sort_jobs(jobs, keywords=["CRISPR"])
            # US jobs should come first, then EU, then Asia
            assert result[0]["country"] == "United States"
            assert result[1]["country"] == "Germany"
            assert result[2]["country"] == "Japan"

    def test_empty_list(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            assert score_and_sort_jobs([]) == []

    def test_tier_ordering_within_region(self, mock_rankings):
        with patch("src.matching.scorer.load_rankings", return_value=mock_rankings):
            jobs = [
                {
                    "title": "Job at Unknown",
                    "description": "CRISPR research",
                    "field": "",
                    "pi_name": "",
                    "institute": "Unknown University",
                    "country": "United States",
                    "h_index": 10,
                },
                {
                    "title": "Job at MIT",
                    "description": "CRISPR research",
                    "field": "",
                    "pi_name": "",
                    "institute": "MIT",
                    "country": "United States",
                    "h_index": 50,
                },
            ]
            result = score_and_sort_jobs(jobs, keywords=["CRISPR"])
            # MIT (tier 1) should come before Unknown (tier 4)
            assert result[0]["institute"] == "MIT"
            assert result[1]["institute"] == "Unknown University"
