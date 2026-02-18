"""Tests for src/matching/dedup.py — duplicate detection and merging."""

import pytest

from src.matching.dedup import (
    normalize_title,
    normalize_institute,
    similarity,
    is_duplicate,
    _pick_best,
    deduplicate_jobs,
)


# ===== normalize_title =====

class TestNormalizeTitle:
    def test_lowercase_and_strip(self):
        # "postdoc position" matches prefix removal: "^postdoc\s+(position|...)" -> ""
        assert normalize_title("  Postdoc Position  ") == ""

    def test_collapse_whitespace(self):
        # "research" (not "researcher") does not match the prefix pattern,
        # so the full string remains after postdoc unification
        assert normalize_title("Postdoc   Research   Associate") == "postdoc research associate"

    def test_unify_postdoc_variants(self):
        """All postdoc variants normalize the same way."""
        variants = [
            "Postdoctoral Research Associate - Synthetic Biology",
            "Post-Doc Research Associate - Synthetic Biology",
            "Post Doc Research Associate - Synthetic Biology",
            "postdoc Research Associate - Synthetic Biology",
        ]
        results = [normalize_title(v) for v in variants]
        # All should strip the "postdoc research associate" prefix
        assert all("synthetic biology" in r for r in results)

    def test_remove_research_prefix(self):
        result = normalize_title("Research Associate: CRISPR")
        assert result == "crispr"

    def test_empty_and_none(self):
        assert normalize_title("") == ""
        assert normalize_title(None) == ""

    def test_no_prefix_match(self):
        """Titles without common prefixes pass through."""
        result = normalize_title("Lab Manager - Biology Department")
        assert "lab manager" in result


# ===== normalize_institute =====

class TestNormalizeInstitute:
    def test_uc_shortening(self):
        result = normalize_institute("University of California, Berkeley")
        assert result == "uc berkeley"

    def test_mit(self):
        result = normalize_institute("Massachusetts Institute of Technology")
        assert result == "mit"

    def test_caltech(self):
        result = normalize_institute("California Institute of Technology")
        assert result == "caltech"

    def test_passthrough(self):
        result = normalize_institute("Max Planck Institute")
        assert result == "max planck institute"

    def test_empty_and_none(self):
        assert normalize_institute("") == ""
        assert normalize_institute(None) == ""


# ===== similarity =====

class TestSimilarity:
    def test_identical_strings(self):
        assert similarity("hello", "hello") == 1.0

    def test_completely_different(self):
        assert similarity("abc", "xyz") < 0.5

    def test_empty_string(self):
        assert similarity("", "hello") == 0.0
        assert similarity("hello", "") == 0.0
        assert similarity("", "") == 0.0

    def test_none_input(self):
        assert similarity(None, "hello") == 0.0
        assert similarity("hello", None) == 0.0

    def test_similar_strings(self):
        score = similarity("synthetic biology lab", "synthetic biology laboratory")
        assert score > 0.8


# ===== is_duplicate =====

class TestIsDuplicate:
    def test_same_url(self):
        """Same URL is always a duplicate."""
        a = {"url": "https://example.com/job/1", "title": "A", "institute": "X"}
        b = {"url": "https://example.com/job/1", "title": "B", "institute": "Y"}
        assert is_duplicate(a, b) is True

    def test_different_url_different_content(self):
        a = {
            "url": "https://example.com/job/1",
            "title": "Postdoc in Neuroscience",
            "institute": "Harvard University",
        }
        b = {
            "url": "https://example.com/job/2",
            "title": "Software Engineer",
            "institute": "Google",
        }
        assert is_duplicate(a, b) is False

    def test_same_pi_same_institute(self):
        """Same PI at same institute with similar title = duplicate."""
        a = {
            "url": "https://nature.com/job/1",
            "title": "Postdoc in Synthetic Biology",
            "institute": "MIT",
            "pi_name": "Feng Zhang",
        }
        b = {
            "url": "https://indeed.com/job/2",
            "title": "Post-Doctoral Fellow Synthetic Biology",
            "institute": "Massachusetts Institute of Technology",
            "pi_name": "feng zhang",
        }
        assert is_duplicate(a, b) is True

    def test_same_institute_similar_title(self):
        """Same institute + similar normalized title = duplicate."""
        a = {
            "url": "https://a.com/1",
            "title": "Postdoc Research Associate - CRISPR Gene Editing",
            "institute": "Stanford University",
        }
        b = {
            "url": "https://b.com/2",
            "title": "Post-Doc Research Associate: CRISPR Gene Editing",
            "institute": "Stanford University",
        }
        assert is_duplicate(a, b) is True

    def test_high_combined_similarity(self):
        """High combined title + institute similarity above threshold."""
        a = {
            "url": "https://a.com/1",
            "title": "Postdoc in Protein Engineering and Directed Evolution",
            "institute": "University of California, Berkeley",
        }
        b = {
            "url": "https://b.com/2",
            "title": "Postdoc: Protein Engineering & Directed Evolution",
            "institute": "UC Berkeley",
        }
        assert is_duplicate(a, b) is True

    def test_no_url_match(self):
        """No URL = cannot match via URL path."""
        a = {"title": "Postdoc A", "institute": "MIT"}
        b = {"title": "Postdoc A", "institute": "MIT"}
        # No URL, but same inst + similar title
        assert is_duplicate(a, b) is True

    def test_empty_url_not_duplicate(self):
        """Empty URLs should not match as duplicate via URL."""
        a = {"url": "", "title": "Postdoc A", "institute": "MIT"}
        b = {"url": "", "title": "Postdoc B", "institute": "Harvard"}
        assert is_duplicate(a, b) is False

    def test_different_pi_different_institute(self):
        """Different PI at different institute = not duplicate."""
        a = {
            "url": "https://a.com/1",
            "title": "Postdoc in Synthetic Biology",
            "institute": "MIT",
            "pi_name": "Feng Zhang",
        }
        b = {
            "url": "https://b.com/2",
            "title": "Postdoc in Synthetic Biology",
            "institute": "Stanford University",
            "pi_name": "Christina Smolke",
        }
        assert is_duplicate(a, b) is False

    def test_custom_threshold(self):
        """Lower threshold catches more duplicates."""
        a = {
            "url": "https://a.com/1",
            "title": "Postdoc Synthetic Biology",
            "institute": "MIT",
        }
        b = {
            "url": "https://b.com/2",
            "title": "Postdoc Systems Biology",
            "institute": "MIT",
        }
        # With default threshold these might not match
        # With a very low threshold they should
        assert is_duplicate(a, b, threshold=0.3) is True


# ===== _pick_best =====

class TestPickBest:
    def test_keeps_longer_description(self):
        a = {"title": "Job A", "description": "Short", "pi_name": "Alice Smith"}
        b = {"title": "Job B", "description": "Much longer description with more detail", "pi_name": None}
        result = _pick_best(a, b)
        assert result["description"] == b["description"]

    def test_merges_missing_fields(self):
        a = {"title": "Job A", "description": "Long description here", "pi_name": None, "deadline": "2026-03-15"}
        b = {"title": "Job B", "description": "Short", "pi_name": "John Smith", "deadline": None}
        result = _pick_best(a, b)
        assert result["description"] == a["description"]
        assert result["pi_name"] == "John Smith"
        assert result["deadline"] == "2026-03-15"

    def test_merge_does_not_overwrite(self):
        """Fields in the 'best' entry are not overwritten."""
        a = {"title": "Job A", "description": "Longer description here", "pi_name": "Alice Smith", "field": "SynBio"}
        b = {"title": "Job B", "description": "Short", "pi_name": "Bob Jones", "field": "CRISPR"}
        result = _pick_best(a, b)
        assert result["pi_name"] == "Alice Smith"
        assert result["field"] == "SynBio"

    def test_returns_new_dict(self):
        """Result is a new dict, not modifying originals."""
        a = {"title": "A", "description": "Long text here now", "pi_name": None}
        b = {"title": "B", "description": "Short", "pi_name": "Name"}
        result = _pick_best(a, b)
        assert result is not a
        assert result is not b


# ===== deduplicate_jobs =====

class TestDeduplicateJobs:
    def test_no_duplicates(self, sample_jobs):
        """All distinct jobs survive dedup."""
        result = deduplicate_jobs(sample_jobs)
        assert len(result) == len(sample_jobs)

    def test_url_duplicates_removed(self):
        jobs = [
            {"url": "https://example.com/1", "title": "Job 1", "institute": "MIT", "description": "Short"},
            {"url": "https://example.com/1", "title": "Job 1 (copy)", "institute": "MIT", "description": "Longer description here"},
        ]
        result = deduplicate_jobs(jobs)
        assert len(result) == 1
        assert result[0]["description"] == "Longer description here"

    def test_title_duplicates_removed(self):
        jobs = [
            {"url": "https://a.com/1", "title": "Postdoc in CRISPR Gene Editing", "institute": "Stanford University", "description": "A"},
            {"url": "https://b.com/2", "title": "Post-Doc in CRISPR Gene Editing", "institute": "Stanford University", "description": "B"},
        ]
        result = deduplicate_jobs(jobs)
        assert len(result) == 1

    def test_empty_list(self):
        assert deduplicate_jobs([]) == []

    def test_single_job(self):
        jobs = [{"url": "https://example.com/1", "title": "Job", "institute": "MIT"}]
        result = deduplicate_jobs(jobs)
        assert len(result) == 1

    def test_merges_complementary_fields(self):
        """When deduplicating, fields from both entries are merged."""
        jobs = [
            {
                "url": "https://a.com/1",
                "title": "Postdoc Synthetic Biology",
                "institute": "MIT",
                "description": "Long description with lots of detail about the position",
                "pi_name": "John Smith",
                "deadline": None,
            },
            {
                "url": "https://b.com/2",
                "title": "Postdoc Synthetic Biology",
                "institute": "MIT",
                "description": "Short",
                "pi_name": None,
                "deadline": "2026-06-01",
            },
        ]
        result = deduplicate_jobs(jobs)
        assert len(result) == 1
        assert result[0]["pi_name"] == "John Smith"
        assert result[0]["deadline"] == "2026-06-01"

    def test_preserves_order_of_first_seen(self):
        """The first occurrence's position is preserved."""
        jobs = [
            {"url": "https://unique.com/1", "title": "Job A", "institute": "Harvard", "description": ""},
            {"url": "https://dup.com/1", "title": "Postdoc SynBio", "institute": "MIT", "description": "Short"},
            {"url": "https://unique.com/2", "title": "Job C", "institute": "Stanford", "description": ""},
            {"url": "https://dup.com/2", "title": "Postdoc SynBio", "institute": "MIT", "description": "Longer text here"},
        ]
        result = deduplicate_jobs(jobs)
        assert len(result) == 3
        assert result[0]["url"] == "https://unique.com/1"
        assert result[2]["url"] == "https://unique.com/2"
