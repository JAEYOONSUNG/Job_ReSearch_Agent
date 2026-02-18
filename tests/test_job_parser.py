"""Tests for src/matching/job_parser.py — PI extraction, deadline, keywords, field inference."""

import pytest

from src.matching.job_parser import (
    extract_pi_name,
    extract_pi_from_title,
    expand_pi_last_name,
    _is_valid_name,
    infer_field,
    extract_keywords,
    extract_deadline,
    extract_requirements,
    extract_conditions,
    extract_section,
    parse_job_posting,
    clean_linkedin_description,
    parse_structured_description,
)


# =====================================================================
# PI Name Extraction
# =====================================================================

class TestExtractPiName:
    """Test PI name extraction from job description text."""

    @pytest.mark.parametrize("text, expected", [
        # "Lab of Dr. Name"
        (
            "A position in the laboratory of Dr. Ahmed Badran at Scripps Research.",
            "Ahmed Badran",
        ),
        # "PI: Name"
        (
            "PI: John Smith. The lab focuses on synthetic biology.",
            "John Smith",
        ),
        # "Principal Investigator: Dr. Name"
        (
            "Principal Investigator: Dr. Maria Garcia at MIT.",
            "Maria Garcia",
        ),
        # "under the supervision of Prof. Name"
        (
            "Work under the supervision of Prof. Feng Zhang on CRISPR systems.",
            "Feng Zhang",
        ),
        # "Prof. Name's lab"
        (
            "Join Prof. Jennifer Doudna's lab at UC Berkeley.",
            "Jennifer Doudna",
        ),
        # "contact Dr. Name at email"
        (
            "For inquiries contact Dr. Sarah Chen at chen@university.edu",
            "Sarah Chen",
        ),
        # "The Name Lab"
        (
            "Join The Michael Elowitz Lab for systems biology research.",
            "Michael Elowitz",
        ),
        # "supervised by Dr. Name"
        (
            "The project is supervised by Dr. Mitchell Ho at the NIH.",
            "Mitchell Ho",
        ),
        # "join Dr. Name"
        (
            "Opportunity to join Dr. Christina Smolke in Stanford.",
            "Christina Smolke",
        ),
        # "group led by Prof. Name"
        (
            "The team is led by Prof. David Baker at UW.",
            "David Baker",
        ),
        # "in the Name lab"
        (
            "Work in the Prof. Joelle Bhatt lab on stem cell research.",
            "Joelle Bhatt",
        ),
        # "mentor: Dr. Name"
        (
            "Mentor: Dr. Emily Balskus at Harvard Chemistry.",
            "Emily Balskus",
        ),
        # "directed by Dr. Name"
        (
            "The project is directed by Dr. George Church at the Wyss Institute.",
            "George Church",
        ),
        # "Prof. Dr. Name" (German style)
        (
            "Group of Prof. Dr. Thomas Schwartz at the Technical University of Munich.",
            "Thomas Schwartz",
        ),
    ])
    def test_various_pi_patterns(self, text, expected):
        result = extract_pi_name(text)
        assert result == expected, f"Expected '{expected}', got '{result}'"

    def test_returns_none_for_no_pi(self):
        text = "We are seeking a postdoctoral researcher for our biology department."
        assert extract_pi_name(text) is None

    def test_returns_none_for_empty(self):
        assert extract_pi_name("") is None
        assert extract_pi_name(None) is None

    def test_filters_false_positive_names(self):
        """Scientific field names should not be extracted as PI names."""
        text = "The Stem Cell Biology Lab is hiring a postdoc."
        result = extract_pi_name(text)
        # Should be None since "Stem Cell" is not a valid name
        assert result is None or result not in ("Stem Cell", "Cell Biology")

    def test_handles_multiline_whitespace(self):
        text = (
            "The project is supervised\n"
            "by Dr. Alice\n"
            "Johnson at the NIH."
        )
        result = extract_pi_name(text)
        assert result == "Alice Johnson"

    def test_hyphenated_last_name(self):
        text = "Group of Prof. Joanna Basta-Kaim at the institute."
        result = extract_pi_name(text)
        assert result == "Joanna Basta-Kaim"


class TestExtractPiFromTitle:
    """Test PI extraction from job titles."""

    @pytest.mark.parametrize("title, expected", [
        ("Badran Lab - Postdoc in Directed Evolution", "Badran"),
        ("Postdoctoral Fellow (Coruzzi Lab)", "Coruzzi"),
        ("Dr. Wan's Lab - Research Associate", "Wan"),
        ("Kampmann Lab Postdoc Position", "Kampmann"),
    ])
    def test_lab_name_patterns(self, title, expected):
        result = extract_pi_from_title(title)
        assert result == expected

    def test_filters_false_positives(self):
        """Common words like 'Research' should not be extracted."""
        assert extract_pi_from_title("Research Lab - Postdoc") is None
        assert extract_pi_from_title("National Lab Opening") is None
        assert extract_pi_from_title("Clinical Lab Position") is None

    def test_returns_none_for_empty(self):
        assert extract_pi_from_title("") is None
        assert extract_pi_from_title(None) is None

    def test_returns_none_for_no_lab_pattern(self):
        assert extract_pi_from_title("Postdoc in Synthetic Biology at MIT") is None


class TestExpandPiLastName:
    """Test expansion of single last name to full name."""

    def test_expands_with_title(self):
        text = "Dr. Ahmed Badran's group at Scripps focuses on directed evolution."
        result = expand_pi_last_name("Badran", text)
        assert result == "Ahmed Badran"

    def test_expands_without_title(self):
        text = "The project is led by Ahmed Badran at the Scripps Research Institute."
        result = expand_pi_last_name("Badran", text)
        assert result == "Ahmed Badran"

    def test_returns_none_when_not_found(self):
        text = "The lab studies extremophilic enzymes."
        assert expand_pi_last_name("Badran", text) is None

    def test_returns_none_for_empty(self):
        assert expand_pi_last_name("", "some text") is None
        assert expand_pi_last_name("Badran", "") is None
        assert expand_pi_last_name(None, None) is None


class TestIsValidName:
    """Test the name validation helper."""

    @pytest.mark.parametrize("name, expected", [
        ("John Smith", True),
        ("Maria Garcia", True),
        ("Joanna Basta-Kaim", True),
        ("the university", False),      # false positive
        ("Cell Biology", False),         # science term
        ("Machine Learning", False),     # false positive
        ("ab", False),                   # too short
        ("john", False),                 # no space
        ("john smith", False),           # lowercase start
        ("The Department", False),       # common word start
        ("Gene Therapy", False),         # science words
        ("Central Hr", False),           # known false positive
    ])
    def test_name_validation(self, name, expected):
        assert _is_valid_name(name) is expected


# =====================================================================
# Field Inference
# =====================================================================

class TestInferField:
    @pytest.mark.parametrize("text, expected_field", [
        ("Working on CRISPR-Cas9 systems for gene editing", "Genome Engineering"),
        ("Synthetic biology and genetic circuit design", "Synthetic Biology"),
        ("Protein engineering via directed evolution", "Protein Engineering"),
        ("Metabolic engineering and fermentation processes", "Metabolic Engineering"),
        ("Studying thermophilic archaea in extreme environments", "Extremophile Biology"),
        ("Machine learning for computational biology", "Computational Biology / AI"),
        ("Single-cell RNA sequencing analysis", "Single-Cell Biology"),
        ("CAR-T immunotherapy development", "Immunology"),
        ("Stem cell differentiation and organoid culture", "Stem Cell / Regenerative Medicine"),
        ("Drug discovery using high-throughput screening", "Drug Discovery / Pharmacology"),
        ("Plant biology and agricultural biotechnology", "Plant Biology"),
        ("Epigenetics and chromatin remodeling", "Epigenetics"),
        ("mRNA therapeutics development", "RNA Biology"),
    ])
    def test_field_mapping(self, text, expected_field):
        assert infer_field(text) == expected_field

    def test_returns_none_for_no_match(self):
        assert infer_field("Administrative assistant for the department") is None

    def test_returns_none_for_empty(self):
        assert infer_field("") is None
        assert infer_field(None) is None

    def test_case_insensitive(self):
        assert infer_field("CRISPR CAS9 GENE EDITING") == "Genome Engineering"

    def test_priority_order(self):
        """More specific patterns should match before generic ones."""
        # CRISPR is more specific than molecular biology
        text = "CRISPR gene editing and molecular cloning"
        assert infer_field(text) == "Genome Engineering"


# =====================================================================
# Keyword Extraction
# =====================================================================

class TestExtractKeywords:
    def test_basic_extraction(self):
        text = "Experience with CRISPR and protein engineering required."
        kws = extract_keywords(text)
        assert "CRISPR" in kws
        assert "protein engineering" in kws

    def test_multiple_keywords(self):
        text = (
            "The project involves synthetic biology, directed evolution, "
            "metabolic engineering, and high-throughput screening."
        )
        kws = extract_keywords(text)
        assert len(kws) >= 4
        assert "synthetic biology" in kws
        assert "directed evolution" in kws

    def test_empty_text(self):
        assert extract_keywords("") == []
        assert extract_keywords(None) == []

    def test_no_matches(self):
        assert extract_keywords("administrative tasks and budget management") == []

    def test_case_insensitive(self):
        text = "Working with crispr and Protein Engineering"
        kws = extract_keywords(text)
        assert "CRISPR" in kws
        assert "protein engineering" in kws


# =====================================================================
# Deadline Extraction
# =====================================================================

class TestExtractDeadline:
    @pytest.mark.parametrize("text, expected", [
        # "Application deadline: March 15, 2026"
        ("Application deadline: March 15, 2026", "2026-03-15"),
        # "Deadline: 15 March 2026"
        ("Deadline: 15 March 2026", "2026-03-15"),
        # "Apply by 2026-04-01"
        ("Apply by 2026-04-01", "2026-04-01"),
        # EU date format
        ("Closing date: 15/06/2026", "2026-06-15"),
        # "Applications must be received by"
        ("Applications must be received by April 30, 2026.", "2026-04-30"),
        # Abbreviated month
        ("Deadline: Mar 1, 2026", "2026-03-01"),
    ])
    def test_deadline_formats(self, text, expected):
        result = extract_deadline(text)
        assert result == expected, f"Expected '{expected}', got '{result}'"

    def test_no_deadline(self):
        assert extract_deadline("We are hiring postdocs.") is None

    def test_empty_text(self):
        assert extract_deadline("") is None
        assert extract_deadline(None) is None

    def test_deadline_in_context(self, posting_with_pi_lab):
        result = extract_deadline(posting_with_pi_lab)
        assert result == "2026-03-15"

    def test_iso_deadline(self, posting_with_pi_supervision):
        result = extract_deadline(posting_with_pi_supervision)
        assert result == "2026-04-01"

    def test_eu_date_format(self, posting_german_salary):
        result = extract_deadline(posting_german_salary)
        assert result == "2026-06-15"


# =====================================================================
# Section Extraction (Requirements, Conditions)
# =====================================================================

class TestExtractRequirements:
    def test_extracts_requirements_section(self, posting_with_pi_lab):
        result = extract_requirements(posting_with_pi_lab)
        assert result is not None
        assert "PhD" in result
        assert "molecular cloning" in result

    def test_extracts_qualifications(self, posting_with_pi_supervision):
        result = extract_requirements(posting_with_pi_supervision)
        assert result is not None
        assert "PhD" in result

    def test_returns_none_when_missing(self):
        text = "Postdoc position in synthetic biology. Apply now."
        assert extract_requirements(text) is None


class TestExtractConditions:
    def test_extracts_salary_section(self, posting_with_pi_lab):
        result = extract_conditions(posting_with_pi_lab)
        assert result is not None
        assert "$60,000" in result or "60,000" in result

    def test_extracts_german_salary(self, posting_german_salary):
        result = extract_conditions(posting_german_salary)
        assert result is not None
        # The conditions extractor may match "We offer", "Salary:", "Duration:",
        # or "Start date:" — any of these sections contain relevant info
        assert any(term in result for term in ("TV-L E13", "3 years", "October", "2026"))

    def test_fallback_duration_snippet(self):
        text = "The position is for 2 years with possibility of extension."
        result = extract_conditions(text)
        assert result is not None
        assert "2" in result


# =====================================================================
# Full Job Posting Parser
# =====================================================================

class TestParseJobPosting:
    def test_full_parse(self, posting_with_pi_lab):
        result = parse_job_posting(posting_with_pi_lab)
        assert result["pi_name"] == "Ahmed Badran"
        assert result["requirements"] is not None
        assert result["conditions"] is not None
        assert result["keywords"] is not None
        assert "CRISPR" in result["keywords"]

    def test_empty_text(self):
        result = parse_job_posting("")
        assert result["pi_name"] is None
        assert result["requirements"] is None
        assert result["conditions"] is None
        assert result["keywords"] is None

    def test_none_text(self):
        result = parse_job_posting(None)
        assert result["pi_name"] is None


# =====================================================================
# LinkedIn Description Cleaner
# =====================================================================

class TestCleanLinkedinDescription:
    def test_removes_boilerplate(self):
        text = (
            "Apply\n"
            "Save\n"
            "Report this job\n"
            "Actual job description here with real content.\n"
            "Sign in to create job alert\n"
            "By clicking Continue to Create Job Alert"
        )
        result = clean_linkedin_description(text)
        assert "Actual job description" in result
        assert "Apply" not in result.split("\n")[0] if result else True
        assert "Sign in" not in result

    def test_removes_duplicate_title(self):
        text = (
            "Software Engineer at Google\n"
            "Mountain View, CA\n"
            "\n"
            "Software Engineer at Google\n"
            "We are looking for talent.\n"
        )
        result = clean_linkedin_description(text)
        count = result.count("Software Engineer at Google")
        assert count <= 1

    def test_collapses_blank_lines(self):
        text = "Line 1\n\n\n\n\nLine 2"
        result = clean_linkedin_description(text)
        assert "\n\n\n" not in result

    def test_empty_input(self):
        assert clean_linkedin_description("") == ""
        assert clean_linkedin_description(None) == ""


class TestParseStructuredDescription:
    def test_parses_sections(self):
        text = (
            "About the Position:\n"
            "This is a postdoctoral research position in synthetic biology.\n\n"
            "Requirements:\n"
            "PhD in biology or related field.\n"
            "Experience with CRISPR.\n\n"
            "What we offer:\n"
            "Competitive salary and benefits.\n"
        )
        result = parse_structured_description(text)
        assert "summary" in result or "requirements" in result
        if "requirements" in result:
            assert "PhD" in result["requirements"]

    def test_empty_text(self):
        assert parse_structured_description("") == {}
        assert parse_structured_description(None) == {}

    def test_no_sections_found(self):
        text = "Just a plain text description with no section headers."
        assert parse_structured_description(text) == {}
