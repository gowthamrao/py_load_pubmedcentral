"""
Unit tests for the JATS XML parser.
"""
import io
from datetime import date, datetime
from pathlib import Path

# Although we can't run pytest, we write the tests as if we could.
# import pytest

from py_load_pubmedcentral.parser import parse_jats_xml

# The path to the test data file
TEST_XML_PATH = Path(__file__).parent / "test.xml"


def test_parse_jats_xml_comprehensive_article():
    """
    Tests that a well-formed article with all fields is parsed correctly.
    """
    test_date = datetime(2023, 1, 1, 12, 0, 0)
    with open(TEST_XML_PATH, "rb") as f:
        content = f.read()
    results = list(parse_jats_xml(io.BytesIO(content), source_last_updated=test_date, is_retracted=True))

    # Check that two articles were parsed
    assert len(results) == 2

    # --- Assertions for the first, complete article ---
    metadata, content = results[0]

    # IDs
    assert metadata.pmcid == "PMC12345"
    assert metadata.pmid == 54321
    assert metadata.doi == "10.1000/jtm.2025.01"

    # Core metadata
    assert metadata.title == "A Comprehensive Test of the JATS Parser"
    assert "This is the abstract." in metadata.abstract_text
    assert metadata.publication_date == date(2025, 9, 1)
    # Check new fields
    assert metadata.is_retracted is True
    assert metadata.source_last_updated == test_date

    # Journal Info
    assert metadata.journal_info.name == "Journal of Test Medicine"
    assert metadata.journal_info.issn == "1234-5678"
    assert metadata.journal_info.publisher == "Test Publisher"

    # Contributors
    assert len(metadata.contributors) == 1
    assert metadata.contributors[0].name == "John A. Doe"
    assert metadata.contributors[0].affiliation == "Department of Testing, University of Examples"
    assert metadata.contributors[0].orcid == "0000-0001-2345-6789"

    # License Info
    assert metadata.license_info.license_type == "open-access"
    assert metadata.license_info.url == "http://creativecommons.org/licenses/by/4.0/"

    # Content
    assert content.pmcid == "PMC12345"
    assert "<article>" in content.raw_jats_xml
    assert "This is the full body text of the article." in content.body_text
    assert "We tested things thoroughly." in content.body_text


def test_parse_jats_xml_incomplete_article():
    """
    Tests that an article with missing fields is parsed gracefully.
    """
    test_date = datetime(2023, 2, 1, 12, 0, 0)
    with open(TEST_XML_PATH, "rb") as f:
        content = f.read()
    results = list(parse_jats_xml(io.BytesIO(content), source_last_updated=test_date, is_retracted=False))

    # Check that two articles were parsed
    assert len(results) == 2

    # --- Assertions for the second, incomplete article ---
    metadata, content = results[1]

    # IDs
    assert metadata.pmcid == "PMC67890"
    assert metadata.pmid is None
    assert metadata.doi is None

    # Core metadata
    assert metadata.title == "An Article with Missing Fields"
    assert metadata.abstract_text is None
    assert metadata.publication_date == date(2024, 1, 1) # Defaults month/day to 1
    # Check new fields
    assert metadata.is_retracted is False
    assert metadata.source_last_updated == test_date

    # Journal Info
    assert metadata.journal_info.name == "Journal of Incomplete Data"
    assert metadata.journal_info.issn is None
    assert metadata.journal_info.publisher == "Missing Info Press"

    # Contributors
    assert len(metadata.contributors) == 1
    assert metadata.contributors[0].name == "Jane Smith"
    assert metadata.contributors[0].affiliation is None
    assert metadata.contributors[0].orcid is None

    # License Info (the whole object should be None)
    assert metadata.license_info is None

    # Content
    assert content.pmcid == "PMC67890"
    assert "Body text is present." in content.body_text


def test_parser_is_a_generator():
    """
    Tests that the parser functions as a generator to keep memory usage low.
    """
    test_date = datetime(2023, 3, 1, 12, 0, 0)
    with open(TEST_XML_PATH, "rb") as f:
        content = f.read()
    parser = parse_jats_xml(io.BytesIO(content), source_last_updated=test_date, is_retracted=False)

    # Check that it's a generator
    assert hasattr(parser, '__next__')

    # Pull one item
    metadata, content = next(parser)
    assert metadata.pmcid == "PMC12345"

    # Pull the next
    metadata, content = next(parser)
    assert metadata.pmcid == "PMC67890"

    # Should be exhausted now
    try:
        next(parser)
        # This line should not be reached
        assert False, "Parser should be exhausted after two articles."
    except StopIteration:
        # This is the expected outcome
        pass
