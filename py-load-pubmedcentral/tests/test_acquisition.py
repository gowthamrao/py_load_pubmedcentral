import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import requests

from py_load_pubmedcentral.acquisition import NcbiFtpDataSource

class MockResponse:
    def __init__(self, content, status_code=200):
        self._content = content
        self.status_code = status_code
        self.text = self._content.decode('utf-8') if isinstance(self._content, bytes) else self._content

    def iter_content(self, chunk_size=8192):
        for i in range(0, len(self._content), chunk_size):
            yield self._content[i:i + chunk_size]

    def iter_lines(self):
        for line in self.text.splitlines():
            yield line.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP Error {self.status_code}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


@pytest.fixture
def mock_requests_get():
    """Fixture to patch requests.get."""
    with patch("requests.get") as mock_get:
        yield mock_get


def test_get_article_file_list(mock_requests_get):
    """
    Tests that the data source can correctly parse the CSV file list.
    """
    # Load the sample CSV content
    sample_csv_path = Path(__file__).parent / "sample_oa_file_list.csv"
    with open(sample_csv_path, "rb") as f:
        sample_csv_content = f.read()

    # Mock the response from requests.get
    mock_requests_get.return_value = MockResponse(sample_csv_content)

    data_source = NcbiFtpDataSource()
    article_list = data_source.get_article_file_list()

    assert len(article_list) == 2

    # Check the first record
    assert article_list[0].file_path == "oa_package/a5/39/PMC12345.tar.gz"
    assert article_list[0].pmcid == "PMC12345"
    assert article_list[0].pmid == "98765"
    assert article_list[0].last_updated == datetime(2023, 1, 15, 10, 0, 0)
    assert article_list[0].is_retracted is True

    # Check the second record (with a missing PMID)
    assert article_list[1].pmcid == "PMC67890"
    assert article_list[1].pmid is None
    assert article_list[1].is_retracted is False


def test_get_retracted_pmcids_with_header(mock_requests_get):
    """
    Tests parsing the retractions CSV file with a header row.
    """
    csv_content = "PMCID\nPMC12345\nPMC67890"
    mock_requests_get.return_value = MockResponse(csv_content.encode("utf-8"))
    data_source = NcbiFtpDataSource()
    retracted_list = data_source.get_retracted_pmcids()
    assert retracted_list == ["PMC12345", "PMC67890"]


def test_get_retracted_pmcids_no_header(mock_requests_get):
    """
    Tests parsing the retractions CSV file without a header row.
    """
    csv_content = "PMC12345\nPMC67890"
    mock_requests_get.return_value = MockResponse(csv_content.encode("utf-8"))
    data_source = NcbiFtpDataSource()
    retracted_list = data_source.get_retracted_pmcids()
    assert retracted_list == ["PMC12345", "PMC67890"]


def test_download_file_success(mock_requests_get, tmp_path):
    """
    Tests the successful download and verification of a file.
    """
    file_content = b"This is a test file."
    file_hash = hashlib.md5(file_content).hexdigest()
    md5_content = f"MD5(test_file.tar.gz)= {file_hash}"
    url = "https://fake.host/test_file.tar.gz"

    # Set up multiple return values for requests.get
    mock_requests_get.side_effect = [
        MockResponse(file_content),
        MockResponse(md5_content.encode("utf-8"))
    ]

    data_source = NcbiFtpDataSource()
    destination_path = data_source.download_file(url, tmp_path)

    assert destination_path.exists()
    assert destination_path.read_bytes() == file_content
    assert mock_requests_get.call_count == 2
    mock_requests_get.assert_any_call(url, stream=True)
    mock_requests_get.assert_any_call(url + ".md5")


def test_download_file_checksum_failure(mock_requests_get, tmp_path):
    """
    Tests that an IOError is raised when the checksum verification fails.
    """
    file_content = b"This is a test file."
    incorrect_hash = "0" * 32
    md5_content = f"MD5(test_file.tar.gz)= {incorrect_hash}"
    url = "https://fake.host/test_file.tar.gz"

    mock_requests_get.side_effect = [
        MockResponse(file_content),
        MockResponse(md5_content.encode("utf-8"))
    ]

    data_source = NcbiFtpDataSource()

    with pytest.raises(IOError, match=r"Checksum mismatch"):
        data_source.download_file(url, tmp_path)

    # Check that the corrupt file was cleaned up
    destination_path = tmp_path / "test_file.tar.gz"
    assert not destination_path.exists()
