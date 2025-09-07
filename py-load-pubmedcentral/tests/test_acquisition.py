import hashlib
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from py_load_pubmedcentral.acquisition import NcbiFtpDataSource


@pytest.fixture
def mock_requests_get():
    """Fixture to patch requests.get."""
    with patch("requests.get") as mock_get:
        yield mock_get


@pytest.fixture
def sample_html_listing():
    """Provides a sample HTML directory listing."""
    return """
    <html><body>
    <a href="other_file.txt">other_file.txt</a>
    <a href="oa_comm_xml.baseline.2023-12-12.tar.gz">oa_comm_xml.baseline.2023-12-12.tar.gz</a>
    <a href="oa_comm_xml.incr.2024-01-01.tar.gz">oa_comm_xml.incr.2024-01-01.tar.gz</a>
    <a href="oa_comm_xml.baseline.2024-06-15.tar.gz">oa_comm_xml.baseline.2024-06-15.tar.gz</a>
    </body></html>
    """


def test_list_baseline_files(mock_requests_get, sample_html_listing):
    """
    Tests that the data source can correctly parse an HTML directory listing
    and return only the baseline file URLs.
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = sample_html_listing.encode("utf-8")
    mock_requests_get.return_value = mock_response

    data_source = NcbiFtpDataSource()
    baseline_files = data_source.list_baseline_files()

    assert len(baseline_files) == 2
    assert "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/oa_comm_xml.baseline.2023-12-12.tar.gz" in baseline_files
    assert "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/oa_comm_xml.baseline.2024-06-15.tar.gz" in baseline_files
    assert "https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml/oa_comm_xml.incr.2024-01-01.tar.gz" not in baseline_files


def test_download_file_success(mock_requests_get, tmp_path):
    """
    Tests the successful download and verification of a file.
    """
    file_content = b"This is a test file."
    file_hash = hashlib.md5(file_content).hexdigest()
    md5_content = f"MD5(test_file.tar.gz)= {file_hash}"
    url = "https://fake.host/test_file.tar.gz"

    # Set up multiple return values for requests.get
    mock_tar_response = MagicMock()
    mock_tar_response.status_code = 200
    mock_tar_response.iter_content.return_value = [file_content]

    mock_md5_response = MagicMock()
    mock_md5_response.status_code = 200
    mock_md5_response.text = md5_content

    mock_requests_get.side_effect = [mock_tar_response, mock_md5_response]

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
    correct_hash = hashlib.md5(file_content).hexdigest()
    incorrect_hash = "0" * 32
    md5_content = f"MD5(test_file.tar.gz)= {incorrect_hash}"
    url = "https://fake.host/test_file.tar.gz"

    mock_tar_response = MagicMock()
    mock_tar_response.status_code = 200
    mock_tar_response.iter_content.return_value = [file_content]

    mock_md5_response = MagicMock()
    mock_md5_response.status_code = 200
    mock_md5_response.text = md5_content

    mock_requests_get.side_effect = [mock_tar_response, mock_md5_response]

    data_source = NcbiFtpDataSource()

    with pytest.raises(IOError, match=r"Checksum mismatch"):
        data_source.download_file(url, tmp_path)

    # Check that the corrupt file was cleaned up
    destination_path = tmp_path / "test_file.tar.gz"
    assert not destination_path.exists()
