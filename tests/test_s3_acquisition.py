import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime

from py_load_pubmedcentral.acquisition import S3DataSource
from py_load_pubmedcentral.models import ArticleFileInfo


@patch("boto3.client")
def test_get_article_file_list_s3_success(mock_boto_client):
    """
    Tests that get_article_file_list successfully parses a CSV from S3.
    """
    # Arrange: Mock the S3 client and its get_object method
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    csv_content = (
        b"File,Accession ID,PMID,Last Updated\n"
        b"oa_package/a0/00/PMC12345.tar.gz,PMC12345,1234567,2025-01-01 12:00:00\n"
        b"oa_package/b1/11/PMC67890.tar.gz,PMC67890,,2025-01-02 13:00:00\n" # No PMID
    )
    mock_body = MagicMock()
    mock_body.iter_lines.return_value = csv_content.splitlines()
    mock_s3.get_object.return_value = {"Body": mock_body}

    # Act
    data_source = S3DataSource()
    result = data_source.get_article_file_list()

    # Assert
    mock_s3.get_object.assert_called_once_with(
        Bucket="pmc-oa-opendata", Key="oa_file_list.csv"
    )
    assert len(result) == 2
    assert isinstance(result[0], ArticleFileInfo)
    assert result[0].pmcid == "PMC12345"
    assert result[0].pmid == "1234567"
    assert result[1].pmcid == "PMC67890"
    assert result[1].pmid is None


@patch("boto3.client")
def test_download_file_s3_success_with_matching_etag(mock_boto_client, tmp_path: Path):
    """
    Tests that download_file succeeds when the ETag matches the local file's hash.
    """
    # Arrange
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    s3_key = "oa_package/00/00/PMC1.tar.gz"
    destination_path = tmp_path / "PMC1.tar.gz"
    dummy_etag = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 of an empty string

    # Mock S3 calls
    mock_s3.head_object.return_value = {"ETag": f'"{dummy_etag}"'}

    # Create a dummy downloaded file
    destination_path.touch()

    # Act
    data_source = S3DataSource()
    result_path = data_source.download_file(s3_key, tmp_path)

    # Assert
    assert result_path == destination_path
    mock_s3.head_object.assert_called_once_with(Bucket="pmc-oa-opendata", Key=s3_key)
    mock_s3.download_file.assert_called_once_with(
        Bucket="pmc-oa-opendata", Key=s3_key, Filename=str(destination_path)
    )


@patch("boto3.client")
def test_get_retracted_pmcids_s3(mock_boto_client):
    """
    Tests that get_retracted_pmcids successfully parses the CSV from S3.
    """
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    csv_content = b"PMCID\nPMC123\nPMC456"
    mock_body = MagicMock()
    mock_body.iter_lines.return_value = csv_content.splitlines()
    mock_s3.get_object.return_value = {"Body": mock_body}

    data_source = S3DataSource()
    result = data_source.get_retracted_pmcids()

    assert result == ["PMC123", "PMC456"]

@patch("boto3.client")
def test_get_retracted_pmcids_s3_no_header(mock_boto_client):
    """
    Tests that get_retracted_pmcids works without a header.
    """
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    csv_content = b"PMC123\nPMC456"
    mock_body = MagicMock()
    mock_body.iter_lines.return_value = csv_content.splitlines()
    mock_s3.get_object.return_value = {"Body": mock_body}

    data_source = S3DataSource()
    result = data_source.get_retracted_pmcids()

    assert result == ["PMC123", "PMC456"]

@patch("boto3.client")
def test_get_retracted_pmcids_s3_no_such_key(mock_boto_client):
    """
    Tests that get_retracted_pmcids returns an empty list if the file doesn't exist.
    """
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    from botocore.exceptions import ClientError
    mock_s3.get_object.side_effect = ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")

    data_source = S3DataSource()
    result = data_source.get_retracted_pmcids()

    assert result == []


@patch("boto3.client")
def test_get_incremental_updates_s3(mock_boto_client):
    """
    Tests that get_incremental_updates successfully finds new updates from S3.
    """
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = [
        [
            {
                "Contents": [
                    {"Key": "oa_bulk/oa_comm/xml/comm_use.incr.2023-01-03.tar.gz"},
                    {"Key": "oa_bulk/oa_comm/xml/comm_use.incr.2022-12-31.tar.gz"},
                ]
            }
        ],
        [], # for oa_noncomm
        [], # for oa_other
    ]
    mock_s3.get_paginator.return_value = mock_paginator
    mock_boto_client.return_value = mock_s3

    data_source = S3DataSource()
    since_date = datetime(2023, 1, 1, 0, 0)
    updates = data_source.get_incremental_updates(since=since_date)

    assert len(updates) == 1
    assert "comm_use.incr.2023-01-03" in updates[0].archive_path


@patch("boto3.client")
def test_download_file_s3_raises_error_on_mismatched_etag(mock_boto_client, tmp_path: Path):
    """
    Tests that download_file raises an IOError and deletes the file
    when the ETag does not match the local file's hash.
    """
    # Arrange
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    s3_key = "oa_package/00/00/PMC1.tar.gz"
    destination_path = tmp_path / "PMC1.tar.gz"
    dummy_etag = "d41d8cd98f00b204e9800998ecf8427e"  # Correct ETag

    mock_s3.head_object.return_value = {"ETag": f'"{dummy_etag}"'}

    # Create a dummy file that will have a "wrong" hash
    with open(destination_path, "w") as f:
        f.write("some content")

    # Act & Assert
    data_source = S3DataSource()
    with pytest.raises(IOError, match="Checksum mismatch"):
        data_source.download_file(s3_key, tmp_path)

    # Assert that the corrupt file was deleted
    assert not destination_path.exists()
    mock_s3.head_object.assert_called_once_with(Bucket="pmc-oa-opendata", Key=s3_key)


@patch("boto3.client")
def test_download_file_s3_skips_check_for_multipart_etag(mock_boto_client, tmp_path: Path):
    """
    Tests that download_file succeeds without a checksum check for multipart ETags.
    """
    # Arrange
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    s3_key = "oa_package/00/00/PMC1.tar.gz"
    destination_path = tmp_path / "PMC1.tar.gz"
    multipart_etag = "d41d8cd98f00b204e9800998ecf8427e-2" # ETag with a hyphen

    mock_s3.head_object.return_value = {"ETag": f'"{multipart_etag}"'}
    destination_path.touch()

    # Act
    data_source = S3DataSource()
    with patch("hashlib.md5") as mock_md5:
        result_path = data_source.download_file(s3_key, tmp_path)
        # Assert that the hashing logic was never called
        mock_md5.assert_not_called()

    # Assert
    assert result_path == destination_path
    assert destination_path.exists()


@patch("boto3.client")
def test_download_file_s3_skips_check_for_non_archive_files(mock_boto_client, tmp_path: Path):
    """
    Tests that download_file skips checksum logic for non .tar.gz files.
    """
    # Arrange
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    s3_key = "oa_file_list.csv"  # A file that shouldn't be checksummed
    destination_path = tmp_path / "oa_file_list.csv"

    # Act
    data_source = S3DataSource()
    result_path = data_source.download_file(s3_key, tmp_path)

    # Assert
    assert result_path == destination_path
    # head_object should not be called for non-archive files
    mock_s3.head_object.assert_not_called()
    mock_s3.download_file.assert_called_once_with(
        Bucket="pmc-oa-opendata", Key=s3_key, Filename=str(destination_path)
    )
