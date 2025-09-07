import io
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

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
def test_list_baseline_files_s3(mock_boto_client):
    """
    Tests that list_baseline_files correctly paginates and filters for .tar.gz files.
    """
    # Arrange: Mock the S3 client and its paginator
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator

    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "oa_file_list.csv"},
                {"Key": "oa_package/00/00/PMC1.tar.gz"},
            ]
        },
        {
            "Contents": [
                {"Key": "oa_package/00/01/PMC2.tar.gz"},
                {"Key": "other_file.txt"},
            ]
        },
    ]
    mock_boto_client.return_value = mock_s3

    # Act
    data_source = S3DataSource()
    result = data_source.list_baseline_files()

    # Assert
    mock_s3.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(Bucket="pmc-oa-opendata")
    assert len(result) == 2
    assert "oa_package/00/00/PMC1.tar.gz" in result
    assert "oa_package/00/01/PMC2.tar.gz" in result
    assert "oa_file_list.csv" not in result


@patch("boto3.client")
def test_download_file_s3_with_checksum(mock_boto_client, tmp_path: Path):
    """
    Tests that download_file calls the S3 client with ChecksumMode enabled.
    """
    # Arrange
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    s3_key = "oa_package/00/00/PMC1.tar.gz"
    destination_dir = tmp_path

    # Act
    data_source = S3DataSource()
    result_path = data_source.download_file(s3_key, destination_dir)

    # Assert
    expected_path = destination_dir / "PMC1.tar.gz"
    assert result_path == expected_path

    mock_s3.download_file.assert_called_once_with(
        Bucket="pmc-oa-opendata",
        Key=s3_key,
        Filename=str(expected_path),
        ExtraArgs={"ChecksumMode": "ENABLED"},
    )
