"""
End-to-end integration tests for the full and delta load pipelines.
"""
import pytest
from pathlib import Path
from typer.testing import CliRunner
from unittest.mock import MagicMock

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.db import PostgreSQLAdapter
from py_load_pubmedcentral.acquisition import ArticleFileInfo, IncrementalUpdateInfo
from datetime import datetime, timezone

# Path to our test data directory
TEST_DATA_DIR = Path(__file__).parent / "test_data"

# Mark all tests in this file as 'integration' tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def runner():
    """Provides a Typer CliRunner for invoking the CLI commands."""
    return CliRunner()


@pytest.fixture(autouse=True)
def mock_pipeline_components(mocker, test_db_adapter: PostgreSQLAdapter):
    """
    Mocks components of the pipeline for robust integration testing.

    This fixture will:
    1.  Patch `get_db_adapter` to always return the session-scoped test adapter.
    2.  Patch the download workers to prevent actual downloads.
    3.  Patch the parsing workers to return pre-defined TSV files, decoupling
        the orchestration logic from the complexities of parsing test data.
    """
    # 1. Patch the DB adapter and prevent it from being closed by the CLI runner
    mocker.patch("py_load_pubmedcentral.cli.get_db_adapter", return_value=test_db_adapter)
    mocker.patch.object(test_db_adapter, 'close', return_value=None)

    # 2. Mock the data source methods that list files to control the flow
    mock_full_load_file_list = [
        ArticleFileInfo(file_path="archive1.tar.gz", pmcid="PMC001", pmid="10001", last_updated=datetime(2023, 1, 1, 12, 0, 0), is_retracted=False),
        ArticleFileInfo(file_path="archive1.tar.gz", pmcid="PMC002", pmid="10002", last_updated=datetime(2023, 1, 2, 12, 0, 0), is_retracted=False),
    ]
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_article_file_list", return_value=mock_full_load_file_list)
    mocker.patch("py_load_pubmedcentral.acquisition.NcbiFtpDataSource.get_article_file_list", return_value=mock_full_load_file_list)

    mock_delta_update_info = [
        IncrementalUpdateInfo(archive_path="delta1.tar.gz", file_list_path="delta1.filelist.csv", date=datetime(2023, 6, 15, tzinfo=timezone.utc))
    ]
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_incremental_updates", return_value=mock_delta_update_info)
    mocker.patch("py_load_pubmedcentral.acquisition.NcbiFtpDataSource.get_incremental_updates", return_value=mock_delta_update_info)

    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_retracted_pmcids", return_value=["PMC002", "PMC004"])


    # 3. Mock the worker functions to simulate their output directly.
    # This is more robust than relying on parsing physical test files.

    # Mock the download worker to do nothing and return a dummy path
    mocker.patch("py_load_pubmedcentral.cli._download_archive_worker", return_value=Path("/dev/null"))

    # Mock the full load parsing worker
    def mock_full_load_parser(verified_path, article_info_lookup, tmp_path):
        meta_path = tmp_path / "full_meta.tsv"
        content_path = tmp_path / "full_content.tsv"
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write("PMC001\t10001\t10.1000/test.1\tThe Science of Baseline Data\tThis is the abstract for the first baseline article.\t2023-01-01\t{\"name\": \"Journal of Test Data\", \"issn\": null, \"publisher\": null}\t[{\"name\": \"John Smith\", \"affiliation\": \"Data University\", \"orcid\": null}]\t{\"type\": \"cc-by\", \"url\": null}\tFalse\t2023-01-01 12:00:00+00:00\t2025-09-08 13:51:59.442037+00:00\n")
            f.write("PMC002\t10002\t10.1000/test.2\tAn Article to be Retracted\tThis article will be retracted later.\t2023-01-02\t{\"name\": \"Journal of Test Data\", \"issn\": null, \"publisher\": null}\t[{\"name\": \"Mary Jones\", \"affiliation\": \"Institute of Testing\", \"orcid\": null}]\t{\"type\": \"cc-by-nc\", \"url\": null}\tFalse\t2023-01-02 12:00:00+00:00\t2025-09-08 13:51:59.442037+00:00\n")
        with open(content_path, "w", encoding="utf-8") as f:
            f.write("PMC001\t<article>...PMC001...</article>\tThis is the full text of the first article (PMC001).\n")
            f.write("PMC002\t<article>...PMC002...</article>\tThis is the body of the second article (PMC002).\n")
        return meta_path, content_path, 2

    mocker.patch("py_load_pubmedcentral.cli._parse_archive_worker", side_effect=mock_full_load_parser)

    # Mock the delta load parsing worker
    def mock_delta_load_parser(verified_path, update_info, source_name, tmp_path):
        meta_path = tmp_path / "delta_meta.tsv"
        content_path = tmp_path / "delta_content.tsv"
        with open(meta_path, "w", encoding="utf-8") as f:
            # This represents an update to PMC001 and a new article PMC003
            f.write("PMC001\t10001\t10.1000/test.1\tThe Updated Science of Baseline Data\tThis is the abstract for the first baseline article.\t2023-01-01\t{\"name\": \"Journal of Test Data\", \"issn\": null, \"publisher\": null}\t[{\"name\": \"John Smith\", \"affiliation\": \"Data University\", \"orcid\": null}, {\"name\": \"Jane Doe\", \"affiliation\": \"Data University\", \"orcid\": null}]\t{\"type\": \"cc-by\", \"url\": null}\tFalse\t2023-06-15 10:00:00+00:00\t2025-09-08 13:51:59.442037+00:00\n")
            f.write("PMC003\t10003\t10.1000/test.3\tA Brand New Article\tThis is a new article for the delta load.\t2023-06-15\t{\"name\": \"Journal of Delta Loads\", \"issn\": null, \"publisher\": null}\t[{\"name\": \"Mark Miller\", \"affiliation\": \"Test Inc.\", \"orcid\": null}]\t{\"type\": \"cc0\", \"url\": null}\tFalse\t2023-06-15 10:00:00+00:00\t2025-09-08 13:51:59.442037+00:00\n")
        with open(content_path, "w", encoding="utf-8") as f:
            f.write("PMC001\t<article>...PMC001 updated...</article>\tThis is the UPDATED full text of the first article (PMC001).\n")
            f.write("PMC003\t<article>...PMC003...</article>\tThis is the body of the new article (PMC003).\n")
        # Simulate that the delta file list also noted PMC002 was retracted
        retracted_pmcids = ["PMC002"]
        return meta_path, content_path, 2, retracted_pmcids, "delta1.tar.gz"

    mocker.patch("py_load_pubmedcentral.cli._parse_delta_archive_worker", side_effect=mock_delta_load_parser)

    yield


import pytest

# ... (other imports)

@pytest.mark.dependency()
def test_full_load_pipeline(runner: CliRunner, test_db_adapter: PostgreSQLAdapter):
    """
    Tests the entire full_load pipeline: initialize -> full-load -> verify db.
    """
    # ... (rest of the test)

@pytest.mark.dependency(depends=["test_full_load_pipeline"])
def test_delta_load_pipeline(runner: CliRunner, test_db_adapter: PostgreSQLAdapter):
    """
    Tests the delta_load pipeline. Depends on the full_load test having run first.
    """
    # 1. Run the delta load
    delta_load_result = runner.invoke(app, [
        "delta-load",
        "--source", "s3",
        "--download-workers", "1",
        "--parsing-workers", "1"
    ])
    assert delta_load_result.exit_code == 0, delta_load_result.stdout

    # 2. Verify the data in the database
    with test_db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
        records = cursor.fetchall()

        # We should now have 3 articles in total
        assert len(records) == 3

        # PMC001: Should be updated
        assert records[0][0] == "PMC001"
        assert records[0][1] == "The Updated Science of Baseline Data"
        assert records[0][2] is False  # is_retracted should still be False

        # PMC002: Should be marked as retracted
        assert records[1][0] == "PMC002"
        assert records[1][2] is True  # is_retracted should now be True

        # PMC003: Should be newly inserted
        assert records[2][0] == "PMC003"
        assert records[2][1] == "A Brand New Article"
        assert records[2][2] is False

        # Check sync history
        cursor.execute("SELECT run_type, status FROM sync_history ORDER BY run_id;")
        history_records = cursor.fetchall()
        assert len(history_records) == 2
        assert history_records[0] == ("FULL", "SUCCESS")
        assert history_records[1] == ("DELTA", "SUCCESS")

# The above change is incomplete. I need to add the dependency marker to the first test.
# I'll do that now.

@pytest.mark.dependency()
def test_full_load_pipeline(runner: CliRunner, test_db_adapter: PostgreSQLAdapter):
    """
    Tests the entire full_load pipeline: initialize -> full-load -> verify db.
    """
    # 1. Initialize the database schema
    # Note: The schema path is relative to the project root where pytest is run.
    schema_path = "schemas/pmc_schema.sql"
    init_result = runner.invoke(app, ["initialize", "--schema", schema_path])
    assert init_result.exit_code == 0

    # 2. Run the full load
    # We use minimal workers for deterministic testing.
    # The --source s3 is the default, but we specify it for clarity.
    full_load_result = runner.invoke(app, [
        "full-load",
        "--source", "s3",
        "--download-workers", "1",
            "--parsing-workers", "1"
    ], catch_exceptions=False)
    assert full_load_result.exit_code == 0


    # 3. Verify the data in the database
    with test_db_adapter.conn.cursor() as cursor:
        # Check metadata table
        cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
        metadata_records = cursor.fetchall()
        assert len(metadata_records) == 2
        assert metadata_records[0] == ("PMC001", "The Science of Baseline Data", False)
        assert metadata_records[1] == ("PMC002", "An Article to be Retracted", False)

        # Check content table
        cursor.execute("SELECT pmcid, body_text FROM pmc_articles_content ORDER BY pmcid;")
        content_records = cursor.fetchall()
        assert len(content_records) == 2
        assert content_records[0] == ("PMC001", "This is the full text of the first article (PMC001).")
        assert content_records[1] == ("PMC002", "This is the body of the second article (PMC002).")

        # Check sync history
        cursor.execute("SELECT run_type, status FROM sync_history;")
        history_records = cursor.fetchall()
        assert len(history_records) == 1
        assert history_records[0] == ("FULL", "SUCCESS")


@pytest.mark.dependency(depends=["test_full_load_pipeline"])
def test_delta_load_pipeline(runner: CliRunner, test_db_adapter: PostgreSQLAdapter):
    """
    Tests the delta_load pipeline. Depends on the full_load test having run first.
    """
    # 1. Run the delta load
    delta_load_result = runner.invoke(app, [
        "delta-load",
        "--source", "s3",
        "--download-workers", "1",
        "--parsing-workers", "1"
    ], catch_exceptions=False)
    assert delta_load_result.exit_code == 0

    # 2. Verify the data in the database
    with test_db_adapter.conn.cursor() as cursor:
        cursor.execute("SELECT pmcid, title, is_retracted FROM pmc_articles_metadata ORDER BY pmcid;")
        records = cursor.fetchall()

        # We should now have 3 articles in total
        assert len(records) == 3

        # PMC001: Should be updated
        assert records[0][0] == "PMC001"
        assert records[0][1] == "The Updated Science of Baseline Data"
        assert records[0][2] is False  # is_retracted should still be False

        # PMC002: Should be marked as retracted
        assert records[1][0] == "PMC002"
        assert records[1][2] is True  # is_retracted should now be True

        # PMC003: Should be newly inserted
        assert records[2][0] == "PMC003"
        assert records[2][1] == "A Brand New Article"
        assert records[2][2] is False

        # Check that the content for PMC001 was updated
        cursor.execute("SELECT body_text FROM pmc_articles_content WHERE pmcid = 'PMC001';")
        content_record = cursor.fetchone()
        assert content_record[0] == "This is the UPDATED full text of the first article (PMC001)."

        # Check sync history
        cursor.execute("SELECT run_type, status FROM sync_history ORDER BY run_id;")
        history_records = cursor.fetchall()
        assert len(history_records) == 2
        assert history_records[0] == ("FULL", "SUCCESS")
        assert history_records[1] == ("DELTA", "SUCCESS")
