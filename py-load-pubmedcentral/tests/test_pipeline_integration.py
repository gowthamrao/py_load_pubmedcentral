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
def mock_data_sources(mocker, test_db_adapter: PostgreSQLAdapter):
    """
    Mocks the data sources and database adapter for the entire test module.

    This fixture will:
    1.  Patch `get_db_adapter` to always return the session-scoped test adapter.
    2.  Patch the data acquisition methods of both FTP and S3 sources to
        return our curated local test data instead of making network calls.
    """
    # 1. Patch the DB adapter where it's used
    mocker.patch("py_load_pubmedcentral.cli.get_db_adapter", return_value=test_db_adapter)

    # Prevent the CLI commands from closing our session-scoped test connection
    mocker.patch.object(test_db_adapter, 'close', return_value=None)


    # 2. Define the mock data to be returned
    # For full_load:
    mock_full_load_file_list = [
        ArticleFileInfo(
            file_path="comm_use.A-B.xml.tar.gz",
            pmcid="PMC001",
            pmid="10001",
            last_updated=datetime(2023, 1, 1, 12, 0, 0),
            is_retracted=False
        ),
        ArticleFileInfo(
            file_path="comm_use.A-B.xml.tar.gz",
            pmcid="PMC002",
            pmid="10002",
            last_updated=datetime(2023, 1, 2, 12, 0, 0),
            is_retracted=False
        ),
    ]

    # For delta_load:
    mock_delta_update_info = [
        IncrementalUpdateInfo(
            archive_path=str(TEST_DATA_DIR / "daily.2023-06-15.tar.gz"),
            file_list_path=str(TEST_DATA_DIR / "delta.filelist.csv"),
            date=datetime(2023, 6, 15, tzinfo=timezone.utc),
        )
    ]

    # 3. Apply the patches
    # Mock S3 Data Source
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_article_file_list", return_value=mock_full_load_file_list)
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.download_file", side_effect=lambda key, dest: TEST_DATA_DIR / key.split('/')[-1])
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_retracted_pmcids", return_value=["PMC002", "PMC004"])
    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.get_incremental_updates", return_value=mock_delta_update_info)

    def mock_stream_delta_infos(file_list_path):
        """Mock version of streaming infos from the delta file list."""
        # Note: In a real scenario, this would read the CSV. For this test, we can hardcode it.
        yield ArticleFileInfo(file_path='daily.2023-06-15.tar.gz', pmcid='PMC001', pmid='10001', last_updated=datetime(2023, 6, 15, 10, 0), is_retracted=False)
        yield ArticleFileInfo(file_path='daily.2023-06-15.tar.gz', pmcid='PMC003', pmid='10003', last_updated=datetime(2023, 6, 15, 10, 0), is_retracted=False)
        yield ArticleFileInfo(file_path='daily.2023-06-15.tar.gz', pmcid='PMC002', pmid='10002', last_updated=datetime(2023, 1, 2, 12, 0), is_retracted=True)

    mocker.patch("py_load_pubmedcentral.acquisition.S3DataSource.stream_article_infos_from_file_list", side_effect=mock_stream_delta_infos)

    # We don't need to mock the FTP source if we only test with the S3 source,
    # but it's good practice in case we add a test for it later.
    mocker.patch("py_load_pubmedcentral.acquisition.NcbiFtpDataSource.get_article_file_list", return_value=mock_full_load_file_list)
    mocker.patch("py_load_pubmedcentral.acquisition.NcbiFtpDataSource.download_file", side_effect=lambda url, dest: TEST_DATA_DIR / url.split('/')[-1])

    # This ensures the mocks are set up before any test in the module runs
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
    assert delta_load_result.exit_code == 0
    assert "--- Starting Delta Load ---" in delta_load_result.stdout
    assert "Marked 2 articles as retracted based on master list." in delta_load_result.stdout
    assert "Found 1 incremental archives to process" in delta_load_result.stdout
    assert "Upserting data from 1 archives" in delta_load_result.stdout
    assert "Processing 1 retractions from daily file lists" in delta_load_result.stdout
    assert "Updating sync_history for run_id 2 with status 'SUCCESS'" in delta_load_result.stdout

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
    assert "Database schema initialized successfully" in init_result.stdout

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
