import pytest
from typer.testing import CliRunner
from unittest.mock import patch
import os
from datetime import datetime, timezone
from pathlib import Path

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.acquisition import IncrementalUpdateInfo


@pytest.fixture(autouse=True)
def cleanup_dummy_file():
    """Fixture to clean up dummy files created during tests."""
    yield
    if os.path.exists("dummy_schema.sql"):
        os.remove("dummy_schema.sql")


def test_initialize_file_not_found():
    """
    Tests that the initialize command exits gracefully if the schema file is not found.
    """
    runner = CliRunner()
    result = runner.invoke(app, ["initialize", "--schema", "non_existent_file.sql"])
    assert result.exit_code == 1
    assert "Error: Schema file not found" in result.stdout


@patch("py_load_pubmedcentral.cli.get_db_adapter")
def test_initialize_success(mock_get_adapter):
    """
    Tests the happy path for the initialize command.
    """
    # Mock the adapter to prevent any actual DB calls
    mock_adapter_instance = mock_get_adapter.return_value

    runner = CliRunner()
    # Create a dummy schema file
    schema_content = "CREATE TABLE articles (id INT);"
    with open("dummy_schema.sql", "w") as f:
        f.write(schema_content)

    result = runner.invoke(app, ["initialize", "--schema", "dummy_schema.sql"])

    assert result.exit_code == 0
    assert "Database schema initialized successfully" in result.stdout
    # Verify that the adapter's execute_sql method was called with the schema content
    mock_adapter_instance.execute_sql.assert_called_once_with(schema_content)


@patch("py_load_pubmedcentral.cli.get_db_adapter")
def test_initialize_db_error(mock_get_adapter):
    """
    Tests that the initialize command handles generic exceptions during DB connection.
    """
    # Mock the adapter to raise an exception
    mock_adapter_instance = mock_get_adapter.return_value
    mock_adapter_instance.execute_sql.side_effect = Exception("Connection refused")

    runner = CliRunner()
    # We need a real file for this test to pass the `exists=True` check
    with open("dummy_schema.sql", "w") as f:
        f.write("CREATE TABLE test;")

    result = runner.invoke(app, ["initialize", "--schema", "dummy_schema.sql"])
    assert result.exit_code == 1
    assert "An error occurred during database initialization" in result.stdout
    # The traceback should be in the output because of exc_info=True
    assert "Connection refused" in result.stdout


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource.get_article_file_list")
def test_full_load_no_articles_found(mock_get_list, mock_get_adapter):
    """
    Tests that full-load exits gracefully when no articles are found.
    """
    # Mock the data source to return an empty list
    mock_get_list.return_value = []
    # Mock the db adapter to prevent any actual DB calls
    mock_db_instance = mock_get_adapter.return_value
    mock_db_instance.start_run.return_value = 1

    runner = CliRunner()
    # Use catch_exceptions=False to see the full traceback if something unexpected happens
    result = runner.invoke(app, ["full-load", "--source", "s3"], catch_exceptions=False)

    # The command should exit cleanly, but with a warning.
    # The original code used typer.Exit(), which defaults to exit_code 0.
    assert result.exit_code == 0
    assert "No article files found. Exiting." in result.stdout
    # Verify that the run was started and ended correctly
    mock_db_instance.start_run.assert_called_once_with(run_type="FULL")
    mock_db_instance.end_run.assert_called_once()


@patch("py_load_pubmedcentral.cli.logger")
@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli._download_archive_worker")
@patch("py_load_pubmedcentral.cli._parse_archive_worker")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_full_load_success(
    MockS3DataSource,
    mock_parse_worker,
    mock_download_worker,
    mock_get_adapter,
    mock_logger,
    tmp_path,
):
    """
    Tests the happy path for the full-load command.
    """
    runner = CliRunner()

    # --- Setup Mocks ---
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value
    mock_db_instance.start_run.return_value = 1

    # Mock get_article_file_list to return some articles
    from py_load_pubmedcentral.models import ArticleFileInfo
    article_info = ArticleFileInfo(file_path="archive1.tar.gz", pmcid="PMC1")
    mock_source_instance.get_article_file_list.return_value = [article_info]

    # Mock the worker functions
    mock_downloaded_archive = tmp_path / "fake_archive.tar.gz"
    mock_downloaded_archive.touch()
    mock_download_worker.return_value = mock_downloaded_archive

    mock_meta_path = tmp_path / "meta.tsv"
    mock_content_path = tmp_path / "content.tsv"
    mock_meta_path.touch()
    mock_content_path.touch()
    mock_parse_worker.return_value = (mock_meta_path, mock_content_path, 1)

    # --- Execute Command ---
    result = runner.invoke(
        app,
        ["full-load", "--source", "s3", "--parsing-workers", "1"],
        catch_exceptions=False,
    )

    # --- Assertions ---
    assert result.exit_code == 0

    # Check logger calls
    log_calls = " ".join([call.args[0] for call in mock_logger.info.call_args_list])

    assert "--- Starting Full Baseline Load from source: s3 ---" in log_calls
    assert "Discovered 1 unique archives to process." in log_calls
    assert "Successfully downloaded 1 archives." in log_calls
    assert "Successfully parsed 1 archives, yielding 1 records." in log_calls
    assert "Loading data from 1 batches into the database..." in log_calls
    assert "Database loading complete." in log_calls

    mock_db_instance.bulk_insert_from_files_for_full_load.assert_called_once()


from unittest.mock import patch, ANY


from unittest.mock import patch, ANY

@patch("py_load_pubmedcentral.cli.logger")
@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli._get_staging_directory")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_full_load_staging_dir_exception(
    MockS3DataSource,
    mock_get_staging_directory,
    mock_get_adapter,
    mock_logger,
):
    """
    Tests that full-load handles an exception during staging directory creation.
    """
    runner = CliRunner()

    # --- Setup Mocks ---
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value
    mock_db_instance.start_run.return_value = 1

    from py_load_pubmedcentral.models import ArticleFileInfo
    article_info = ArticleFileInfo(file_path="archive1.tar.gz", pmcid="PMC1")
    mock_source_instance.get_article_file_list.return_value = [article_info]

    # Mock the staging directory context manager to raise an exception
    mock_get_staging_directory.side_effect = Exception("Cannot create temp dir")

    # --- Execute Command ---
    result = runner.invoke(
        app,
        ["full-load", "--source", "s3"],
        catch_exceptions=False,
    )

    # --- Assertions ---
    assert result.exit_code == 0 # The CLI should exit gracefully

    # Check logger calls
    mock_logger.critical.assert_called_once()
    assert "A critical error occurred during full_load" in mock_logger.critical.call_args[0][0]

    # Check that end_run was called with FAILED status
    mock_db_instance.end_run.assert_called_once()
    assert mock_db_instance.end_run.call_args.args[1] == "FAILED"


@patch("py_load_pubmedcentral.cli.logger")
@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli._download_archive_worker")
@patch("py_load_pubmedcentral.cli._parse_archive_worker")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_full_load_no_records_parsed(
    MockS3DataSource,
    mock_parse_worker,
    mock_download_worker,
    mock_get_adapter,
    mock_logger,
    tmp_path,
):
    """
    Tests that full-load handles the case where no records are parsed.
    """
    runner = CliRunner()

    # --- Setup Mocks ---
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value
    mock_db_instance.start_run.return_value = 1

    from py_load_pubmedcentral.models import ArticleFileInfo
    article_info = ArticleFileInfo(file_path="archive1.tar.gz", pmcid="PMC1")
    mock_source_instance.get_article_file_list.return_value = [article_info]

    mock_downloaded_archive = tmp_path / "fake_archive.tar.gz"
    mock_downloaded_archive.touch()
    mock_download_worker.return_value = mock_downloaded_archive

    # Mock the parse worker to return None
    mock_parse_worker.return_value = None

    # --- Execute Command ---
    result = runner.invoke(
        app,
        ["full-load", "--source", "s3", "--parsing-workers", "1"],
        catch_exceptions=False,
    )

    # --- Assertions ---
    assert result.exit_code == 0

    log_calls = " ".join([call.args[0] for call in mock_logger.info.call_args_list])
    assert "No new records were parsed, skipping database load." in log_calls

    mock_db_instance.bulk_insert_from_files_for_full_load.assert_not_called()


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli._download_archive_worker")
@patch("py_load_pubmedcentral.cli._parse_delta_archive_worker")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_success(
    MockS3DataSource,
    mock_parse_worker,
    mock_download_worker,
    mock_get_adapter,
    db_with_schema,
    tmp_path,
):
    """
    Tests the happy path for the delta-load command, using mocks to isolate
    the CLI logic from the underlying workers and data sources.
    """
    runner = CliRunner()

    # --- 1. Setup Mocks ---
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value

    last_run_time = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    mock_db_instance.get_last_successful_run_info.return_value = (last_run_time, "file0.xml")

    update1_date = datetime(2023, 1, 2, 0, 0, tzinfo=timezone.utc)
    update1 = IncrementalUpdateInfo(
        archive_path="update1.tar.gz",
        file_list_path="update1.filelist.csv",
        date=update1_date,
    )
    mock_source_instance.get_incremental_updates.return_value = [update1]
    mock_source_instance.get_retracted_pmcids.return_value = ["PMC123"]
    mock_db_instance.handle_deletions.return_value = 1

    # --- 1a. Setup mock file system ---
    # The CLI will try to .unlink() the files returned by the parse worker,
    # so we need to create real (empty) files for it to find.
    mock_meta_path = tmp_path / "meta.tsv"
    mock_content_path = tmp_path / "content.tsv"
    mock_downloaded_archive = tmp_path / "fake_archive.tar.gz"
    mock_meta_path.touch()
    mock_content_path.touch()
    mock_downloaded_archive.touch()

    # Mock the worker functions to return our temporary paths
    mock_download_worker.return_value = mock_downloaded_archive
    mock_parse_worker.return_value = (
        mock_meta_path,
        mock_content_path,
        10,
        ["PMC456"],
        "update1.tar.gz",
    )

    # --- 2. Execute Command ---
    result = runner.invoke(
        app,
        ["delta-load", "--source", "s3", "--parsing-workers", "1"],
        catch_exceptions=False,
    )

    # --- 3. Assertions ---
    print("CLI OUTPUT:")
    print(result.stdout)
    assert result.exit_code == 0
    assert "--- Starting Delta Load ---" in result.stdout
    # The log messages can be multi-line, so we check for substrings.
    assert "Marked 1 articles as retracted based on master" in result.stdout
    assert "Found 1 incremental archives to process." in result.stdout
    assert "-> Loading 10 records from update1.tar.gz" in result.stdout
    assert "Marked 1 articles as retracted based on daily" in result.stdout
    assert "Updating sync_history for run_id" in result.stdout
    assert "with status 'SUCCESS'" in result.stdout

    # Check that the key methods were called
    mock_db_instance.get_last_successful_run_info.assert_called_once_with()
    mock_source_instance.get_incremental_updates.assert_called_once()
    mock_source_instance.get_retracted_pmcids.assert_called_once()
    mock_db_instance.handle_deletions.assert_any_call(["PMC123"])
    mock_db_instance.handle_deletions.assert_any_call(["PMC456"])
    mock_db_instance.bulk_upsert_and_update_state.assert_called_once()


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_no_new_updates(MockS3DataSource, mock_get_adapter):
    """
    Tests that delta-load exits gracefully when no new updates are found.
    """
    # --- Setup ---
    # Mock the data source to return no new updates
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value
    last_run_time = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    mock_db_instance.get_last_successful_run_info.return_value = (last_run_time, "file0.xml")
    mock_source_instance.get_incremental_updates.return_value = []
    mock_source_instance.get_retracted_pmcids.return_value = []

    # --- Execution ---
    runner = CliRunner()
    result = runner.invoke(app, ["delta-load", "--source", "s3"], catch_exceptions=False)

    # --- Verification ---
    assert result.exit_code == 0
    assert "No new incremental updates found." in result.stdout


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_no_last_run(MockS3DataSource, mock_get_adapter):
    """
    Tests that delta-load exits with an error if no successful full load was ever run.
    """
    mock_db_instance = mock_get_adapter.return_value
    # Mock the case where there's no successful 'DELTA' run
    mock_db_instance.get_last_successful_run_info.side_effect = [
        None, # First call for 'DELTA'
        None  # Second call for 'FULL'
    ]

    runner = CliRunner()
    result = runner.invoke(app, ["delta-load", "--source", "s3"], catch_exceptions=False)

    assert result.exit_code == 1
    assert "No successful 'FULL' load found." in result.stdout


@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_download_archive_worker_exception(MockS3DataSource, tmp_path):
    """
    Tests that the _download_archive_worker handles exceptions gracefully.
    """
    from py_load_pubmedcentral.cli import _download_archive_worker, DataSourceName

    # Mock the data source to raise an exception
    mock_source_instance = MockS3DataSource.return_value
    mock_source_instance.download_file.side_effect = Exception("Download failed")

    result = _download_archive_worker("some_file", DataSourceName.s3, tmp_path)

    assert result is None


@patch("py_load_pubmedcentral.cli.logger")
@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.stream_and_parse_tar_gz_archive")
def test_parse_archive_worker_exception(mock_stream_and_parse, mock_get_adapter, mock_logger, tmp_path):
    """
    Tests that the _parse_archive_worker handles exceptions gracefully.
    """
    from py_load_pubmedcentral.cli import _parse_archive_worker

    # Mock the stream_and_parse_tar_gz_archive function to raise an exception
    mock_stream_and_parse.side_effect = Exception("Parsing failed")

    # Create a dummy file to parse
    dummy_file = tmp_path / "dummy.tar.gz"
    dummy_file.touch()

    result = _parse_archive_worker(dummy_file, {}, tmp_path)

    assert result is None
    mock_logger.error.assert_called_once()
    assert "Failed to parse archive" in mock_logger.error.call_args[0][0]


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_parse_delta_archive_worker_exception(MockS3DataSource, mock_get_adapter, tmp_path):
    """
    Tests that the _parse_delta_archive_worker handles exceptions gracefully.
    """
    from py_load_pubmedcentral.cli import _parse_delta_archive_worker, DataSourceName
    from py_load_pubmedcentral.acquisition import IncrementalUpdateInfo
    from datetime import datetime, timezone

    # Mock the data source to raise an exception
    mock_source_instance = MockS3DataSource.return_value
    mock_source_instance.stream_article_infos_from_file_list.side_effect = Exception("Parsing failed")

    # Create a dummy file to parse
    dummy_file = tmp_path / "dummy.tar.gz"
    dummy_file.touch()

    update_info = IncrementalUpdateInfo(
        archive_path="update1.tar.gz",
        file_list_path="update1.filelist.csv",
        date=datetime(2023, 1, 2, 0, 0, tzinfo=timezone.utc),
    )

    result = _parse_delta_archive_worker(dummy_file, update_info, DataSourceName.s3, tmp_path)

    assert result is None


@patch("py_load_pubmedcentral.cli.settings")
def test_get_staging_directory_with_setting(mock_settings, tmp_path):
    """
    Tests that the _get_staging_directory function uses the staging_dir from settings.
    """
    from py_load_pubmedcentral.cli import _get_staging_directory

    # Set the staging_dir in the mock settings
    staging_dir = tmp_path / "staging"
    mock_settings.staging_dir = staging_dir

    with _get_staging_directory() as path:
        assert path == staging_dir
        assert path.exists()


@patch("py_load_pubmedcentral.cli.get_db_adapter")
@patch("py_load_pubmedcentral.cli.S3DataSource")
def test_delta_load_last_file_not_found(MockS3DataSource, mock_get_adapter):
    """
    Tests that delta-load continues gracefully if the last processed file is not found.
    """
    # --- Setup ---
    mock_source_instance = MockS3DataSource.return_value
    mock_db_instance = mock_get_adapter.return_value
    last_run_time = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
    mock_db_instance.get_last_successful_run_info.return_value = (last_run_time, "non_existent_file.xml")
    update1_date = datetime(2023, 1, 2, 0, 0, tzinfo=timezone.utc)
    update1 = IncrementalUpdateInfo(
        archive_path="update1.tar.gz",
        file_list_path="update1.filelist.csv",
        date=update1_date,
    )
    mock_source_instance.get_incremental_updates.return_value = [update1]
    mock_source_instance.get_retracted_pmcids.return_value = []

    # --- Execution ---
    runner = CliRunner()
    result = runner.invoke(app, ["delta-load", "--source", "s3"], catch_exceptions=False)

    # --- Verification ---
    assert result.exit_code == 1
    assert "A critical error occurred during delta-load" in result.stdout
