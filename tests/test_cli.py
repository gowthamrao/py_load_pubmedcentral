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
    # Typer exits with 2 for CLI argument validation errors
    assert result.exit_code == 2
    # Typer's validation error messages are sent to stderr.
    assert "Invalid value" in result.stderr
    assert "does not exist" in result.stderr


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
