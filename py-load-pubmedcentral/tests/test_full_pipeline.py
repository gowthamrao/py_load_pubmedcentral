"""
Integration test for the full data pipeline.

This test requires a running PostgreSQL instance and the following
environment variables to be set for the test database:
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_HOST
- DB_PORT
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from py_load_pubmedcentral.cli import app
from py_load_pubmedcentral.utils import get_db_adapter

# Create a CliRunner instance to invoke the CLI commands
runner = CliRunner()

# Path to the test data
TEST_DATA_DIR = Path(__file__).parent
TEST_SCHEMA_PATH = TEST_DATA_DIR.parent / "schemas" / "pmc_schema.sql"
TEST_ARCHIVE_PATH = TEST_DATA_DIR / "test_archive.tar.gz"


@pytest.fixture(scope="module")
def db_adapter():
    """Pytest fixture to provide a configured and connected database adapter."""
    # Check if DB credentials are set, otherwise skip the test
    if not all(os.environ.get(k) for k in ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]):
        pytest.skip("Database environment variables not set, skipping integration tests.")

    adapter = get_db_adapter()
    adapter.connect()
    yield adapter
    adapter.close()


def cleanup_db(adapter):
    """Helper to drop tables for a clean slate."""
    with adapter.conn.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS pmc_articles_content;
            DROP TABLE IF EXISTS pmc_articles_metadata;
            DROP TABLE IF EXISTS sync_history;
        """)
    adapter.conn.commit()


@patch("py_load_pubmedcentral.cli.NcbiFtpDataSource")
def test_full_pipeline_end_to_end(mock_data_source, db_adapter):
    """
    Tests the entire pipeline with a mocked data source:
    1. Initialize the schema.
    2. Run the automated full load command.
    3. Verify the data is correctly loaded in the database.
    4. Verify the sync_history table is updated correctly.
    """
    # --- 1. Configure the Mock ---
    mock_instance = mock_data_source.return_value
    # Simulate finding one baseline file
    mock_instance.list_baseline_files.return_value = ["https://fake.host/test_archive.tar.gz"]
    # Simulate a successful download, returning the path to our local test file
    mock_instance.download_file.return_value = TEST_ARCHIVE_PATH

    # --- 2. Cleanup and Initialization ---
    cleanup_db(db_adapter)
    init_result = runner.invoke(app, ["initialize", "--schema", str(TEST_SCHEMA_PATH)])
    assert init_result.exit_code == 0
    assert "Database schema initialized successfully" in init_result.stdout

    # --- 3. Run Full Load ---
    load_result = runner.invoke(app, ["full-load"])
    assert load_result.exit_code == 0
    assert "Found 1 baseline archives to process" in load_result.stdout
    assert "Full baseline load process finished successfully" in load_result.stdout

    # --- 4. Verification ---
    with db_adapter.conn.cursor() as cursor:
        # Check metadata and content tables
        cursor.execute("SELECT COUNT(*) FROM pmc_articles_metadata;")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT COUNT(*) FROM pmc_articles_content;")
        assert cursor.fetchone()[0] == 2

        # Check for a specific record
        cursor.execute("SELECT title FROM pmc_articles_metadata WHERE pmcid = 'PMC12345';")
        assert cursor.fetchone()[0] == "A Comprehensive Test of the JATS Parser"

        # Check sync_history table
        cursor.execute("SELECT run_type, status, metrics->>'files_processed' FROM sync_history;")
        history_record = cursor.fetchone()
        assert history_record[0] == "FULL"
        assert history_record[1] == "SUCCESS"
        assert int(history_record[2]) == 1

    # --- 5. Final Cleanup ---
    cleanup_db(db_adapter)
